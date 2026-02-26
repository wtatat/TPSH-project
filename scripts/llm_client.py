import asyncio
from datetime import datetime, timezone
import json
import logging
import random
import re

import aiohttp

from scripts.config import Settings


OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
logger = logging.getLogger(__name__)


class SQLValidationError(ValueError):
    pass


FORBIDDEN_PATTERN = re.compile(
    r"\b(insert|update|delete|drop|alter|truncate|create|grant|revoke|copy|call|do|vacuum|analyze|comment)\b",
    re.IGNORECASE,
)


def validate_sql(sql: str) -> str:
    candidate = sql.strip()
    if not candidate:
        raise SQLValidationError("Empty SQL")

    if candidate.endswith(";"):
        candidate = candidate[:-1].strip()

    if not re.match(r"^select\b", candidate, flags=re.IGNORECASE):
        raise SQLValidationError("Only SELECT is allowed")

    if FORBIDDEN_PATTERN.search(candidate):
        raise SQLValidationError("Forbidden keyword detected")

    if "--" in candidate or "/*" in candidate or "*/" in candidate:
        raise SQLValidationError("SQL comments are not allowed")

    if ";" in candidate:
        raise SQLValidationError("Multiple statements are not allowed")

    if not re.search(r"\b(videos|video_snapshots)\b", candidate, flags=re.IGNORECASE):
        raise SQLValidationError("Only allowed tables can be used")

    return candidate


def build_system_prompt() -> str:
    today = datetime.now(timezone.utc).date().isoformat()
    return f"""
Ты конвертируешь вопрос на русском в один SQL для PostgreSQL 16.
Текущая дата: {today}.

Схема:
videos(id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count, created_at, updated_at)
video_snapshots(id, video_id, views_count, likes_count, comments_count, reports_count, delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count, created_at, updated_at)

Правила:
1) Итоговые значения за все время бери из videos.
2) Прирост/новые за день или период бери из video_snapshots по delta_*.
3) "Сколько разных видео получали новые просмотры" = COUNT(DISTINCT video_id) и delta_views_count > 0.
4) Дата публикации видео: videos.video_created_at.
5) Активность/прирост по времени: video_snapshots.created_at.
6) Одна дата и диапазон дат включительны:
   col >= DATE 'YYYY-MM-DD' AND col < DATE 'YYYY-MM-DD' + INTERVAL '1 day'
   для периода "с ... по ...": левая граница = start, правая = end + 1 day.
7) Возвращай одну числовую колонку `value`.
8) Для SUM используй COALESCE(SUM(...), 0).
9) Только SELECT. Без объяснений, markdown, комментариев, лишнего текста.

Примеры:
Q: Сколько всего видео есть в системе?
SQL: SELECT COUNT(*)::bigint AS value FROM videos

Q: Сколько видео у креатора с id abc123 вышло с 1 ноября 2025 по 5 ноября 2025 включительно?
SQL: SELECT COUNT(*)::bigint AS value FROM videos WHERE creator_id = 'abc123' AND video_created_at >= DATE '2025-11-01' AND video_created_at < DATE '2025-11-05' + INTERVAL '1 day'

Q: Сколько видео набрало больше 100000 просмотров за всё время?
SQL: SELECT COUNT(*)::bigint AS value FROM videos WHERE views_count > 100000

Q: На сколько просмотров в сумме выросли все видео 28 ноября 2025?
SQL: SELECT COALESCE(SUM(delta_views_count), 0)::bigint AS value FROM video_snapshots WHERE created_at >= DATE '2025-11-28' AND created_at < DATE '2025-11-28' + INTERVAL '1 day'

Q: Сколько разных видео получали новые просмотры 27 ноября 2025?
SQL: SELECT COUNT(DISTINCT video_id)::bigint AS value FROM video_snapshots WHERE delta_views_count > 0 AND created_at >= DATE '2025-11-27' AND created_at < DATE '2025-11-27' + INTERVAL '1 day'

Q: Сколько всего лайков у всех видео за всё время?
SQL: SELECT COALESCE(SUM(likes_count), 0)::bigint AS value FROM videos

Q: На сколько в сумме выросли лайки с 1 ноября 2025 по 3 ноября 2025?
SQL: SELECT COALESCE(SUM(delta_likes_count), 0)::bigint AS value FROM video_snapshots WHERE created_at >= DATE '2025-11-01' AND created_at < DATE '2025-11-03' + INTERVAL '1 day'

Q: Сколько всего жалоб у всех видео?
SQL: SELECT COALESCE(SUM(reports_count), 0)::bigint AS value FROM videos

Верни только SQL-запрос.
""".strip()


def extract_sql(text: str) -> str:
    cleaned = re.sub(r"<think>.*?</think>", "", text, flags=re.IGNORECASE | re.DOTALL)
    fenced = re.search(r"```(?:sql)?\s*(.*?)```", cleaned, flags=re.IGNORECASE | re.DOTALL)
    if fenced:
        cleaned = fenced.group(1)

    cleaned = cleaned.strip()
    select_match = re.search(r"select\b", cleaned, flags=re.IGNORECASE)
    if select_match and select_match.start() > 0:
        cleaned = cleaned[select_match.start() :]

    if ";" in cleaned:
        cleaned = cleaned.split(";", 1)[0]

    return cleaned.strip()


class OpenRouterSQLBuilder:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.settings.llm_timeout_seconds)
        )

    async def close(self) -> None:
        if not self.session.closed:
            await self.session.close()

    async def build_sql(self, question: str) -> str:
        system_prompt = build_system_prompt()
        sql = await self._request_sql(system_prompt, question)
        try:
            return validate_sql(sql)
        except SQLValidationError as exc:
            fix_request = (
                "Исправь SQL под ограничения.\n"
                f"Вопрос:\n{question}\n\n"
                f"SQL:\n{sql}\n\n"
                f"Ошибка валидации:\n{exc}\n\n"
                "Верни только исправленный SQL."
            )
            repaired = await self._request_sql(system_prompt, fix_request)
            return validate_sql(repaired)

    async def is_metric_question(self, question: str) -> bool:
        system_prompt = (
            "Ты классифицируешь пользовательский вопрос.\n"
            "Верни только YES или NO.\n"
            "YES: вопрос про метрики видео/снапшотов, количество/сумму/прирост/фильтрацию по дате/креатору/id.\n"
            "NO: приветствия, мат, оффтоп, бессмысленный текст, команды вне аналитики."
        )
        text = await self._request_text(system_prompt, question)
        normalized = text.strip().upper()
        if normalized.startswith("YES"):
            return True
        if normalized.startswith("NO"):
            return False
        return False

    async def _request_sql(self, system_prompt: str, user_prompt: str) -> str:
        text = await self._request_text(system_prompt, user_prompt)
        sql = extract_sql(text)
        if not sql:
            raise RuntimeError("LLM returned empty SQL")
        return sql

    async def _request_text(self, system_prompt: str, user_prompt: str) -> str:
        payload = {
            "model": self.settings.openrouter_model,
            "temperature": 0,
            "stream": False,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        headers = {
            "Authorization": f"Bearer {self.settings.openrouter_api_key}",
            "Content-Type": "application/json",
        }
        data = await self._post_with_retries(headers=headers, payload=payload)
        content = data["choices"][0]["message"]["content"]

        if isinstance(content, list):
            text = "".join(
                chunk.get("text", "") for chunk in content if isinstance(chunk, dict)
            )
        else:
            text = str(content)
        return text

    async def _post_with_retries(self, headers: dict[str, str], payload: dict) -> dict:
        last_error: Exception | None = None
        for attempt in range(1, self.settings.llm_max_retries + 1):
            try:
                async with self.session.post(
                    OPENROUTER_URL, headers=headers, json=payload
                ) as response:
                    body = await response.text()
                    if response.status >= 500:
                        raise RuntimeError(f"OpenRouter HTTP {response.status}: {body}")
                    if response.status >= 400:
                        raise RuntimeError(f"OpenRouter HTTP {response.status}: {body}")
                return json.loads(body)
            except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
                json.JSONDecodeError,
                RuntimeError,
            ) as exc:
                last_error = exc
                if attempt >= self.settings.llm_max_retries:
                    break
                delay = min(
                    self.settings.llm_retry_base_delay_seconds * (2 ** (attempt - 1)),
                    self.settings.llm_retry_max_delay_seconds,
                )
                jitter = random.uniform(0, 0.25)
                logger.warning(
                    "OpenRouter call failed (attempt %s/%s): %s",
                    attempt,
                    self.settings.llm_max_retries,
                    exc,
                )
                await asyncio.sleep(delay + jitter)
        raise RuntimeError("OpenRouter request failed after retries") from last_error
