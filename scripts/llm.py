from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

import aiohttp

from scripts.heuristic_parser import parse_with_heuristics
from scripts.core import QueryPlanError, build_query_from_plan


PROMPT = """
Ты преобразуешь русский запрос пользователя в JSON-план для расчета одной числовой метрики по PostgreSQL.

Схема данных:
1) Таблица videos
- id (text)
- creator_id (text)
- video_created_at (timestamp with time zone)
- views_count, likes_count, comments_count, reports_count (final metrics, bigint)
- created_at, updated_at (timestamp with time zone)

2) Таблица video_snapshots
- id (text)
- video_id (text, ссылка на videos.id)
- views_count, likes_count, comments_count, reports_count (значения на момент снапшота, bigint)
- delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count (прирост с прошлого снапшота, bigint)
- created_at, updated_at (timestamp with time zone)

Нужно вернуть только JSON без markdown.

Формат JSON:
{
  "source": "videos" | "video_snapshots",
  "aggregation": "count_rows" | "count_distinct" | "sum" | "sum_delta_first_hours_after_publication",
  "field": "*" | "id" | "video_id" | "creator_id" | "video_created_at" | "created_at" | "updated_at" | "views_count" | "likes_count" | "comments_count" | "reports_count" | "delta_views_count" | "delta_likes_count" | "delta_comments_count" | "delta_reports_count",
  "hours": 3,
  "filters": [
    {
      "field": "точное имя поля",
      "op": "eq" | "gt" | "gte" | "lt" | "lte" | "date_on" | "date_between",
      "value": "значение для eq/gt/gte/lt/lte/date_on",
      "from": "YYYY-MM-DD для date_between",
      "to": "YYYY-MM-DD для date_between"
    }
  ]
}

Правила:
- Ответ бота всегда одно число, поэтому выбирай только одну агрегацию.
- Для вопросов "сколько всего видео" используй source=videos, aggregation=count_rows.
- Для "сколько разных видео ..." используй aggregation=count_distinct и field=video_id.
- Для "выросли просмотры/лайки/комментарии/жалобы" используй таблицу video_snapshots и соответствующее поле delta_*.
- Для "набрало больше N просмотров за все время" используй videos.views_count > N.
- Для вопросов про "вышло" и период публикации используй videos.video_created_at с date_between.
- Для даты вида "28 ноября 2025" используй date_on.
- Диапазон "с 1 по 5 ноября 2025" преобразуй в date_between и восстанови месяц/год для обеих дат.
- Для "за первые N часов после публикации каждого из них" используй aggregation=sum_delta_first_hours_after_publication, source=video_snapshots, field=delta_*, hours=N.
- Не выдумывай поля и таблицы.
- Не добавляй объяснений.

Текущая дата UTC: {today}
""".strip()


class LlmParserError(RuntimeError):
    pass


class OpenRouterPlanner:
    def __init__(
        self,
        session: aiohttp.ClientSession,
        api_key: str,
        model: str,
        site_url: str = "",
        site_name: str = "",
    ) -> None:
        self._session = session
        self._api_key = api_key
        self._model = model
        self._site_url = site_url
        self._site_name = site_name

    async def plan(self, user_text: str) -> dict[str, Any]:
        heuristic_plan = parse_with_heuristics(user_text)
        if heuristic_plan is not None:
            return heuristic_plan
        if not self._api_key:
            raise LlmParserError("OPENROUTER_API_KEY is empty")

        prompt = PROMPT.replace("{today}", datetime.now(timezone.utc).date().isoformat())
        base_payload = {
            "model": self._model,
            "temperature": 0,
            "max_tokens": 220,
            "messages": [
                {"role": "system", "content": prompt},
                {"role": "user", "content": user_text},
            ],
        }
        payloads = [
            {**base_payload, "response_format": {"type": "json_object"}},
            base_payload,
        ]

        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }
        if self._site_url:
            headers["HTTP-Referer"] = self._site_url
        if self._site_name:
            headers["X-Title"] = self._site_name

        last_error: Exception | None = None
        for payload in payloads:
            try:
                async with self._session.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers=headers,
                    json=payload,
                ) as response:
                    text = await response.text()
                    if response.status >= 400:
                        raise LlmParserError(f"OpenRouter error {response.status}: {text}")
                    data = json.loads(text)
                    content = self._extract_content(data)
                    plan = self._parse_json(content)
                    self._validate_plan(plan)
                    return plan
            except Exception as error:
                last_error = error

        heuristic_plan = parse_with_heuristics(user_text)
        if heuristic_plan is not None:
            return heuristic_plan
        raise LlmParserError(str(last_error))

    def _extract_content(self, data: dict[str, Any]) -> str:
        try:
            message = data["choices"][0]["message"]
        except Exception as error:
            raise LlmParserError(f"Invalid OpenRouter response: {error}")
        content = message.get("content", "")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    parts.append(str(item.get("text", "")))
            return "".join(parts)
        return str(content)

    def _parse_json(self, text: str) -> dict[str, Any]:
        raw = text.strip()
        if raw.startswith("```"):
            raw = re.sub(r"^```(?:json)?", "", raw).strip()
            raw = re.sub(r"```$", "", raw).strip()
        try:
            value = json.loads(raw)
            if isinstance(value, dict):
                return value
        except Exception:
            pass
        match = re.search(r"\{.*\}", raw, flags=re.DOTALL)
        if match:
            value = json.loads(match.group(0))
            if isinstance(value, dict):
                return value
        raise LlmParserError(f"Failed to parse JSON plan: {text}")

    def _validate_plan(self, plan: dict[str, Any]) -> None:
        if not isinstance(plan, dict):
            raise LlmParserError("LLM plan is not an object")
        try:
            build_query_from_plan(plan)
        except QueryPlanError as error:
            raise LlmParserError(f"Invalid plan from LLM: {error}") from error
