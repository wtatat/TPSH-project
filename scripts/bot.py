from decimal import Decimal
import logging
import time

from aiogram import Bot, Dispatcher, F
from aiogram.types import Message

from scripts.config import Settings
from scripts.database import Database
from scripts.llm_client import OpenRouterSQLBuilder


logger = logging.getLogger(__name__)


def to_number_text(value: object) -> str:
    if value is None:
        return "0"

    if isinstance(value, bool):
        return "1" if value else "0"

    if isinstance(value, Decimal):
        normalized = value.normalize()
        if normalized == normalized.to_integral():
            return str(int(normalized))
        text = format(normalized, "f").rstrip("0").rstrip(".")
        return text if text else "0"

    if isinstance(value, float):
        text = f"{value:.12f}".rstrip("0").rstrip(".")
        return text if text else "0"

    return str(value)


class MetricsBot:
    def __init__(
        self,
        settings: Settings,
        database: Database,
        sql_builder: OpenRouterSQLBuilder,
    ) -> None:
        self.settings = settings
        self.database = database
        self.sql_builder = sql_builder
        self.dispatcher = Dispatcher()
        self.dispatcher.message.register(self.handle_text, F.text)
        self.dispatcher.message.register(self.handle_non_text, ~F.text)

    async def handle_text(self, message: Message) -> None:
        question = (message.text or "").strip()
        if not question:
            await self._send_number(message, "0")
            return

        started_at = time.perf_counter()
        try:
            is_metric_question = await self.sql_builder.is_metric_question(question)
            if not is_metric_question:
                source = "reject"
                answer = "0"
                elapsed_ms = round((time.perf_counter() - started_at) * 1000)
                logger.info(
                    "Handled message user_id=%s source=%s elapsed_ms=%s question=%r sql=%r answer=%s",
                    message.from_user.id if message.from_user else None,
                    source,
                    elapsed_ms,
                    question,
                    None,
                    answer,
                )
                await self._send_number(message, answer)
                return

            source = "llm"
            sql = await self.sql_builder.build_sql(question)
            value = await self.database.fetch_single_number(sql)
            answer = to_number_text(value)
            elapsed_ms = round((time.perf_counter() - started_at) * 1000)
            logger.info(
                "Handled message user_id=%s source=%s elapsed_ms=%s question=%r sql=%r answer=%s",
                message.from_user.id if message.from_user else None,
                source,
                elapsed_ms,
                question,
                sql,
                answer,
            )
            await self._send_number(message, answer)
        except Exception:
            elapsed_ms = round((time.perf_counter() - started_at) * 1000)
            logger.exception(
                "Message handling failed user_id=%s elapsed_ms=%s question=%r",
                message.from_user.id if message.from_user else None,
                elapsed_ms,
                question,
            )
            await self._send_number(message, "0")

    async def handle_non_text(self, message: Message) -> None:
        await self._send_number(message, "0")

    async def _send_number(self, message: Message, number_text: str) -> None:
        try:
            await message.reply(number_text)
        except Exception:
            await message.answer(number_text)

    async def run(self) -> None:
        bot = Bot(token=self.settings.bot_token)
        try:
            await bot.delete_webhook(drop_pending_updates=True)
            await self.dispatcher.start_polling(
                bot,
                allowed_updates=self.dispatcher.resolve_used_update_types(),
            )
        finally:
            await bot.session.close()
