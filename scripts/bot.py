from __future__ import annotations

import logging

import asyncpg
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart
from aiogram.types import Message

from scripts.core import QueryPlanError, execute_query_plan
from scripts.llm import OpenRouterPlanner


class BotService:
    def __init__(self, pool: asyncpg.Pool, planner: OpenRouterPlanner) -> None:
        self._pool = pool
        self._planner = planner
        self._logger = logging.getLogger(__name__)

    async def handle_text(self, text: str) -> int:
        plan = await self._planner.plan(text)
        self._logger.info("plan=%s", plan)
        return await execute_query_plan(self._pool, plan)


def create_dispatcher(service: BotService) -> Dispatcher:
    router = Router()

    @router.message(CommandStart())
    async def on_start(message: Message) -> None:
        await message.answer("Отправьте вопрос по статистике видео. В ответ верну одно число.")

    @router.message(F.text)
    async def on_text(message: Message) -> None:
        text = (message.text or "").strip()
        if not text:
            await message.answer("0")
            return
        try:
            value = await service.handle_text(text)
            await message.answer(str(value))
        except QueryPlanError:
            service._logger.exception("query plan validation error")
            await message.answer("0")
        except Exception:
            service._logger.exception("request processing error")
            await message.answer("0")

    dispatcher = Dispatcher()
    dispatcher.include_router(router)
    return dispatcher
