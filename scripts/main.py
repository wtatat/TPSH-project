from __future__ import annotations

import asyncio
import logging

import aiohttp
from aiogram import Bot

from scripts.bot import BotService, create_dispatcher
from scripts.core import Settings, apply_schema, create_pool, import_json_if_needed
from scripts.llm import OpenRouterPlanner


async def run() -> None:
    settings = Settings.from_env()
    logging.basicConfig(
        level=getattr(logging, settings.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    if not settings.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is required")

    pool = await create_pool(settings.database_url)
    try:
        await apply_schema(pool, "sql/schema.sql")
        imported = await import_json_if_needed(pool, settings.data_file_path)
        logging.getLogger(__name__).info("imported=%s", imported)

        timeout = aiohttp.ClientTimeout(total=settings.llm_timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout) as http_session:
            planner = OpenRouterPlanner(
                session=http_session,
                api_key=settings.openrouter_api_key,
                model=settings.openrouter_model,
                site_url=settings.openrouter_site_url,
                site_name=settings.openrouter_site_name,
            )
            service = BotService(pool, planner)
            dispatcher = create_dispatcher(service)
            async with Bot(token=settings.telegram_bot_token) as bot:
                await dispatcher.start_polling(bot)
    finally:
        await pool.close()


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
