import asyncio
import logging
import sys

from scripts.bot import MetricsBot
from scripts.config import load_settings
from scripts.database import Database
from scripts.llm_client import OpenRouterSQLBuilder


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )


async def run_bot() -> None:
    setup_logging()
    settings = load_settings()
    database = Database(settings)
    sql_builder = OpenRouterSQLBuilder(settings)
    bot = MetricsBot(settings, database, sql_builder)

    try:
        await database.connect_with_retry()
        await database.init_schema()
        await database.load_json(force=False)
        await bot.run()
    finally:
        await sql_builder.close()
        await database.close()


async def load_data(force: bool) -> None:
    setup_logging()
    settings = load_settings()
    database = Database(settings)
    try:
        await database.connect_with_retry()
        await database.init_schema()
        await database.load_json(force=force)
    finally:
        await database.close()


if __name__ == "__main__":
    args = set(sys.argv[1:])
    if "--load-only" in args:
        asyncio.run(load_data(force="--force" in args))
    else:
        asyncio.run(run_bot())
