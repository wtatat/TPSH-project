from __future__ import annotations

import asyncio

from scripts.core import Settings, apply_schema, create_pool, import_json_if_needed


async def main() -> None:
    settings = Settings.from_env()
    pool = await create_pool(settings.database_url)
    try:
        await apply_schema(pool, "sql/schema.sql")
        await import_json_if_needed(pool, settings.data_file_path)
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
