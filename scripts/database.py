import asyncio
from datetime import datetime
import json
from pathlib import Path
from typing import Any

import asyncpg

from scripts.config import Settings


def parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class Database:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.pool: asyncpg.Pool | None = None

    async def connect_with_retry(self) -> None:
        last_error: Exception | None = None
        for attempt in range(1, self.settings.db_connect_retries + 1):
            try:
                self.pool = await asyncpg.create_pool(
                    dsn=self.settings.postgres_dsn,
                    min_size=1,
                    max_size=10,
                )
                return
            except Exception as exc:
                last_error = exc
                if attempt == self.settings.db_connect_retries:
                    break
                await asyncio.sleep(self.settings.db_connect_retry_delay_seconds)
        raise RuntimeError("Cannot connect to PostgreSQL") from last_error

    async def close(self) -> None:
        if self.pool is not None:
            await self.pool.close()
            self.pool = None

    async def init_schema(self) -> None:
        if self.pool is None:
            raise RuntimeError("Database pool is not initialized")
        schema_path = Path(self.settings.schema_sql_path)
        sql = schema_path.read_text(encoding="utf-8")
        async with self.pool.acquire() as conn:
            await conn.execute(sql)

    async def is_data_loaded(self) -> bool:
        if self.pool is None:
            raise RuntimeError("Database pool is not initialized")
        async with self.pool.acquire() as conn:
            count = await conn.fetchval("SELECT COUNT(*) FROM videos")
        return int(count or 0) > 0

    async def load_json(self, force: bool = False) -> bool:
        if self.pool is None:
            raise RuntimeError("Database pool is not initialized")

        if not force and await self.is_data_loaded():
            return False

        json_path = Path(self.settings.videos_json_path)
        root = json.loads(json_path.read_text(encoding="utf-8"))
        videos_data = root.get("videos", [])
        if not isinstance(videos_data, list):
            raise RuntimeError("videos.json has invalid structure")

        video_rows: list[tuple[Any, ...]] = []
        snapshot_rows: list[tuple[Any, ...]] = []

        for video in videos_data:
            video_rows.append(
                (
                    str(video["id"]),
                    str(video["creator_id"]),
                    parse_ts(video["video_created_at"]),
                    int(video["views_count"]),
                    int(video["likes_count"]),
                    int(video["comments_count"]),
                    int(video["reports_count"]),
                    parse_ts(video["created_at"]),
                    parse_ts(video["updated_at"]),
                )
            )

            snapshots = video.get("snapshots", [])
            for snapshot in snapshots:
                snapshot_rows.append(
                    (
                        str(snapshot["id"]),
                        str(snapshot["video_id"]),
                        int(snapshot["views_count"]),
                        int(snapshot["likes_count"]),
                        int(snapshot["comments_count"]),
                        int(snapshot["reports_count"]),
                        int(snapshot["delta_views_count"]),
                        int(snapshot["delta_likes_count"]),
                        int(snapshot["delta_comments_count"]),
                        int(snapshot["delta_reports_count"]),
                        parse_ts(snapshot["created_at"]),
                        parse_ts(snapshot["updated_at"]),
                    )
                )

        async with self.pool.acquire() as conn:
            async with conn.transaction():
                if force:
                    await conn.execute("TRUNCATE TABLE video_snapshots, videos")

                await conn.executemany(
                    """
                    INSERT INTO videos (
                        id,
                        creator_id,
                        video_created_at,
                        views_count,
                        likes_count,
                        comments_count,
                        reports_count,
                        created_at,
                        updated_at
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                    ON CONFLICT (id) DO UPDATE SET
                        creator_id = EXCLUDED.creator_id,
                        video_created_at = EXCLUDED.video_created_at,
                        views_count = EXCLUDED.views_count,
                        likes_count = EXCLUDED.likes_count,
                        comments_count = EXCLUDED.comments_count,
                        reports_count = EXCLUDED.reports_count,
                        created_at = EXCLUDED.created_at,
                        updated_at = EXCLUDED.updated_at
                    """,
                    video_rows,
                )

                await conn.executemany(
                    """
                    INSERT INTO video_snapshots (
                        id,
                        video_id,
                        views_count,
                        likes_count,
                        comments_count,
                        reports_count,
                        delta_views_count,
                        delta_likes_count,
                        delta_comments_count,
                        delta_reports_count,
                        created_at,
                        updated_at
                    )
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                    ON CONFLICT (id) DO UPDATE SET
                        video_id = EXCLUDED.video_id,
                        views_count = EXCLUDED.views_count,
                        likes_count = EXCLUDED.likes_count,
                        comments_count = EXCLUDED.comments_count,
                        reports_count = EXCLUDED.reports_count,
                        delta_views_count = EXCLUDED.delta_views_count,
                        delta_likes_count = EXCLUDED.delta_likes_count,
                        delta_comments_count = EXCLUDED.delta_comments_count,
                        delta_reports_count = EXCLUDED.delta_reports_count,
                        created_at = EXCLUDED.created_at,
                        updated_at = EXCLUDED.updated_at
                    """,
                    snapshot_rows,
                )

        return True

    async def fetch_single_number(self, sql: str) -> object:
        if self.pool is None:
            raise RuntimeError("Database pool is not initialized")
        async with self.pool.acquire() as conn:
            async with conn.transaction(readonly=True):
                await conn.execute("SET LOCAL statement_timeout = '20s'")
                value = await conn.fetchval(sql)
        return value
