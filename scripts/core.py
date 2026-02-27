from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

import asyncpg
from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    telegram_bot_token: str
    openrouter_api_key: str
    openrouter_model: str
    openrouter_site_url: str
    openrouter_site_name: str
    database_url: str
    data_file_path: str
    llm_timeout_seconds: int
    log_level: str

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        return cls(
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            openrouter_api_key=os.getenv("OPENROUTER_API_KEY", "").strip(),
            openrouter_model=os.getenv("OPENROUTER_MODEL", "qwen/qwen3-235b-a22b-thinking-2507").strip(),
            openrouter_site_url=os.getenv("OPENROUTER_SITE_URL", "").strip(),
            openrouter_site_name=os.getenv("OPENROUTER_SITE_NAME", "").strip(),
            database_url=os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/wizard_stats").strip(),
            data_file_path=os.getenv("DATA_FILE_PATH", "videos.json").strip(),
            llm_timeout_seconds=int(os.getenv("LLM_TIMEOUT_SECONDS", "60")),
            log_level=os.getenv("LOG_LEVEL", "INFO").strip().upper(),
        )


async def create_pool(database_url: str, retries: int = 30, delay_seconds: float = 2.0) -> asyncpg.Pool:
    last_error: Exception | None = None
    for _ in range(retries):
        try:
            return await asyncpg.create_pool(dsn=database_url, min_size=1, max_size=10)
        except Exception as error:
            last_error = error
            await asyncio.sleep(delay_seconds)
    raise RuntimeError(f"Database connection failed: {last_error}")


async def apply_schema(pool: asyncpg.Pool, schema_path: str | Path) -> None:
    sql = Path(schema_path).read_text(encoding="utf-8")
    async with pool.acquire() as conn:
        await conn.execute(sql)


def _parse_dt(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _chunked(items: list[tuple], size: int) -> Iterable[list[tuple]]:
    for index in range(0, len(items), size):
        yield items[index:index + size]


async def import_json_if_needed(pool: asyncpg.Pool, json_path: str | Path) -> bool:
    async with pool.acquire() as conn:
        has_data = await conn.fetchval("SELECT EXISTS (SELECT 1 FROM videos LIMIT 1)")
    if has_data:
        return False
    path = Path(json_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    videos = payload["videos"]
    video_rows: list[tuple] = []
    snapshot_rows: list[tuple] = []
    for item in videos:
        video_rows.append(
            (
                item["id"],
                item["creator_id"],
                _parse_dt(item["video_created_at"]),
                int(item["views_count"]),
                int(item["likes_count"]),
                int(item["comments_count"]),
                int(item["reports_count"]),
                _parse_dt(item["created_at"]),
                _parse_dt(item["updated_at"]),
            )
        )
        for snap in item.get("snapshots", []):
            snapshot_rows.append(
                (
                    snap["id"],
                    snap["video_id"],
                    int(snap["views_count"]),
                    int(snap["likes_count"]),
                    int(snap["comments_count"]),
                    int(snap["reports_count"]),
                    int(snap["delta_views_count"]),
                    int(snap["delta_likes_count"]),
                    int(snap["delta_comments_count"]),
                    int(snap["delta_reports_count"]),
                    _parse_dt(snap["created_at"]),
                    _parse_dt(snap["updated_at"]),
                )
            )
    insert_videos_sql = """
        INSERT INTO videos (
            id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count, created_at, updated_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (id) DO NOTHING
    """
    insert_snapshots_sql = """
        INSERT INTO video_snapshots (
            id, video_id, views_count, likes_count, comments_count, reports_count,
            delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
            created_at, updated_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
        ON CONFLICT (id) DO NOTHING
    """
    async with pool.acquire() as conn:
        async with conn.transaction():
            for chunk in _chunked(video_rows, 500):
                await conn.executemany(insert_videos_sql, chunk)
            for chunk in _chunked(snapshot_rows, 2000):
                await conn.executemany(insert_snapshots_sql, chunk)
    return True


class QueryPlanError(ValueError):
    pass


VIDEO_FIELDS = {
    "id",
    "creator_id",
    "video_created_at",
    "views_count",
    "likes_count",
    "comments_count",
    "reports_count",
    "created_at",
    "updated_at",
}

SNAPSHOT_FIELDS = {
    "id",
    "video_id",
    "views_count",
    "likes_count",
    "comments_count",
    "reports_count",
    "delta_views_count",
    "delta_likes_count",
    "delta_comments_count",
    "delta_reports_count",
    "created_at",
    "updated_at",
}

NUMERIC_FIELDS = {
    "views_count",
    "likes_count",
    "comments_count",
    "reports_count",
    "delta_views_count",
    "delta_likes_count",
    "delta_comments_count",
    "delta_reports_count",
}

DATE_FIELDS = {"video_created_at", "created_at", "updated_at"}
TEXT_FIELDS = {"id", "creator_id", "video_id"}

ALLOWED_SOURCES = {
    "videos": VIDEO_FIELDS,
    "video_snapshots": SNAPSHOT_FIELDS,
}

ALLOWED_AGGREGATIONS = {"count_rows", "count_distinct", "sum"}
ALLOWED_OPERATORS = {"eq", "gt", "gte", "lt", "lte", "date_on", "date_between"}


@dataclass(frozen=True)
class BuiltQuery:
    sql: str
    params: list[Any]


def _ensure_dict(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise QueryPlanError(f"{name} must be an object")
    return value


def _ensure_list(value: Any, name: str) -> list[Any]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise QueryPlanError(f"{name} must be a list")
    return value


def _validate_field(source: str, field: str) -> str:
    if field not in ALLOWED_SOURCES[source]:
        raise QueryPlanError(f"Field {field} is not allowed for {source}")
    return field


def _normalize_number(value: Any) -> int:
    if isinstance(value, bool):
        raise QueryPlanError("Boolean is not a valid number")
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        cleaned = value.replace(" ", "").strip()
        if cleaned.startswith("+"):
            cleaned = cleaned[1:]
        if cleaned.isdigit():
            return int(cleaned)
    raise QueryPlanError(f"Invalid numeric value: {value}")


def _normalize_date(value: Any) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        raw = value.strip()
        try:
            return date.fromisoformat(raw)
        except ValueError as error:
            raise QueryPlanError(f"Invalid date value: {value}") from error
    raise QueryPlanError(f"Invalid date value: {value}")


def build_query_from_plan(plan: dict[str, Any]) -> BuiltQuery:
    plan = _ensure_dict(plan, "plan")
    source = str(plan.get("source", "")).strip()
    if source not in ALLOWED_SOURCES:
        raise QueryPlanError("source must be videos or video_snapshots")
    aggregation = str(plan.get("aggregation", "")).strip()
    if aggregation not in ALLOWED_AGGREGATIONS | {"sum_delta_first_hours_after_publication"}:
        raise QueryPlanError("Unsupported aggregation")
    field = plan.get("field")
    if field is None:
        field = "*"
    field = str(field).strip()
    filters = _ensure_list(plan.get("filters"), "filters")

    if aggregation == "sum_delta_first_hours_after_publication":
        if source != "video_snapshots":
            raise QueryPlanError("sum_delta_first_hours_after_publication requires source=video_snapshots")
        if field not in {
            "delta_views_count",
            "delta_likes_count",
            "delta_comments_count",
            "delta_reports_count",
        }:
            raise QueryPlanError("field must be delta_* metric for sum_delta_first_hours_after_publication")
        hours = _normalize_number(plan.get("hours"))
        if hours <= 0:
            raise QueryPlanError("hours must be > 0")
        sql = (
            f"SELECT COALESCE(SUM(s.{field}), 0)::bigint AS value "
            "FROM video_snapshots s "
            "JOIN videos v ON v.id = s.video_id "
            "WHERE s.created_at >= v.video_created_at "
            "AND s.created_at <= v.video_created_at + ($1::int * INTERVAL '1 hour')"
        )
        return BuiltQuery(sql=sql, params=[hours])

    if aggregation == "count_rows":
        select_expr = "COUNT(*)::bigint"
    elif aggregation == "count_distinct":
        if field == "*":
            raise QueryPlanError("field is required for count_distinct")
        _validate_field(source, field)
        select_expr = f"COUNT(DISTINCT {field})::bigint"
    elif aggregation == "sum":
        if field == "*" or field not in NUMERIC_FIELDS:
            raise QueryPlanError("field must be a numeric metric for sum")
        _validate_field(source, field)
        select_expr = f"COALESCE(SUM({field}), 0)::bigint"
    else:
        raise QueryPlanError("Unsupported aggregation")

    conditions: list[str] = []
    params: list[Any] = []

    for raw_filter in filters:
        item = _ensure_dict(raw_filter, "filter")
        filter_field = str(item.get("field", "")).strip()
        _validate_field(source, filter_field)
        op = str(item.get("op", "")).strip()
        if op not in ALLOWED_OPERATORS:
            raise QueryPlanError(f"Unsupported operator: {op}")

        if op in {"eq", "gt", "gte", "lt", "lte"}:
            value = item.get("value")
            param_index = len(params) + 1
            if filter_field in NUMERIC_FIELDS:
                params.append(_normalize_number(value))
            elif filter_field in TEXT_FIELDS:
                params.append(str(value))
            elif filter_field in DATE_FIELDS:
                params.append(_normalize_date(value) if op == "eq" else str(value))
            else:
                params.append(value)
            sql_op = {
                "eq": "=",
                "gt": ">",
                "gte": ">=",
                "lt": "<",
                "lte": "<=",
            }[op]
            if filter_field in DATE_FIELDS and op == "eq":
                conditions.append(f"{filter_field}::date = ${param_index}::date")
            else:
                conditions.append(f"{filter_field} {sql_op} ${param_index}")
            continue

        if filter_field not in DATE_FIELDS:
            raise QueryPlanError(f"Date operator is allowed only for date fields: {filter_field}")

        if op == "date_on":
            value = str(item.get("value", "")).strip()
            if not value:
                raise QueryPlanError("date_on requires value")
            param_index = len(params) + 1
            params.append(_normalize_date(value))
            conditions.append(f"{filter_field}::date = ${param_index}::date")
            continue

        if op == "date_between":
            date_from = str(item.get("from", "")).strip()
            date_to = str(item.get("to", "")).strip()
            if not date_from or not date_to:
                raise QueryPlanError("date_between requires from and to")
            first = len(params) + 1
            second = len(params) + 2
            params.extend([_normalize_date(date_from), _normalize_date(date_to)])
            conditions.append(f"{filter_field}::date BETWEEN ${first}::date AND ${second}::date")
            continue

    sql = f"SELECT {select_expr} AS value FROM {source}"
    if conditions:
        sql += " WHERE " + " AND ".join(conditions)
    return BuiltQuery(sql=sql, params=params)


async def execute_query_plan(pool: asyncpg.Pool, plan: dict[str, Any]) -> int:
    built = build_query_from_plan(plan)
    async with pool.acquire() as conn:
        result = await conn.fetchval(built.sql, *built.params)
    return int(result or 0)
