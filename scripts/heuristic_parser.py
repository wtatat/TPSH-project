from __future__ import annotations

import re
from datetime import date
from typing import Any


MONTHS = {
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    "мая": 5,
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
}


def _normalize(text: str) -> str:
    text = text.lower().replace("ё", "е")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _parse_ru_date(fragment: str) -> str | None:
    match = re.search(r"(\d{1,2})\s+([а-я]+)\s+(\d{4})", fragment.lower())
    if not match:
        return None
    day = int(match.group(1))
    month_name = match.group(2)
    year = int(match.group(3))
    month = MONTHS.get(month_name)
    if not month:
        return None
    return date(year, month, day).isoformat()


def _parse_date_range(text: str) -> tuple[str, str] | None:
    match = re.search(
        r"с\s+(\d{1,2})\s+([а-я]+)\s+(\d{4})\s+по\s+(\d{1,2})\s+([а-я]+)\s+(\d{4})",
        text,
    )
    if match:
        d1 = _parse_ru_date(f"{match.group(1)} {match.group(2)} {match.group(3)}")
        d2 = _parse_ru_date(f"{match.group(4)} {match.group(5)} {match.group(6)}")
        if d1 and d2:
            return d1, d2

    short_match = re.search(
        r"с\s+(\d{1,2})\s+по\s+(\d{1,2})\s+([а-я]+)\s+(\d{4})",
        text,
    )
    if short_match:
        day_from = int(short_match.group(1))
        day_to = int(short_match.group(2))
        month_name = short_match.group(3)
        year = int(short_match.group(4))
        month = MONTHS.get(month_name)
        if month:
            return date(year, month, day_from).isoformat(), date(year, month, day_to).isoformat()
    return None


def _metric_from_text(text: str, delta: bool) -> str | None:
    if "просмотр" in text:
        return "delta_views_count" if delta else "views_count"
    if "лайк" in text:
        return "delta_likes_count" if delta else "likes_count"
    if "коммент" in text:
        return "delta_comments_count" if delta else "comments_count"
    if "жалоб" in text:
        return "delta_reports_count" if delta else "reports_count"
    return None


def _extract_number(text: str) -> int | None:
    numbers = re.findall(r"\d[\d\s]*", text)
    if not numbers:
        return None
    candidate = max(numbers, key=lambda x: len(x))
    return int(candidate.replace(" ", ""))


def parse_with_heuristics(text: str) -> dict[str, Any] | None:
    value = _normalize(text)

    first_hours_match = re.search(r"первые\s+(\d+)\s+час", value)
    if first_hours_match and "после публикации" in value and "прирост" in value:
        metric = _metric_from_text(value, delta=True)
        if metric:
            return {
                "source": "video_snapshots",
                "aggregation": "sum_delta_first_hours_after_publication",
                "field": metric,
                "hours": int(first_hours_match.group(1)),
                "filters": [],
            }

    if "сколько всего видео" in value and ("в системе" in value or "есть" in value):
        return {
            "source": "videos",
            "aggregation": "count_rows",
            "field": "*",
            "filters": [],
        }

    if "сколько видео у креатора" in value and "вышло" in value:
        creator_match = re.search(r"id\s+([a-z0-9-]+)", value)
        date_range = _parse_date_range(value)
        if creator_match and date_range:
            return {
                "source": "videos",
                "aggregation": "count_rows",
                "field": "*",
                "filters": [
                    {"field": "creator_id", "op": "eq", "value": creator_match.group(1)},
                    {"field": "video_created_at", "op": "date_between", "from": date_range[0], "to": date_range[1]},
                ],
            }

    if "сколько видео у креатора" in value and "набрал" in value and "больше" in value:
        creator_match = re.search(r"id\s+([a-z0-9-]+)", value)
        metric = _metric_from_text(value, delta=False)
        threshold = _extract_number(value)
        if creator_match and metric and threshold is not None:
            return {
                "source": "videos",
                "aggregation": "count_rows",
                "field": "*",
                "filters": [
                    {"field": "creator_id", "op": "eq", "value": creator_match.group(1)},
                    {"field": metric, "op": "gt", "value": threshold},
                ],
            }

    if "сколько видео" in value and "больше" in value:
        metric = _metric_from_text(value, delta=False)
        threshold = _extract_number(value)
        if metric and threshold is not None:
            return {
                "source": "videos",
                "aggregation": "count_rows",
                "field": "*",
                "filters": [
                    {"field": metric, "op": "gt", "value": threshold},
                ],
            }

    if "в сумме вырос" in value or "в сумме выросли" in value:
        metric = _metric_from_text(value, delta=True)
        date_range = _parse_date_range(value)
        if metric and date_range:
            return {
                "source": "video_snapshots",
                "aggregation": "sum",
                "field": metric,
                "filters": [
                    {"field": "created_at", "op": "date_between", "from": date_range[0], "to": date_range[1]},
                ],
            }
        one_date = _parse_ru_date(value)
        if metric and one_date:
            return {
                "source": "video_snapshots",
                "aggregation": "sum",
                "field": metric,
                "filters": [
                    {"field": "created_at", "op": "date_on", "value": one_date},
                ],
            }

    if "сколько разных видео" in value and ("новые просмотр" in value or "новые просмотры" in value):
        one_date = _parse_ru_date(value)
        if one_date:
            return {
                "source": "video_snapshots",
                "aggregation": "count_distinct",
                "field": "video_id",
                "filters": [
                    {"field": "delta_views_count", "op": "gt", "value": 0},
                    {"field": "created_at", "op": "date_on", "value": one_date},
                ],
            }

    return None
