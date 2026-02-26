# Telegram-бот тестовое задание ТПШ

Проект поднимает PostgreSQL, загружает `videos.json` в нормализованные таблицы и запускает Telegram-бота, который принимает вопросы на русском и отвечает одним числом.

## Стек

- Python 3.12
- aiogram (асинхронный Telegram-бот)
- asyncpg (асинхронные запросы к PostgreSQL)
- aiohttp (асинхронный HTTP-клиент для OpenRouter)
- PostgreSQL 16
- Docker Compose

## Структура

- `scripts/main.py` - точка входа
- `scripts/bot.py` - обработка сообщений Telegram
- `scripts/llm_client.py` - классификация вопроса, системный промпт, валидация и генерация SQL через OpenRouter
- `scripts/database.py` - инициализация БД, загрузка JSON, выполнение SQL
- `sql/schema.sql` - создание таблиц и индексов

## Переменные окружения

Шаблон: `.env.example`.

- `BOT_TOKEN` — токен Telegram-бота
- `OPENROUTER_API_KEY` — ключ OpenRouter

## Как получить чувствительные данные

- `BOT_TOKEN`: создать бота через `@BotFather` в Telegram (команда `/newbot`), затем взять выданный токен и записать в `.env`.
- `OPENROUTER_API_KEY`: зарегистрироваться на OpenRouter, создать ключ в кабинете (`https://openrouter.ai/keys`) и записать его в `.env`.
- Перед запуском создать `.env` на основе `.env.example` и заполнить обязательные переменные.


Параметры устойчивости LLM-запросов:

- `LLM_MAX_RETRIES` — число повторов при сетевых сбоях OpenRouter
- `LLM_RETRY_BASE_DELAY_SECONDS` — базовая задержка между ретраями
- `LLM_RETRY_MAX_DELAY_SECONDS` — максимальная задержка между ретраями


## Запуск 

```bash
docker compose up --build
```

## Ручная перезагрузка данных

```bash
docker compose run --rm bot python -m scripts.main --load-only --force
```

## Подход к преобразованию текста в SQL

1. Пользователь отправляет вопрос на русском.
2. `scripts/llm_client.py` через OpenRouter определяет, относится ли вопрос к метрикам.
3. Если вопрос не по метрикам — бот возвращает `0`.
4. Если вопрос по метрикам — OpenRouter модель `qwen/qwen3-235b-a22b-thinking-2507` строит SQL.
5. SQL проходит валидацию (внутри `scripts/llm_client.py`): только `SELECT`, только разрешенные таблицы, без опасных ключевых слов и без нескольких выражений.
6. Запрос исполняется в read-only транзакции, пользователю возвращается только одно число.

## Системный промпт

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
