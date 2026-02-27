# Telegram-бот для расчета метрик по видео

Бот принимает вопрос на русском языке, преобразует его в структурированный план запроса через LLM и возвращает одно число, рассчитанное по PostgreSQL.

## Что реализовано

- PostgreSQL со схемой `videos` и `video_snapshots`
- Автоматическая инициализация таблиц
- Автоматическая загрузка `videos.json` при первом запуске
- Telegram-бот на `aiogram` (async)
- Асинхронные запросы к БД (`asyncpg`)
- Асинхронный вызов LLM API (`aiohttp`)
- Запуск локально одной командой через `docker compose up --build`

## Стек

- Python 3.12
- aiogram 3
- asyncpg
- aiohttp
- PostgreSQL 16
- Docker Compose
- OpenRouter API
- Модель: `arcee-ai/trinity-large-preview:free`

## Структура проекта

- `scripts/main.py` — запуск приложения
- `scripts/bot.py` — Telegram handlers
- `scripts/llm.py` — вызов OpenRouter и преобразование текста в JSON-план
- `scripts/core.py` — настройки, подключение к БД, импорт JSON и SQL-builder
- `scripts/seed.py` — запуск инициализации БД и импорта
- `sql/schema.sql` — создание таблиц и индексов
- `videos.json` — входные данные

## Быстрый запуск

1. Создать `.env` на основе `.env.example`
2. Заполнить `TELEGRAM_BOT_TOKEN`
3. Заполнить `OPENROUTER_API_KEY`
4. Запустить:

```bash
docker compose up --build
```

После старта приложение:

- подключится к PostgreSQL
- создаст таблицы
- загрузит `videos.json` (если таблица `videos` пустая)
- запустит Telegram-бота

## Переменные окружения

Пример в `.env.example`.

Обязательные:

- `TELEGRAM_BOT_TOKEN` — токен Telegram-бота
- `OPENROUTER_API_KEY` — ключ OpenRouter

Основные:

- `OPENROUTER_MODEL` — по умолчанию `arcee-ai/trinity-large-preview:free`
- `DATABASE_URL` — строка подключения к PostgreSQL
- `DATA_FILE_PATH` — путь до `videos.json`

## Как получить чувствительные данные

- `TELEGRAM_BOT_TOKEN`: создать бота через `@BotFather` в Telegram
- `OPENROUTER_API_KEY`: создать ключ в `https://openrouter.ai/settings/keys` и указать его в `.env`

## Инициализация БД и загрузка JSON

Инициализация и импорт выполняются автоматически при старте `scripts`.

Отдельно можно запустить только импорт:

```bash
docker compose run --rm bot python -m scripts.seed
```

## Архитектура и преобразование текста в запрос к БД

Пайплайн:

1. Пользователь отправляет вопрос в Telegram
2. `scripts/llm.py` отправляет текст в OpenRouter
3. LLM возвращает строго JSON-план (не SQL)
4. `scripts/core.py` валидирует поля/операторы по whitelist
5. Из JSON-плана собирается безопасный SQL с параметрами
6. Выполняется запрос в PostgreSQL
7. Бот возвращает одно число

Такой подход дает:

- гибкость распознавания естественного языка
- контроль над SQL (LLM не исполняет произвольный SQL)
- предсказуемость при проверке



Дальше:

- валидирует JSON-план по whitelist полей и операций
- строит обычный SQL-запрос к PostgreSQL
- выполняет этот SQL в базе
- возвращает одно число

То есть фактический расчет всегда выполняется SQL в PostgreSQL, LLM же используется для преобразования естественного языка в контролируемую структуру.

## Описание схемы данных для LLM и промпт

LLM получает явное описание двух таблиц:

- `videos` — финальные значения метрик по ролику
- `video_snapshots` — почасовые замеры и поля `delta_*` для приростов

LLM  возвращает JSON-план формата:

```json
{
  "source": "videos",
  "aggregation": "count_rows",
  "field": "*",
  "filters": [
    {
      "field": "video_created_at",
      "op": "date_between",
      "from": "2025-11-01",
      "to": "2025-11-05"
    }
  ]
}
```

В промпте зафиксированы правила:

- для прироста использовать `video_snapshots` и поля `delta_*`
- для финальных значений использовать `videos`
- для дат использовать `date_on` и `date_between`
- не выдумывать таблицы и поля
- возвращать только JSON

Промпт находится в `scripts/llm.py`.

Пример преобразования:

- Вопрос: `Сколько видео набрало больше 100 000 просмотров за всё время?`
- JSON-план: `{"source":"videos","aggregation":"count_rows","field":"*","filters":[{"field":"views_count","op":"gt","value":100000}]}`
- SQL, который получается на выходе: `SELECT COUNT(*)::bigint AS value FROM videos WHERE views_count > $1`
