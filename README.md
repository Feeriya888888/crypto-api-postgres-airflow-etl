# End-to-End API to DWH Data Pipeline (Crypto Market Analytics)

Производственный пет-проект : регулярный пакетный сбор рыночных данных по криптовалютам из открытого **CoinGecko REST API** (по схеме датасета Kaggle *Cryptocurrency Historical Prices*), очистка, проверка качества данных (Data Quality) и построение аналитических витрин в **PostgreSQL 17** под оркестрацией **Apache Airflow**.

## Архитектура и слои данных (Medallion Architecture)

```
[ CoinGecko REST API ] ──> ( Airflow Task ) ──> [ raw.crypto_prices_raw ]  (Сырой JSON-слепок)
                                                       │
                                                       ▼
                                                [ core.fact_crypto_daily ] (Очищенные факты, DQ-проверки)
                                                       │
                                                       ▼
                                                [ mart.crypto_analytics ]  (Витрина: MA_7, доходность, ранги)
```

1. **RAW (`raw.crypto_prices_raw`)** — хранение необработанных ответов API с привязкой к дате батча (`batch_date`). Позволяет пересчитать витрины без повторных запросов к внешнему источнику.
2. **CORE (`core.fact_crypto_daily`)** — нормализованная таблица фактов. Отфильтровывает некорректные котировки (`price <= 0`, `volume < 0`), удаляет дубликаты по паре `(coin_id, price_date)` и рассчитывает дневной размах цены (`price_spread`).
3. **MART (`mart.crypto_analytics`)** — витрина для BI-инструментов (Superset / Metabase) с расчётом 7-дневной скользящей средней (`ma_7_price`), дневной процентной доходности (`daily_return_pct`) и ранга монет по капитализации (`market_cap_rank`).

## Стек технологий
* **Python 3.11** (`requests`, `pandas`, `sqlalchemy`, `psycopg2`)
* **PostgreSQL 17** (оконные функции, хранимые процедуры, атомарные транзакции)
* **Apache Airflow 2.10** (TaskFlow API, `LocalExecutor`, поддержка Backfill)
* **Docker & Docker Compose** (полная контейнеризация окружения)

## Пошаговый запуск проекта

1. Создайте файл переменных окружения:
   ```bash
   cp .env.example .env
   ```
2. Запустите инфраструктуру в Docker:
   ```bash
   docker-compose up -d --build
   ```
3. Откройте веб-интерфейс Airflow по адресу `http://localhost:8080` (логин и пароль: `admin` / `admin`) и активируйте DAG `crypto_api_to_dwh_daily`.
4. Для ручного прогона за конкретную дату (Backfill) выполните:
   ```bash
   docker exec -it crypto_airflow_scheduler airflow dags trigger -e 2026-11-10 crypto_api_to_dwh_daily
   ```

## Примеры проверочных SQL-запросов

```sql
-- Проверка топ-5 монет по капитализации и 7-дневной скользящей средней
SELECT
    price_date,
    coin_id,
    symbol,
    close_price_usd,
    ma_7_price,
    daily_return_pct,
    market_cap_rank
FROM mart.crypto_analytics
ORDER BY price_date DESC, market_cap_rank ASC
LIMIT 5;
```
