# Конфигурация

## 1. Airflow Connections

### `postgres_default`

Целевая PostgreSQL. Используется всеми пятью DAG-ами.

Необходимые права:

```text
SELECT
INSERT
UPDATE
DELETE
CREATE TABLE
ALTER TABLE
DROP TABLE
CREATE INDEX
CREATE VIEW
DROP VIEW
TRUNCATE
```

Из-за `DROP TABLE ... CASCADE` и создания view права должны быть проверены особенно внимательно.

### `mysql_macrodata`

Источник MacroCRM MySQL. Используется `macro_mysql_to_pg_loader`.

Необходимые права чтения как минимум на:

```text
estate_buys
contacts
estate_buys_utm
```

## 2. Airflow Variables

| Variable | Тип | Используется в | Поведение |
|---|---|---|---|
| `yandex_direct_accounts` | JSON array | `yandex_direct_loader` | Обязательный список аккаунтов |
| `macro_v2_full_load` | bool/string | Яндекс + MacroCRM loaders | Общий переключатель полной загрузки |
| `yandex_direct_target_date` | `YYYY-MM-DD` или пусто | `yandex_direct_loader` | При пустом значении используется `data_interval_end` |
| `macro_v2_target_date` | `YYYY-MM-DD` или пусто | `macro_mysql_to_pg_loader` | Второй приоритет после `dag_run.conf.target_date` |
| `macro_mysql_full_reload_from` | timestamp string | `macro_mysql_to_pg_loader` | Начало полной загрузки; default `2026-01-01 00:00:00` |

Значения `true`, `1`, `yes`, `y`, `да` распознаются как истина для full load.

### Пример `yandex_direct_accounts`

```json
[
  {
    "login": "e-XXXXXXXX",
    "token": "__SECRET__",
    "cabinet_id": "e-XXXXXXXX"
  }
]
```

В репозитории хранится только обезличенный пример. Реальный token должен находиться в Airflow/Secrets Backend.

## 3. Определение target date

### Яндекс Директ

Приоритет:

```text
1. Airflow Variable yandex_direct_target_date
2. context["data_interval_end"]
```

`dag_run.conf.target_date` в текущем коде Яндекс DAG-а не поддерживается.

### MacroCRM

Приоритет:

```text
1. dag_run.conf["target_date"]
2. Airflow Variable macro_v2_target_date
3. текущая дата в Asia/Yekaterinburg
```

Пример ручного config:

```json
{
  "target_date": "2026-08-03"
}
```

## 4. Режимы загрузки

### `macro_v2_full_load = true`

- Яндекс: с `2026-01-01` до `target_date` включительно.
- MacroCRM: с `macro_mysql_full_reload_from` до следующего дня после `target_date`, правая граница не включается.

### `macro_v2_full_load = false`

Оба загрузчика обрабатывают текущий месяц: с первого числа месяца `target_date` до `target_date`.

> Один общий переключатель управляет двумя независимыми источниками. Это текущая логика кода, но для production рекомендуется разделить переменные.

## 5. Внешние PostgreSQL-объекты

Эти объекты не создаются переданными DAG-ами и должны существовать заранее.

### `macro_estate_buys`

Минимально используются колонки:

```text
estate_buy_id                         -- conflict key / unique
created_at
date_modified
updated_at
status_name
custom_status_name
deal_id
deal_sum
channel_type
channel_name
channel_medium
utm_source
utm_medium
utm_campaign
utm_content
```

Фактический загрузчик также передает дополнительные поля из `estate_buys` и `contacts`. Схема должна соответствовать DataFrame либо лишние колонки будут отброшены.

### `macro_estate_buys_utm`

Минимально используются:

```text
id                                    -- conflict key / unique
estate_buy_id
date_added
updated_at
channel_type
channel_name
channel_medium
utm_source
utm_medium
utm_campaign
utm_content
utm_term
utm_keyword
utm_campaign_id
utm_ad_id
utm_phrase_id
```

### `yandex_campaigns`

Минимально используются:

```text
campaign_id                           -- conflict key / unique
campaign_name
ad_network_type
cabinet_id
```

### `cabinets`

```text
cabinet_id
cabinet_name
```

### `project_cabinet_map`

```text
project_key
project_name
cabinet_id
cabinet_name
```

`project_key` должен быть нормализован в нижнем регистре, потому что UTM нормализуются, а значение справочника в SQL дополнительно не приводится к lower-case.

## 6. Объекты, создаваемые DAG-ами

Автоматически создаются:

```text
yandex_campaign_costs_daily
yandex_placement_costs_daily
yandex_adgroup_criteria_costs_daily
crm_yandex_leads_matched
daily_deal_aggregates
yandex_criterion_performance_daily
v_yandex_criterion_performance_daily
```

## 7. Runtime-зависимости

Из импортов исходного кода следуют как минимум:

```text
Apache Airflow 2.x
Python 3.10+
apache-airflow-providers-postgres
apache-airflow-providers-mysql
pandas
numpy
pendulum
requests
SQLAlchemy
```

Точные версии в переданных файлах не закреплены. IT должен согласовать их с используемым Airflow image/constraints file.

## 8. Безопасность

- Не коммитить реальные OAuth-токены.
- Не коммитить Connection URI и пароли.
- Ограничить доступ DAG-а MacroCRM к персональным данным.
- Проверить, не выводятся ли PII в Airflow logs при ошибках batch-загрузки.
- Для `yandex_direct_accounts` предпочтителен Secrets Backend либо отдельная стратегия получения токена, а не секрет внутри экспортируемого Variables JSON.
