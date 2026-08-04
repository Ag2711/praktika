# Справочник DAG-ов

## Общие параметры

Все DAG-и используют:

```text
owner = airflow
depends_on_past = False
start_date = 2025-01-01
catchup = False
retry_delay = 5 минут
```

Количество retry различается по DAG-ам.

## 1. `yandex_direct_loader`

| Параметр | Значение |
|---|---|
| Файл | `dags/yandex_direct_loader.py` |
| Cron | `0 21 * * *` |
| Retry | 2 |
| Task | `load_all_accounts` |
| Connection | `postgres_default` |
| Variables | `yandex_direct_accounts`, `macro_v2_full_load`, `yandex_direct_target_date` |

### Назначение

Для каждого аккаунта и дня получает три отчета Яндекс Директа.

### Выходы

```text
yandex_campaign_costs_daily
yandex_placement_costs_daily
yandex_adgroup_criteria_costs_daily
yandex_campaigns
```

### Особенности

- Аккаунты обрабатываются последовательно.
- Историческая загрузка идет циклом по каждому календарному дню.
- API-запрос имеет timeout 180 секунд.
- Ответы HTTP 201/202 опрашиваются до 30 раз с паузой 10 секунд.
- `Cost` делится на `1_000_000`.
- API вызывается с `IncludeVAT = NO`.
- Данные за день/кабинет очищаются только после успешного получения всех трех отчетов.
- Затем выполняется upsert.
- Таблица `yandex_campaigns` должна существовать заранее.

## 2. `macro_mysql_to_pg_loader`

| Параметр | Значение |
|---|---|
| Файл | `dags/macro_mysql_to_pg_loader.py` |
| Cron | `45 21 * * *` |
| Retry | 2 |
| Task | `load_mysql_to_pg` |
| Connections | `mysql_macrodata`, `postgres_default` |
| Variables | `macro_v2_full_load`, `macro_mysql_full_reload_from`, `macro_v2_target_date` |

### Источники

```text
estate_buys
contacts
estate_buys_utm
```

### Выходы

```text
macro_estate_buys
macro_estate_buys_utm
```

### Логика периода

Заявки выбираются, если `created_at` **или** `date_modified` попадают в период. UTM выбираются, если `date_added` **или** исходный `updated_at` попадают в период.

### Особенности

- Batch size: 500.
- Перед upsert DataFrame выравнивается по реальной схеме PostgreSQL.
- Лишние колонки отбрасываются с warning.
- Выполняется проверка `BIGINT`, `NaN` и пустых conflict keys.
- Дубли в одном batch удаляются по приоритету дат; остается последняя запись.
- Upsert keys: `estate_buy_id` и `id`.
- Целевые таблицы и уникальные ограничения должны существовать заранее.

## 3. `crm_yandex_leads_matched_loader`

| Параметр | Значение |
|---|---|
| Файл | `dags/crm_yandex_leads_matched_loader.py` |
| Cron | `15 1 * * *` |
| Retry | 1 |
| Task | `rebuild_crm_yandex_leads_matched` |
| Connection | `postgres_default` |

### Входы

```text
macro_estate_buys
macro_estate_buys_utm
yandex_campaigns
project_cabinet_map
```

### Выход

```text
crm_yandex_leads_matched
```

### Определение Яндекс-кандидата

Кандидат определяется одним из способов:

```text
source из разрешенного списка + medium из рекламного списка
или
наличие cid:/gid:/phid:/aid: в объединенном UTM-тексте
```

Разрешенные source:

```text
yandex
ya.direct
yandex_context
yandex_apartments
ya_apartments
realtyyandex
```

Разрешенные medium:

```text
cpc
cpc_search
cpc_mk
context
контекст
```

### Целевой статус

```text
подбор
бронь
сделка в работе
сделка проведена
сделка совершена
```

### Особенности

- Таблица полностью пересоздается через `DROP TABLE IF EXISTS ... CASCADE`.
- Удаленные заявки сохраняются для аудита.
- `campaign_id` извлекается из `cid:`, `campaign_id:` или `utm_campaign_id`.
- `ad_group_id` извлекается из `gid:`.
- `phrase_id` извлекается из `phid:` или `utm_phrase_id`.
- При отсутствии кампании используется fallback по `project_cabinet_map`.
- В коде есть диагностический запрос, жестко начинающийся с `2026-07-01`; это legacy-проверка.

## 4. `daily_deal_aggregates_loader`

| Параметр | Значение |
|---|---|
| Файл | `dags/daily_deal_aggregates_loader.py` |
| Cron | `20 22 * * *` |
| Retry | 2 |
| Task | `rebuild_table` |
| Connection | `postgres_default` |

### Входы

```text
yandex_campaign_costs_daily
crm_yandex_leads_matched
cabinets
```

### Выход

```text
daily_deal_aggregates
```

### Зерно

```text
date + cabinet_id + campaign_id
```

`row_type` — классификация строки, а не дополнительный ключ группировки.

### Особенности

- Таблица создается/расширяется при необходимости.
- Перед загрузкой выполняется `TRUNCATE ... RESTART IDENTITY`.
- Расходы и лиды соединяются через `FULL JOIN`.
- Статус `Удалено` исключается.
- Поле `lead` = количество целевых лидов.
- Поля `placement`, `device`, `ad_group_id`, `criterion_id` заполняются техническими значениями и не отражают реальную детализацию.

### `row_type`

```text
unresolved_yandex
crm_leads_without_spend
paid_campaign_no_leads
paid_campaign_with_leads
other
```

## 5. `yandex_criterion_performance_loader`

| Параметр | Значение |
|---|---|
| Файл | `dags/yandex_criterion_performance_loader.py` |
| Cron | `40 22 * * *` |
| Retry | 1 |
| Task | `rebuild_yandex_criterion_performance_daily` |
| Connection | `postgres_default` |

### Входы

```text
daily_deal_aggregates
yandex_adgroup_criteria_costs_daily
crm_yandex_leads_matched
```

### Выходы

```text
yandex_criterion_performance_daily
v_yandex_criterion_performance_daily
```

### Особенности

- Сначала удаляются старые view и таблица, затем таблица пересоздается.
- Детальные расходы ограничиваются датами и кампаниями из `daily_deal_aggregates`.
- `device` агрегируется.
- Criterion для CRM парсится из `phid:`, `ret:` или `dsa:`.
- Сделки и выручка считаются только по статусам `сделка проведена` и `сделка совершена`.
- `cost_gap` сохраняет разницу между кампанийным и детальным расходом.
- После построения выполняется сверка с `daily_deal_aggregates`.
- При расхождении DAG пишет список кабинетов в log и завершается с `ValueError`.

### Проверяемые суммы

```text
impressions
clicks
cost
total_leads
target_leads
```

Сделки и выручка в автоматическую сверку не входят.
