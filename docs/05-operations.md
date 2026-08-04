# Operations Runbook

## 1. Обязательный порядок

```text
yandex_direct_loader
→ macro_mysql_to_pg_loader
→ crm_yandex_leads_matched_loader
→ daily_deal_aggregates_loader
→ yandex_criterion_performance_loader
```

Причина:

1. Сначала нужны рекламные факты и справочник кампаний.
2. Затем нужны свежие CRM-заявки и UTM.
3. После этого строится аудитный матчинг.
4. Кампанийная витрина использует свежий матчинг.
5. Детальная витрина сверяется с кампанийной.

## 2. Фактические cron в коде

| Время cron | DAG |
|---|---|
| `21:00` | `yandex_direct_loader` |
| `21:45` | `macro_mysql_to_pg_loader` |
| `22:20` | `daily_deal_aggregates_loader` |
| `22:40` | `yandex_criterion_performance_loader` |
| `01:15` | `crm_yandex_leads_matched_loader` |

Timezone внутри DAG-ов не задан.

### Вывод

Текущая конфигурация не гарантирует правильную цепочку одного расчетного цикла. Рекомендуемый production-вариант:

```text
Airflow Dataset dependencies
или
управляющий DAG с TriggerDagRunOperator/сенсорами
или
единый DAG с пятью task-группами
```

Если временно сохраняется cron, matcher должен завершиться до обеих витрин, а между этапами нужен запас на реальную длительность загрузки.

## 3. Первый запуск / полная загрузка

### Шаг 1. Проверить prerequisites

```text
Connections созданы
внешние таблицы и unique keys созданы
Variables заданы
timezone согласован
DAG-и импортируются без ошибок
```

### Шаг 2. Зафиксировать target date

Для Яндекса:

```text
yandex_direct_target_date = YYYY-MM-DD
```

Для MacroCRM — один из вариантов:

```text
macro_v2_target_date = YYYY-MM-DD
```

или config ручного запуска:

```json
{
  "target_date": "YYYY-MM-DD"
}
```

### Шаг 3. Включить full load

```text
macro_v2_full_load = true
```

Проверить:

```text
macro_mysql_full_reload_from = 2026-01-01 00:00:00
```

### Шаг 4. Запустить цепочку

Запускать следующий DAG только после успешного завершения предыдущего.

### Шаг 5. Проверить контрольные запросы

После успешной сверки вернуть:

```text
macro_v2_full_load = false
```

и очистить ручные target-date variables, если они больше не нужны.

## 4. Ежедневный запуск

В обычном режиме оба загрузчика повторно загружают текущий месяц. Это позволяет подтягивать изменения статусов и перерасчеты текущего периода.

Минимальный контроль:

```text
max date рекламных расходов
max created/modified date CRM
max lead_date matched-таблицы
сходимость daily и criterion
число unresolved-лидов
```

## 5. Backfill одной даты

Текущая реализация несимметрична:

- MacroCRM принимает `dag_run.conf.target_date`.
- Яндекс Директ не читает `dag_run.conf`; нужно временно выставить `yandex_direct_target_date`.

Для безопасного backfill:

```text
1. Зафиксировать target date в обоих источниках.
2. Решить, нужен ли full load или достаточно текущего месяца.
3. Запустить цепочку полностью.
4. Выполнить проверки.
5. Вернуть Variables в штатное состояние.
```

## 6. SQL-проверки

### Свежесть рекламных данных

```sql
SELECT
    MAX(date) AS max_date,
    COUNT(*) FILTER (
        WHERE date >= CURRENT_DATE - INTERVAL '7 days'
    ) AS rows_last_7_days
FROM yandex_campaign_costs_daily;
```

### Свежесть CRM

```sql
SELECT
    MAX(created_at::date) AS max_created_date,
    MAX(date_modified::date) AS max_modified_date,
    COUNT(*) FILTER (
        WHERE created_at::date >= CURRENT_DATE - INTERVAL '7 days'
           OR date_modified::date >= CURRENT_DATE - INTERVAL '7 days'
    ) AS changed_last_7_days
FROM macro_estate_buys;
```

### Свежесть matched-слоя

```sql
SELECT
    MAX(lead_date) AS max_lead_date,
    COUNT(*) FILTER (
        WHERE lead_date >= CURRENT_DATE - INTERVAL '7 days'
    ) AS leads_last_7_days,
    COUNT(*) FILTER (
        WHERE match_status <> 'matched_campaign'
          AND lead_date >= CURRENT_DATE - INTERVAL '7 days'
    ) AS unresolved_last_7_days
FROM crm_yandex_leads_matched;
```

### Кампанийная витрина

```sql
SELECT
    date,
    cabinet_name,
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks,
    SUM(cost) AS cost_without_vat,
    SUM(total_leads) AS total_leads,
    SUM(lead) AS target_leads
FROM daily_deal_aggregates
WHERE date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY date, cabinet_name
ORDER BY date, cabinet_name;
```

### Распределение `row_type`

```sql
SELECT
    row_type,
    COUNT(*) AS rows,
    SUM(cost) AS cost,
    SUM(total_leads) AS total_leads,
    SUM(target_leads) AS target_leads
FROM yandex_criterion_performance_daily
WHERE date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY row_type
ORDER BY row_type;
```

### Независимая сверка витрин по дате и кабинету

Встроенный CHECK детального DAG-а агрегирует данные по кабинету за весь доступный период. Для operational-контроля лучше дополнительно проверять по датам:

```sql
WITH daily AS (
    SELECT
        date,
        COALESCE(cabinet_name, '__NULL__') AS cabinet_key,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(cost) AS cost,
        SUM(total_leads) AS total_leads,
        SUM(lead) AS target_leads
    FROM daily_deal_aggregates
    GROUP BY date, COALESCE(cabinet_name, '__NULL__')
),
criterion AS (
    SELECT
        date,
        COALESCE(cabinet_name, '__NULL__') AS cabinet_key,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(cost) AS cost,
        SUM(total_leads) AS total_leads,
        SUM(target_leads) AS target_leads
    FROM yandex_criterion_performance_daily
    GROUP BY date, COALESCE(cabinet_name, '__NULL__')
)
SELECT
    COALESCE(d.date, c.date) AS date,
    COALESCE(d.cabinet_key, c.cabinet_key) AS cabinet_key,
    COALESCE(d.impressions, 0) - COALESCE(c.impressions, 0) AS diff_impressions,
    COALESCE(d.clicks, 0) - COALESCE(c.clicks, 0) AS diff_clicks,
    ROUND((COALESCE(d.cost, 0) - COALESCE(c.cost, 0))::numeric, 4) AS diff_cost,
    COALESCE(d.total_leads, 0) - COALESCE(c.total_leads, 0) AS diff_total_leads,
    COALESCE(d.target_leads, 0) - COALESCE(c.target_leads, 0) AS diff_target_leads
FROM daily d
FULL JOIN criterion c
  ON c.date = d.date
 AND c.cabinet_key = d.cabinet_key
WHERE COALESCE(d.impressions, 0) <> COALESCE(c.impressions, 0)
   OR COALESCE(d.clicks, 0) <> COALESCE(c.clicks, 0)
   OR ROUND(COALESCE(d.cost, 0)::numeric, 4) <> ROUND(COALESCE(c.cost, 0)::numeric, 4)
   OR COALESCE(d.total_leads, 0) <> COALESCE(c.total_leads, 0)
   OR COALESCE(d.target_leads, 0) <> COALESCE(c.target_leads, 0)
ORDER BY 1, 2;
```

Пустой результат означает сходимость по дате и кабинету.

## 7. Наблюдаемость

Для production желательно добавить alerts на:

```text
DAG failure
отставание max(date)
нулевые строки за ожидаемую дату
рост unresolved share
расхождение витрин
резкий скачок cost_gap
превышение времени загрузки API
```
