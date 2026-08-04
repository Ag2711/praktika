# Troubleshooting

## 1. Расходы есть, лидов нет

Проверить свежесть:

```sql
SELECT MAX(lead_date) FROM crm_yandex_leads_matched;
```

Если matched-таблица отстает:

```text
1. Проверить macro_estate_buys и macro_estate_buys_utm.
2. Запустить crm_yandex_leads_matched_loader.
3. Запустить daily_deal_aggregates_loader.
4. Запустить yandex_criterion_performance_loader.
```

## 2. Много `unresolved_yandex`

Проверить поля:

```text
utm_source
utm_medium
utm_campaign
utm_content
utm_term
utm_campaign_id
all_utm_text
```

Проверить наличие и актуальность:

```text
yandex_campaigns
project_cabinet_map
```

Диагностический запрос:

```sql
SELECT
    lead_date,
    buy_id,
    campaign_name,
    cabinet_name,
    parsed_campaign_id,
    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    utm_term,
    match_status,
    match_method
FROM crm_yandex_leads_matched
WHERE match_status <> 'matched_campaign'
ORDER BY lead_date DESC, buy_id DESC
LIMIT 200;
```

## 3. Лид попал не в тот ЖК

Проверить:

```text
utm_term
utm_campaign
utm_content
channel_name
project_cabinet_map.project_key
```

`project_key` должен быть нижнего регистра и достаточно специфичным. Более длинный ключ выигрывает только после уровня совпадения, заданного SQL-приоритетом.

## 4. Детальная витрина не сходится с кампанийной

DAG сам завершится ошибкой и выведет кабинеты с расхождениями.

Проверить:

```text
1. daily_deal_aggregates обновлена раньше criterion DAG.
2. yandex_adgroup_criteria_costs_daily содержит нужную дату.
3. Нет ли неожиданного cost_gap.
4. Не изменились ли ID/типы данных.
5. Сходятся ли данные по дате, а не только по кабинету за весь период.
```

## 5. Ошибка upsert MacroCRM

Частые причины:

```text
нет unique constraint по conflict key
колонка отсутствует в PostgreSQL
тип не вмещает значение
пустой estate_buy_id или id
NaN дошел до вставки
```

Проверить DDL и Airflow logs. Код выводит примеры дублей и проблемных BIGINT.

## 6. Яндекс DAG не загрузил дату

Проверить:

```text
yandex_direct_target_date
data_interval_end
timezone Airflow
macro_v2_full_load
доступность OAuth token
ответы API 201/202/ошибки HTTP
```

Учесть: `dag_run.conf.target_date` для Яндекс DAG-а не используется.

## 7. MacroCRM загрузил не ту дату

Порядок выбора даты:

```text
dag_run.conf.target_date
→ macro_v2_target_date
→ текущая дата Asia/Yekaterinburg
```

Проверить, не осталось ли старое значение `macro_v2_target_date`.

## 8. View пропала

`yandex_criterion_performance_loader` сначала выполняет:

```sql
DROP VIEW IF EXISTS v_yandex_criterion_performance_daily CASCADE;
DROP TABLE IF EXISTS yandex_criterion_performance_daily CASCADE;
```

Если DAG упал после drop и до создания view, объект будет отсутствовать до успешного перезапуска.

## 9. Зависимый объект пропал после matched DAG

`crm_yandex_leads_matched_loader` выполняет `DROP TABLE ... CASCADE`. Любые внешние views, вручную созданные поверх таблицы, могут быть удалены.

## 10. Значения сделок отличаются между витринами

Это ожидаемо для текущего кода:

- `daily_deal_aggregates.total_deals` считает любой `deal_id > 0`.
- `yandex_criterion_performance_daily.deals` считает только завершенные статусы.

До использования в едином dashboard IT и бизнес должны согласовать одно определение.

## 11. Лиды Яндекс.Недвижимости отсутствуют

В matcher source `realtyyandex`/`yandex_apartments` сам по себе недостаточен: первая ветка также требует medium из рекламного списка, а альтернативная ветка требует динамический Яндекс-token. Проверить фактические UTM. Это ограничение текущего SQL.
