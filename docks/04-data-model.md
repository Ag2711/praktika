# Модель данных и метрики

## 1. Рекламные факты

### `yandex_campaign_costs_daily`

Primary key:

```text
date + cabinet_id + campaign_id
```

Поля:

```text
date
cabinet_id
campaign_id
campaign_name
campaign_type
impressions
clicks
cost
created_at
updated_at
```

Используется как единственный основной источник кампанийного расхода.

### `yandex_placement_costs_daily`

Primary key:

```text
date + cabinet_id + campaign_id + placement
```

Дополнительные поля:

```text
placement
bounces
```

Используется для площадок РСЯ, но не для прямого расчета CPL по CRM.

### `yandex_adgroup_criteria_costs_daily`

Primary key:

```text
date + cabinet_id + campaign_id + ad_group_id + criterion_id + device
```

Поля детализации:

```text
ad_group_id
ad_group_name
criterion_id
criterion
device
```

## 2. CRM

### `macro_estate_buys`

Conflict key:

```text
estate_buy_id
```

Ключевые аналитические поля:

```text
created_at
date_modified
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

### `macro_estate_buys_utm`

Conflict key:

```text
id
```

Одна заявка может иметь несколько строк UTM.

## 3. `crm_yandex_leads_matched`

Одна строка = один CRM-лид, выбранный как Яндекс-кандидат.

Основные поля:

```text
buy_id
lead_date
lead_created_at
campaign_id
campaign_name
cabinet_id
cabinet_name
project_name
status_human
status_norm
is_target
deal_id
deal_sum
utm_source
utm_medium
utm_campaign
utm_content
utm_term
parsed_campaign_id
parsed_ad_group_id
parsed_phrase_id
all_utm_text
match_status
match_method
loaded_at
```

### `is_target`

```sql
CASE WHEN status_norm IN (
  'подбор',
  'бронь',
  'сделка в работе',
  'сделка проведена',
  'сделка совершена'
) THEN 1 ELSE 0 END
```

## 4. `daily_deal_aggregates`

Зерно:

```text
date + cabinet_id + campaign_id
```

Метрики:

| Поле | Определение |
|---|---|
| `impressions` | Сумма показов кампании |
| `clicks` | Сумма кликов кампании |
| `cost` | Расход без НДС |
| `total_leads` | Все matched-лиды, кроме статуса `Удалено` |
| `lead` | Сумма `is_target`, то есть целевые лиды |
| `total_deals` | Уникальные `buy_id` с `deal_id > 0`, без фильтра по завершенному статусу |
| `total_deal_sum` | Сумма `deal_sum` всех включенных matched-лидов, без фильтра по завершенному статусу |

### Важное различие

`total_deals`/`total_deal_sum` здесь не совпадают по бизнес-правилу с `deals`/`revenue` детальной витрины.

## 5. `yandex_criterion_performance_daily`

Основные поля:

```text
date
cabinet_id
cabinet_name
campaign_id
campaign_name
campaign_type
ad_group_id
ad_group_name
criterion_id
criterion
impressions
clicks
cost
total_leads
target_leads
deals
revenue
row_type
```

### Сделки и выручка

Только статусы:

```text
сделка проведена
сделка совершена
```

Поэтому:

```text
deals = COUNT(DISTINCT buy_id по завершенным статусам)
revenue = SUM(deal_sum по завершенным статусам)
```

## 6. `row_type`

### Кампанийная витрина

| Значение | Смысл |
|---|---|
| `paid_campaign_with_leads` | Есть расход и лиды |
| `paid_campaign_no_leads` | Есть расход, лидов нет |
| `crm_leads_without_spend` | Есть лиды, расхода нет |
| `unresolved_yandex` | Яндекс-лид без распознанной кампании |
| `other` | Остальные технические случаи |

### Детальная витрина

| Значение | Смысл |
|---|---|
| `paid_criterion_with_leads` | Расход и лиды на точном criterion |
| `paid_criterion_no_leads` | Расход без лидов |
| `crm_leads_without_criterion_spend` | CRM-лиды есть, детального расхода нет |
| `crm_leads_without_criterion` | Есть группа, но criterion не распознан |
| `crm_leads_without_group` | Не распознана группа |
| `unresolved_yandex` | Не распознана кампания |
| `cost_gap` | Разница кампанийного и детального рекламного итога |
| `other` | Остальные случаи |

## 7. Стоимость

В API:

```text
IncludeVAT = NO
```

Нормализация:

```python
cost = Cost / 1_000_000
```

Во view:

```sql
cost_with_vat = cost * 1.22
```

Документация фиксирует формулу кода, а не подтверждает налоговую ставку как нормативное требование.

## 8. Рекомендуемые Superset-метрики

```sql
-- Показы
SUM(impressions)

-- Клики
SUM(clicks)

-- CTR
SUM(clicks) * 1.0 / NULLIF(SUM(impressions), 0)

-- Расход с коэффициентом из view
SUM(cost_with_vat)

-- CPC
SUM(cost_with_vat) / NULLIF(SUM(clicks), 0)

-- Все лиды
SUM(total_leads)

-- Целевые лиды
SUM(target_leads)

-- Доля целевых
SUM(target_leads) * 1.0 / NULLIF(SUM(total_leads), 0)

-- CPL
SUM(cost_with_vat) / NULLIF(SUM(total_leads), 0)

-- CPL целевого
SUM(cost_with_vat) / NULLIF(SUM(target_leads), 0)

-- Сделки
SUM(deals)

-- Выручка
SUM(revenue)

-- CPO
SUM(cost_with_vat) / NULLIF(SUM(deals), 0)

-- ROAS
SUM(revenue) / NULLIF(SUM(cost_with_vat), 0)
```

Для детальной аналитики рекомендуется подключать `v_yandex_criterion_performance_daily`.
