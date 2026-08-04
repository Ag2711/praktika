# Архитектура и поток данных

## 1. Границы системы

Документация относится к пяти переданным DAG-ам:

```text
yandex_direct_loader
macro_mysql_to_pg_loader
crm_yandex_leads_matched_loader
daily_deal_aggregates_loader
yandex_criterion_performance_loader
```

Пайплайн получает данные из двух источников:

- Яндекс Директ API;
- MySQL базы MacroCRM.

Результат хранится в PostgreSQL и предназначен прежде всего для Superset.

## 2. Слои

### 2.1. Слой рекламных фактов

`yandex_direct_loader` запрашивает для каждого кабинета и каждого дня три отчета:

```text
CAMPAIGN_PERFORMANCE_REPORT без Placement
CAMPAIGN_PERFORMANCE_REPORT с Placement
CUSTOM_REPORT до AdGroup / Criterion / Device
```

Результаты разделены по зерну:

```text
yandex_campaign_costs_daily
yandex_placement_costs_daily
yandex_adgroup_criteria_costs_daily
```

Это предотвращает задвоение показов, кликов и расходов при последующей аналитике.

### 2.2. Слой CRM

`macro_mysql_to_pg_loader` читает:

```text
estate_buys + contacts
estate_buys_utm
```

и выполняет upsert в:

```text
macro_estate_buys
macro_estate_buys_utm
```

Целевые таблицы должны существовать заранее.

### 2.3. Аудитный слой матчинга

`crm_yandex_leads_matched_loader` создает одну строку на CRM-лид, признанный кандидатом на Яндекс.

Алгоритм:

1. Нормализовать UTM: lower-case, trim, замена `ё` на `е`.
2. Собрать UTM и channel-поля в `all_utm_text_norm`.
3. Для каждого `estate_buy_id` выбрать одну лучшую UTM-строку.
4. Определить Яндекс-кандидата по source/medium либо наличию `cid`, `gid`, `phid`, `aid`.
5. Распарсить `campaign_id`, `ad_group_id`, `phrase_id`.
6. Найти кампанию в `yandex_campaigns`.
7. При неуспехе определить ЖК/кабинет через `project_cabinet_map`.
8. Сохранить как распознанный или unresolved-лид.

### 2.4. Кампанийный контрольный слой

`daily_deal_aggregates_loader` объединяет:

```text
yandex_campaign_costs_daily
FULL JOIN
crm_yandex_leads_matched без статуса Удалено
```

Связка выполняется по:

```text
date
cabinet_id
campaign_id
```

Используется `IS NOT DISTINCT FROM`, поэтому строки с `NULL campaign_id` могут корректно объединяться.

### 2.5. Детальный слой

`yandex_criterion_performance_loader` раскладывает кампанийные итоги до:

```text
cabinet → campaign → ad_group → criterion
```

При этом:

- расходы по `device` предварительно агрегируются;
- лиды с `gid` и criterion приклеиваются точно;
- лиды без criterion и без группы сохраняются отдельными служебными строками;
- недостающая детализация расходов попадает в `cost_gap`;
- итог по показам, кликам, расходу и лидам сверяется с `daily_deal_aggregates`.

## 3. Lineage

| Результат | Прямые источники | Потребители |
|---|---|---|
| `yandex_campaign_costs_daily` | Яндекс Директ API | `daily_deal_aggregates` |
| `yandex_placement_costs_daily` | Яндекс Директ API | Отчеты по площадкам РСЯ |
| `yandex_adgroup_criteria_costs_daily` | Яндекс Директ API | `yandex_criterion_performance_daily` |
| `macro_estate_buys` | MacroCRM MySQL | `crm_yandex_leads_matched` |
| `macro_estate_buys_utm` | MacroCRM MySQL | `crm_yandex_leads_matched` |
| `yandex_campaigns` | Яндекс Директ API | `crm_yandex_leads_matched` |
| `project_cabinet_map` | Внешний справочник | `crm_yandex_leads_matched` |
| `crm_yandex_leads_matched` | CRM + UTM + справочники | Обе итоговые витрины |
| `daily_deal_aggregates` | Кампанийные расходы + matched leads | Контроль, Superset, детальная витрина |
| `yandex_criterion_performance_daily` | Контрольный слой + детализация + matched leads | View и Superset |
| `v_yandex_criterion_performance_daily` | Детальная таблица | Superset |

## 4. Match status

`crm_yandex_leads_matched` использует три значения:

| `match_status` | Значение |
|---|---|
| `matched_campaign` | `campaign_id` найден в `yandex_campaigns` |
| `unresolved_campaign_cabinet_matched` | Кампания не найдена, но ЖК/кабинет определен по `project_cabinet_map` |
| `unresolved_campaign` | Не определены ни кампания, ни кабинет |

Соответствующие методы:

```text
campaign_id_from_utm_tokens
campaign_not_resolved / cabinet_by_project_key
campaign_not_resolved / cabinet_not_resolved
```

## 5. Правила выбора UTM

На одну заявку берется только одна UTM-строка.

Приоритет:

1. Явный Яндекс-source либо UTM с `cid`/`gid`.
2. Самая свежая `date_added`.
3. Самая свежая `updated_at`.
4. Наибольший `id`.

Это аудитная модель с одной выбранной атрибуцией, а не полноценная multi-touch модель.

## 6. Служебные строки детальной витрины

Для сохранения сходимости используются строки:

```text
Yandex / без распознанной кампании
Группа не найдена в расходах
Criterion не найден в расходах
CRM-лиды без распознанного criterion
CRM-лиды без распознанной группы
Расход без детализации группы / criterion
```

Их нельзя безусловно отфильтровывать из контрольных отчетов: иначе суммы перестанут сходиться с кампанийным слоем.

## 7. Обновление объектов

| Объект | Способ обновления |
|---|---|
| Три таблицы Яндекс Директа | Очистка дня и кабинета после успешного получения трех отчетов, затем upsert |
| `macro_estate_buys` | Batch upsert по `estate_buy_id` |
| `macro_estate_buys_utm` | Batch upsert по `id` |
| `crm_yandex_leads_matched` | `DROP TABLE ... CASCADE` и полное пересоздание |
| `daily_deal_aggregates` | Создание/alter при необходимости, затем `TRUNCATE` и полная пересборка |
| `yandex_criterion_performance_daily` | Drop/recreate; view также пересоздается |

Особенности и риски этого подхода перечислены в [`07-known-limitations.md`](07-known-limitations.md).
