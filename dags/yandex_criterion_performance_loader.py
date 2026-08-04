#!/usr/bin/env python3
"""
DAG: yandex_criterion_performance_loader

Создает детальную витрину Яндекс Директа:

date
cabinet_name
campaign_name
ad_group_name
criterion

Метрики:

impressions
clicks
cost
total_leads
target_leads
deals
revenue
row_type

Важно:
- daily_deal_aggregates используется как контрольный верхний слой.
- Фильтр row_type в daily_deal_aggregates НЕ ставим.
- Сделки и выручку считаем только по статусам:
  - Сделка проведена
  - Сделка совершена
- Удаленные лиды не считаем.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID = "postgres_default"


CREATE_TABLE_SQL = """
DROP VIEW IF EXISTS v_yandex_criterion_performance_daily CASCADE;

DROP TABLE IF EXISTS yandex_criterion_performance_daily CASCADE;

CREATE TABLE yandex_criterion_performance_daily AS
WITH daily_campaign_totals AS (
    SELECT
        date,
        cabinet_id::text AS cabinet_id,
        cabinet_name,

        COALESCE(campaign_id::text, '0') AS campaign_id,
        COALESCE(campaign_name, 'Yandex / без распознанной кампании') AS campaign_name,

        SUM(impressions)::bigint AS campaign_impressions,
        SUM(clicks)::bigint AS campaign_clicks,
        SUM(cost)::numeric AS campaign_cost,

        SUM(total_leads)::bigint AS campaign_total_leads,
        SUM(lead)::bigint AS campaign_target_leads

    FROM daily_deal_aggregates

    GROUP BY
        date,
        cabinet_id::text,
        cabinet_name,
        COALESCE(campaign_id::text, '0'),
        COALESCE(campaign_name, 'Yandex / без распознанной кампании')
),

costs_by_criterion AS (
    SELECT
        y.date,
        y.cabinet_id::text AS cabinet_id,
        dct.cabinet_name,

        COALESCE(y.campaign_id::text, '0') AS campaign_id,
        dct.campaign_name,
        MAX(y.campaign_type)::text AS campaign_type,

        COALESCE(y.ad_group_id, 0)::bigint AS ad_group_id,
        COALESCE(MAX(y.ad_group_name), 'Группа не найдена в расходах') AS ad_group_name,

        COALESCE(y.criterion_id, 0)::bigint AS criterion_id,
        COALESCE(MAX(y.criterion), '---') AS criterion,

        SUM(y.impressions)::bigint AS impressions,
        SUM(y.clicks)::bigint AS clicks,
        SUM(y.cost)::numeric AS cost

    FROM yandex_adgroup_criteria_costs_daily y

    INNER JOIN daily_campaign_totals dct
        ON dct.date = y.date
       AND dct.cabinet_id = y.cabinet_id::text
       AND dct.campaign_id = COALESCE(y.campaign_id::text, '0')

    GROUP BY
        y.date,
        y.cabinet_id::text,
        dct.cabinet_name,
        COALESCE(y.campaign_id::text, '0'),
        dct.campaign_name,
        COALESCE(y.ad_group_id, 0)::bigint,
        COALESCE(y.criterion_id, 0)::bigint
),

costs_by_campaign AS (
    SELECT
        date,
        cabinet_id,
        campaign_id,
        SUM(impressions)::bigint AS detailed_impressions,
        SUM(clicks)::bigint AS detailed_clicks,
        SUM(cost)::numeric AS detailed_cost
    FROM costs_by_criterion
    GROUP BY
        date,
        cabinet_id,
        campaign_id
),

cost_gap AS (
    SELECT
        d.date,
        d.cabinet_id,
        d.cabinet_name,

        d.campaign_id,
        d.campaign_name,
        NULL::text AS campaign_type,

        0::bigint AS ad_group_id,
        'Расход без детализации группы / criterion'::text AS ad_group_name,

        0::bigint AS criterion_id,
        '---'::text AS criterion,

        (d.campaign_impressions - COALESCE(c.detailed_impressions, 0))::bigint AS impressions,
        (d.campaign_clicks - COALESCE(c.detailed_clicks, 0))::bigint AS clicks,
        (d.campaign_cost - COALESCE(c.detailed_cost, 0))::numeric AS cost,

        0::bigint AS total_leads,
        0::bigint AS target_leads,
        0::bigint AS deals,
        0::numeric AS revenue,

        'cost_gap'::text AS row_type

    FROM daily_campaign_totals d

    LEFT JOIN costs_by_campaign c
        ON c.date = d.date
       AND c.cabinet_id = d.cabinet_id
       AND c.campaign_id = d.campaign_id

    WHERE
        d.campaign_impressions <> COALESCE(c.detailed_impressions, 0)
        OR d.campaign_clicks <> COALESCE(c.detailed_clicks, 0)
        OR ROUND(d.campaign_cost::numeric, 4) <> ROUND(COALESCE(c.detailed_cost, 0)::numeric, 4)
),

leads_parsed AS (
    SELECT
        l.buy_id,
        l.lead_date AS date,
        l.cabinet_id::text AS cabinet_id,
        l.cabinet_name,

        COALESCE(NULLIF(l.campaign_id::text, ''), '0') AS campaign_id,
        COALESCE(l.campaign_name, 'Yandex / без распознанной кампании') AS campaign_name,

        l.status_human,
        l.status_norm,

        LOWER(
            TRIM(
                COALESCE(
                    NULLIF(l.status_human, ''),
                    NULLIF(l.status_norm, ''),
                    ''
                )
            )
        ) AS status_for_deal,

        COALESCE(l.is_target, 0)::bigint AS is_target,

        l.deal_id,
        l.deal_sum,

        NULLIF(
            substring(l.all_utm_text FROM 'gid:([0-9]+)'),
            ''
        )::bigint AS ad_group_id,

        NULLIF(
            COALESCE(
                substring(l.all_utm_text FROM 'phid:([0-9]+)'),
                substring(l.all_utm_text FROM 'ret:([0-9]+)'),
                substring(l.all_utm_text FROM 'dsa:([0-9]+)')
            ),
            ''
        )::bigint AS criterion_id

    FROM crm_yandex_leads_matched l

    WHERE LOWER(TRIM(COALESCE(l.status_norm, ''))) <> 'удалено'
),

leads_by_criterion AS (
    SELECT
        date,
        cabinet_id,
        cabinet_name,

        campaign_id,
        campaign_name,

        ad_group_id,
        criterion_id,

        COUNT(*)::bigint AS total_leads,
        SUM(is_target)::bigint AS target_leads,

        COUNT(DISTINCT CASE
            WHEN status_for_deal IN ('сделка проведена', 'сделка совершена')
            THEN buy_id
        END)::bigint AS deals,

        SUM(
            CASE
                WHEN status_for_deal IN ('сделка проведена', 'сделка совершена')
                THEN COALESCE(deal_sum, 0)
                ELSE 0
            END
        )::numeric AS revenue

    FROM leads_parsed

    WHERE campaign_id IS NOT NULL
      AND ad_group_id IS NOT NULL
      AND criterion_id IS NOT NULL

    GROUP BY
        date,
        cabinet_id,
        cabinet_name,
        campaign_id,
        campaign_name,
        ad_group_id,
        criterion_id
),

costs_with_exact_leads AS (
    SELECT
        COALESCE(c.date, l.date) AS date,
        COALESCE(c.cabinet_id, l.cabinet_id) AS cabinet_id,
        COALESCE(c.cabinet_name, l.cabinet_name) AS cabinet_name,

        COALESCE(c.campaign_id, l.campaign_id) AS campaign_id,
        COALESCE(c.campaign_name, l.campaign_name) AS campaign_name,
        c.campaign_type,

        COALESCE(c.ad_group_id, l.ad_group_id, 0)::bigint AS ad_group_id,
        COALESCE(c.ad_group_name, 'Группа не найдена в расходах') AS ad_group_name,

        COALESCE(c.criterion_id, l.criterion_id, 0)::bigint AS criterion_id,
        COALESCE(c.criterion, 'Criterion не найден в расходах') AS criterion,

        COALESCE(c.impressions, 0)::bigint AS impressions,
        COALESCE(c.clicks, 0)::bigint AS clicks,
        COALESCE(c.cost, 0)::numeric AS cost,

        COALESCE(l.total_leads, 0)::bigint AS total_leads,
        COALESCE(l.target_leads, 0)::bigint AS target_leads,
        COALESCE(l.deals, 0)::bigint AS deals,
        COALESCE(l.revenue, 0)::numeric AS revenue,

        CASE
            WHEN COALESCE(c.cost, 0) > 0 AND COALESCE(l.total_leads, 0) > 0
                THEN 'paid_criterion_with_leads'
            WHEN COALESCE(c.cost, 0) > 0 AND COALESCE(l.total_leads, 0) = 0
                THEN 'paid_criterion_no_leads'
            WHEN COALESCE(c.cost, 0) = 0 AND COALESCE(l.total_leads, 0) > 0
                THEN 'crm_leads_without_criterion_spend'
            ELSE 'other'
        END::text AS row_type

    FROM costs_by_criterion c

    FULL OUTER JOIN leads_by_criterion l
        ON l.date = c.date
       AND l.cabinet_id = c.cabinet_id
       AND l.campaign_id = c.campaign_id
       AND l.ad_group_id = c.ad_group_id
       AND l.criterion_id = c.criterion_id
),

leads_without_criterion AS (
    SELECT
        p.date,
        p.cabinet_id,
        p.cabinet_name,

        p.campaign_id,
        p.campaign_name,
        NULL::text AS campaign_type,

        p.ad_group_id,
        COALESCE(MAX(c.ad_group_name), 'CRM-лиды без распознанной группы') AS ad_group_name,

        0::bigint AS criterion_id,
        'CRM-лиды без распознанного criterion'::text AS criterion,

        0::bigint AS impressions,
        0::bigint AS clicks,
        0::numeric AS cost,

        COUNT(*)::bigint AS total_leads,
        SUM(p.is_target)::bigint AS target_leads,

        COUNT(DISTINCT CASE
            WHEN p.status_for_deal IN ('сделка проведена', 'сделка совершена')
            THEN p.buy_id
        END)::bigint AS deals,

        SUM(
            CASE
                WHEN p.status_for_deal IN ('сделка проведена', 'сделка совершена')
                THEN COALESCE(p.deal_sum, 0)
                ELSE 0
            END
        )::numeric AS revenue,

        'crm_leads_without_criterion'::text AS row_type

    FROM leads_parsed p

    LEFT JOIN costs_by_criterion c
        ON c.date = p.date
       AND c.cabinet_id = p.cabinet_id
       AND c.campaign_id = p.campaign_id
       AND c.ad_group_id = p.ad_group_id

    WHERE p.ad_group_id IS NOT NULL
      AND p.criterion_id IS NULL

    GROUP BY
        p.date,
        p.cabinet_id,
        p.cabinet_name,
        p.campaign_id,
        p.campaign_name,
        p.ad_group_id
),

leads_without_group AS (
    SELECT
        p.date,
        p.cabinet_id,
        p.cabinet_name,

        p.campaign_id,
        p.campaign_name,
        NULL::text AS campaign_type,

        0::bigint AS ad_group_id,

        CASE
            WHEN p.campaign_name = 'Yandex / без распознанной кампании'
                THEN 'Yandex / без распознанной кампании'
            ELSE 'CRM-лиды без распознанной группы'
        END::text AS ad_group_name,

        0::bigint AS criterion_id,
        '---'::text AS criterion,

        0::bigint AS impressions,
        0::bigint AS clicks,
        0::numeric AS cost,

        COUNT(*)::bigint AS total_leads,
        SUM(p.is_target)::bigint AS target_leads,

        COUNT(DISTINCT CASE
            WHEN p.status_for_deal IN ('сделка проведена', 'сделка совершена')
            THEN p.buy_id
        END)::bigint AS deals,

        SUM(
            CASE
                WHEN p.status_for_deal IN ('сделка проведена', 'сделка совершена')
                THEN COALESCE(p.deal_sum, 0)
                ELSE 0
            END
        )::numeric AS revenue,

        CASE
            WHEN p.campaign_name = 'Yandex / без распознанной кампании'
                THEN 'unresolved_yandex'
            ELSE 'crm_leads_without_group'
        END::text AS row_type

    FROM leads_parsed p

    WHERE p.ad_group_id IS NULL

    GROUP BY
        p.date,
        p.cabinet_id,
        p.cabinet_name,
        p.campaign_id,
        p.campaign_name
)

SELECT * FROM costs_with_exact_leads

UNION ALL

SELECT * FROM leads_without_criterion

UNION ALL

SELECT * FROM leads_without_group

UNION ALL

SELECT * FROM cost_gap;
"""


CREATE_INDEXES_SQL = """
CREATE INDEX IF NOT EXISTS idx_yandex_criterion_perf_date
    ON yandex_criterion_performance_daily (date);

CREATE INDEX IF NOT EXISTS idx_yandex_criterion_perf_cabinet
    ON yandex_criterion_performance_daily (cabinet_id);

CREATE INDEX IF NOT EXISTS idx_yandex_criterion_perf_cabinet_name
    ON yandex_criterion_performance_daily (cabinet_name);

CREATE INDEX IF NOT EXISTS idx_yandex_criterion_perf_campaign
    ON yandex_criterion_performance_daily (campaign_id);

CREATE INDEX IF NOT EXISTS idx_yandex_criterion_perf_campaign_name
    ON yandex_criterion_performance_daily (campaign_name);

CREATE INDEX IF NOT EXISTS idx_yandex_criterion_perf_group
    ON yandex_criterion_performance_daily (ad_group_id);

CREATE INDEX IF NOT EXISTS idx_yandex_criterion_perf_group_name
    ON yandex_criterion_performance_daily (ad_group_name);

CREATE INDEX IF NOT EXISTS idx_yandex_criterion_perf_criterion
    ON yandex_criterion_performance_daily (criterion_id);

CREATE INDEX IF NOT EXISTS idx_yandex_criterion_perf_row_type
    ON yandex_criterion_performance_daily (row_type);
"""


CREATE_VIEWS_SQL = """
CREATE OR REPLACE VIEW v_yandex_criterion_performance_daily AS
SELECT
    date,
    EXTRACT(YEAR FROM date)::int AS year,
    EXTRACT(MONTH FROM date)::int AS month,
    DATE_TRUNC('month', date)::date AS month_start,
    TO_CHAR(DATE_TRUNC('month', date), 'YYYY-MM') AS year_month,
    EXTRACT(QUARTER FROM date)::int AS quarter,

    cabinet_id,
    cabinet_name,

    campaign_id,
    campaign_name,
    campaign_type,

    ad_group_id,
    ad_group_name,

    criterion_id,
    criterion,

    impressions,
    clicks,
    cost,
    cost * 1.22 AS cost_with_vat,

    total_leads,
    target_leads,

    deals,
    revenue,

    row_type

FROM yandex_criterion_performance_daily;
"""


CHECK_SQL = """
WITH daily AS (
    SELECT
        COALESCE(cabinet_name, '__NULL_CABINET__') AS cabinet_key,
        cabinet_name,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(cost) AS cost,
        SUM(total_leads) AS total_leads,
        SUM(lead) AS target_leads
    FROM daily_deal_aggregates
    GROUP BY
        COALESCE(cabinet_name, '__NULL_CABINET__'),
        cabinet_name
),

criterion AS (
    SELECT
        COALESCE(cabinet_name, '__NULL_CABINET__') AS cabinet_key,
        cabinet_name,
        SUM(impressions) AS impressions,
        SUM(clicks) AS clicks,
        SUM(cost) AS cost,
        SUM(total_leads) AS total_leads,
        SUM(target_leads) AS target_leads
    FROM yandex_criterion_performance_daily
    GROUP BY
        COALESCE(cabinet_name, '__NULL_CABINET__'),
        cabinet_name
)

SELECT
    COALESCE(d.cabinet_name, c.cabinet_name, 'Без ЖК') AS cabinet_name,

    COALESCE(d.impressions, 0) AS daily_impressions,
    COALESCE(c.impressions, 0) AS criterion_impressions,
    COALESCE(d.impressions, 0) - COALESCE(c.impressions, 0) AS diff_impressions,

    COALESCE(d.clicks, 0) AS daily_clicks,
    COALESCE(c.clicks, 0) AS criterion_clicks,
    COALESCE(d.clicks, 0) - COALESCE(c.clicks, 0) AS diff_clicks,

    ROUND(COALESCE(d.cost, 0)::numeric, 4) AS daily_cost,
    ROUND(COALESCE(c.cost, 0)::numeric, 4) AS criterion_cost,
    ROUND((COALESCE(d.cost, 0) - COALESCE(c.cost, 0))::numeric, 4) AS diff_cost,

    COALESCE(d.total_leads, 0) AS daily_total_leads,
    COALESCE(c.total_leads, 0) AS criterion_total_leads,
    COALESCE(d.total_leads, 0) - COALESCE(c.total_leads, 0) AS diff_total_leads,

    COALESCE(d.target_leads, 0) AS daily_target_leads,
    COALESCE(c.target_leads, 0) AS criterion_target_leads,
    COALESCE(d.target_leads, 0) - COALESCE(c.target_leads, 0) AS diff_target_leads

FROM daily d
FULL OUTER JOIN criterion c
    ON c.cabinet_key = d.cabinet_key

WHERE
    COALESCE(d.impressions, 0) <> COALESCE(c.impressions, 0)
    OR COALESCE(d.clicks, 0) <> COALESCE(c.clicks, 0)
    OR ROUND(COALESCE(d.cost, 0)::numeric, 4) <> ROUND(COALESCE(c.cost, 0)::numeric, 4)
    OR COALESCE(d.total_leads, 0) <> COALESCE(c.total_leads, 0)
    OR COALESCE(d.target_leads, 0) <> COALESCE(c.target_leads, 0)

ORDER BY COALESCE(d.cabinet_name, c.cabinet_name, 'Без ЖК');
"""


def rebuild_yandex_criterion_performance() -> None:
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    logging.info("Пересоздаем yandex_criterion_performance_daily")
    pg_hook.run(CREATE_TABLE_SQL)

    logging.info("Создаем индексы yandex_criterion_performance_daily")
    pg_hook.run(CREATE_INDEXES_SQL)

    logging.info("Создаем view v_yandex_criterion_performance_daily")
    pg_hook.run(CREATE_VIEWS_SQL)

    logging.info("Проверяем расхождения с daily_deal_aggregates")
    rows = pg_hook.get_records(CHECK_SQL)

    if rows:
        logging.warning("Есть расхождения между daily_deal_aggregates и yandex_criterion_performance_daily:")
        for row in rows:
            logging.warning(row)

        raise ValueError(
            "Витрина yandex_criterion_performance_daily не сошлась с daily_deal_aggregates. "
            "Проверь логи DAG: там список ЖК с расхождениями."
        )

    logging.info("Проверка успешна: yandex_criterion_performance_daily сходится с daily_deal_aggregates")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
}


with DAG(
    dag_id="yandex_criterion_performance_loader",
    default_args=default_args,
    schedule_interval="40 22 * * *",
    catchup=False,
    tags=["yandex", "direct", "performance", "criterion"],
    description="Детальная витрина Яндекс Директа до уровня campaign/ad_group/criterion с лидами, сделками и выручкой",
) as dag:

    rebuild_task = PythonOperator(
        task_id="rebuild_yandex_criterion_performance_daily",
        python_callable=rebuild_yandex_criterion_performance,
    )

    rebuild_task