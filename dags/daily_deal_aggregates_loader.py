#!/usr/bin/env python3
"""
DAG для материализации витрины daily_deal_aggregates.

ИТОГОВАЯ ЛОГИКА:

1. Расходы, показы, клики:
   yandex_campaign_costs_daily

2. Лиды, целевые лиды, сделки, статусы, unresolved:
   crm_yandex_leads_matched

3. Статус "Удалено":
   - хранится в crm_yandex_leads_matched для аудита;
   - НЕ попадает в total_leads;
   - НЕ попадает в lead;
   - НЕ попадает в сделки и выручку в daily_deal_aggregates.

4. Зерно итоговой витрины:
   date + cabinet_id + campaign_id

5. Для Superset:
   SUM(impressions)
   SUM(clicks)
   SUM(cost)
   SUM(total_leads)
   SUM(lead)
   SUM(total_deals)
   SUM(total_deal_sum)

6. Важно:
   - В этом DAG нет отдельного sql_unresolved.
   - Unresolved-лиды приходят из crm_yandex_leads_matched.
   - campaign_id может быть NULL для строки "Yandex / без распознанной кампании".
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID = "postgres_default"
TABLE_NAME = "daily_deal_aggregates"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
}


def rebuild_materialized_table():
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id BIGSERIAL PRIMARY KEY,

        date DATE,

        campaign_id VARCHAR(50),
        campaign_name TEXT,

        cabinet_id VARCHAR(50),
        cabinet_name TEXT,

        placement TEXT,
        device TEXT,

        ad_group_id VARCHAR(50),
        ad_group_name TEXT,

        criterion_id VARCHAR(50),
        criterion TEXT,

        impressions BIGINT DEFAULT 0,
        clicks BIGINT DEFAULT 0,
        cost NUMERIC DEFAULT 0,

        total_leads INTEGER DEFAULT 0,
        lead INTEGER DEFAULT 0,

        total_deals INTEGER DEFAULT 0,
        total_deal_sum NUMERIC DEFAULT 0,

        status_human TEXT,

        utm_source VARCHAR(255),
        utm_medium VARCHAR(255),

        project_name TEXT,
        match_status TEXT,
        match_method VARCHAR(255),

        row_type VARCHAR(100),

        created_at TIMESTAMP DEFAULT NOW()
    );
    """
    pg_hook.run(create_sql)

    alter_sql = f"""
    ALTER TABLE {TABLE_NAME}
        ALTER COLUMN campaign_id TYPE VARCHAR(50),
        ALTER COLUMN cabinet_id TYPE VARCHAR(50),
        ALTER COLUMN ad_group_id TYPE VARCHAR(50),
        ALTER COLUMN criterion_id TYPE VARCHAR(50),
        ADD COLUMN IF NOT EXISTS utm_source VARCHAR(255),
        ADD COLUMN IF NOT EXISTS utm_medium VARCHAR(255),
        ADD COLUMN IF NOT EXISTS lead INTEGER DEFAULT 0,
        ADD COLUMN IF NOT EXISTS match_method VARCHAR(255),
        ADD COLUMN IF NOT EXISTS project_name TEXT,
        ADD COLUMN IF NOT EXISTS match_status TEXT,
        ADD COLUMN IF NOT EXISTS row_type VARCHAR(100),
        ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW();
    """
    pg_hook.run(alter_sql)

    pg_hook.run(f"TRUNCATE TABLE {TABLE_NAME} RESTART IDENTITY;")
    logging.info("Таблица %s очищена", TABLE_NAME)

    sql_main = r"""
    WITH costs_aggregated AS (
        SELECT
            ycd.date,
            ycd.campaign_id,
            MAX(ycd.campaign_name) AS campaign_name,
            ycd.cabinet_id,
            MAX(COALESCE(cab.cabinet_name, ycd.cabinet_id)) AS cabinet_name,

            SUM(ycd.impressions) AS impressions,
            SUM(ycd.clicks) AS clicks,
            SUM(ycd.cost) AS cost

        FROM yandex_campaign_costs_daily ycd
        LEFT JOIN cabinets cab
            ON cab.cabinet_id = ycd.cabinet_id

        GROUP BY
            ycd.date,
            ycd.campaign_id,
            ycd.cabinet_id
    ),

    leads_filtered AS (
        SELECT *
        FROM crm_yandex_leads_matched
        WHERE COALESCE(status_norm, '') <> 'удалено'
    ),

    leads_aggregated AS (
        SELECT
            lead_date AS date,

            campaign_id,
            campaign_name,

            cabinet_id,
            cabinet_name,

            MIN(project_name) AS project_name,

            COUNT(*) AS total_leads,

            SUM(COALESCE(is_target, 0)) AS lead,

            COUNT(DISTINCT CASE
                WHEN COALESCE(deal_id, 0) > 0
                THEN buy_id
            END) AS total_deals,

            SUM(COALESCE(deal_sum, 0)) AS total_deal_sum,

            COALESCE(
                MAX(CASE WHEN status_norm = 'сделка проведена' THEN status_human END),
                MAX(CASE WHEN status_norm = 'сделка совершена' THEN status_human END),
                MAX(CASE WHEN status_norm = 'бронь' THEN status_human END),
                MAX(CASE WHEN status_norm = 'сделка в работе' THEN status_human END),
                MAX(CASE WHEN status_norm = 'подбор' THEN status_human END),
                MODE() WITHIN GROUP (ORDER BY status_human)
            ) AS status_human,

            MIN(utm_source) AS utm_source,
            MIN(utm_medium) AS utm_medium,

            MIN(match_status) AS match_status,
            MIN(match_method) AS match_method

        FROM leads_filtered

        GROUP BY
            lead_date,
            campaign_id,
            campaign_name,
            cabinet_id,
            cabinet_name
    ),

    final_rows AS (
        SELECT
            COALESCE(c.date, l.date) AS date,

            COALESCE(c.campaign_id, l.campaign_id) AS campaign_id,
            COALESCE(c.campaign_name, l.campaign_name) AS campaign_name,

            COALESCE(c.cabinet_id, l.cabinet_id) AS cabinet_id,
            COALESCE(c.cabinet_name, l.cabinet_name) AS cabinet_name,

            COALESCE(c.impressions, 0) AS impressions,
            COALESCE(c.clicks, 0) AS clicks,
            COALESCE(c.cost, 0) AS cost,

            COALESCE(l.total_leads, 0) AS total_leads,
            COALESCE(l.lead, 0) AS lead,

            COALESCE(l.total_deals, 0) AS total_deals,
            COALESCE(l.total_deal_sum, 0) AS total_deal_sum,

            l.status_human,

            l.utm_source,
            l.utm_medium,

            l.project_name,
            l.match_status,
            l.match_method

        FROM costs_aggregated c
        FULL JOIN leads_aggregated l
            ON l.date = c.date
           AND l.campaign_id IS NOT DISTINCT FROM c.campaign_id
           AND l.cabinet_id IS NOT DISTINCT FROM c.cabinet_id
    )

    SELECT
        date,

        campaign_id::text AS campaign_id,
        campaign_name,

        cabinet_id::text AS cabinet_id,
        cabinet_name,

        ''::text AS placement,
        ''::text AS device,

        '0'::text AS ad_group_id,
        '--'::text AS ad_group_name,

        '0'::text AS criterion_id,
        '---'::text AS criterion,

        impressions,
        clicks,
        cost,

        total_leads,
        lead,

        total_deals,
        total_deal_sum,

        status_human,

        utm_source,
        utm_medium,

        project_name,
        match_status,
        match_method,

        CASE
            WHEN campaign_name = 'Yandex / без распознанной кампании'
                THEN 'unresolved_yandex'

            WHEN COALESCE(impressions, 0) = 0
              AND COALESCE(clicks, 0) = 0
              AND COALESCE(cost, 0) = 0
              AND COALESCE(total_leads, 0) > 0
                THEN 'crm_leads_without_spend'

            WHEN COALESCE(cost, 0) > 0
              AND COALESCE(total_leads, 0) = 0
                THEN 'paid_campaign_no_leads'

            WHEN COALESCE(cost, 0) > 0
              AND COALESCE(total_leads, 0) > 0
                THEN 'paid_campaign_with_leads'

            ELSE 'other'
        END AS row_type

    FROM final_rows
    """

    insert_sql = f"""
    INSERT INTO {TABLE_NAME} (
        date,

        campaign_id,
        campaign_name,

        cabinet_id,
        cabinet_name,

        placement,
        device,

        ad_group_id,
        ad_group_name,

        criterion_id,
        criterion,

        impressions,
        clicks,
        cost,

        total_leads,
        lead,

        total_deals,
        total_deal_sum,

        status_human,

        utm_source,
        utm_medium,

        project_name,
        match_status,
        match_method,

        row_type
    )
    {sql_main}
    """

    pg_hook.run(insert_sql)
    logging.info("Витрина %s пересобрана", TABLE_NAME)

    index_sql = f"""
    CREATE INDEX IF NOT EXISTS idx_daily_deal_aggregates_date
        ON {TABLE_NAME} (date);

    CREATE INDEX IF NOT EXISTS idx_daily_deal_aggregates_cabinet
        ON {TABLE_NAME} (cabinet_id);

    CREATE INDEX IF NOT EXISTS idx_daily_deal_aggregates_campaign
        ON {TABLE_NAME} (campaign_id);

    CREATE INDEX IF NOT EXISTS idx_daily_deal_aggregates_row_type
        ON {TABLE_NAME} (row_type);
    """
    pg_hook.run(index_sql)
    logging.info("Индексы %s проверены", TABLE_NAME)


with DAG(
    dag_id="daily_deal_aggregates_loader",
    default_args=default_args,
    schedule_interval="20 22 * * *",
    catchup=False,
    tags=["materialized", "aggregates", "yandex", "crm", "superset"],
    description="Витрина daily_deal_aggregates: расходы из yandex_campaign_costs_daily + лиды из crm_yandex_leads_matched без статуса Удалено",
) as dag:

    rebuild_task = PythonOperator(
        task_id="rebuild_table",
        python_callable=rebuild_materialized_table,
    )

    rebuild_task