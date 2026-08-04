#!/usr/bin/env python3
"""
DAG: crm_yandex_leads_matched_loader

Назначение:
Создает аудитную таблицу crm_yandex_leads_matched.

Одна строка = один CRM-лид из MacroCRM, который относится к Яндекс-рекламе
или имеет Яндекс/Яндекс.Недвижимость UTM.

Эта таблица нужна для:
1. daily_deal_aggregates
2. yandex_criterion_performance_daily
3. аудита связки CRM ↔ Яндекс Директ

Источники:
- macro_estate_buys
- macro_estate_buys_utm
- yandex_campaigns
- project_cabinet_map

Логика:
1. Берем CRM-заявки.
2. Подтягиваем лучшую UTM-строку по каждой заявке.
3. Определяем, относится ли заявка к Яндексу.
4. Парсим campaign_id из UTM:
   - cid:
   - utm_campaign_id
   - campaign_id:
5. Матчим campaign_id с yandex_campaigns.
6. Если campaign_id не найден, но видно, что это Яндекс, пытаемся определить ЖК/кабинет по project_cabinet_map.
7. Удаленные заявки НЕ удаляем из crm_yandex_leads_matched.
   Они остаются для аудита.
   Исключаются уже на уровне daily_deal_aggregates.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


POSTGRES_CONN_ID = "postgres_default"


CREATE_TABLE_SQL = """
DROP TABLE IF EXISTS crm_yandex_leads_matched CASCADE;

CREATE TABLE crm_yandex_leads_matched AS
WITH cabinet_names AS (
    SELECT
        cabinet_id::text AS cabinet_id,
        MAX(cabinet_name)::text AS cabinet_name
    FROM project_cabinet_map
    GROUP BY cabinet_id::text
),

utm_ranked AS (
    SELECT
        u.*,

        LOWER(TRIM(REPLACE(COALESCE(u.utm_source, ''), 'ё', 'е'))) AS utm_source_norm,
        LOWER(TRIM(REPLACE(COALESCE(u.utm_medium, ''), 'ё', 'е'))) AS utm_medium_norm,
        LOWER(TRIM(REPLACE(COALESCE(u.utm_campaign, ''), 'ё', 'е'))) AS utm_campaign_norm,
        LOWER(TRIM(REPLACE(COALESCE(u.utm_content, ''), 'ё', 'е'))) AS utm_content_norm,
        LOWER(TRIM(REPLACE(COALESCE(u.utm_term, ''), 'ё', 'е'))) AS utm_term_norm,

        LOWER(
            REPLACE(
                CONCAT_WS('|',
                    COALESCE(u.channel_type, ''),
                    COALESCE(u.channel_name, ''),
                    COALESCE(u.channel_medium, ''),
                    COALESCE(u.utm_source, ''),
                    COALESCE(u.utm_medium, ''),
                    COALESCE(u.utm_campaign, ''),
                    COALESCE(u.utm_content, ''),
                    COALESCE(u.utm_term, ''),
                    COALESCE(u.utm_keyword, ''),
                    COALESCE(u.utm_campaign_id, ''),
                    COALESCE(u.utm_ad_id, ''),
                    COALESCE(u.utm_phrase_id, '')
                ),
                'ё',
                'е'
            )
        ) AS all_utm_text_norm,

        ROW_NUMBER() OVER (
            PARTITION BY u.estate_buy_id
            ORDER BY
                CASE
                    WHEN LOWER(TRIM(COALESCE(u.utm_source, ''))) IN (
                        'yandex',
                        'ya.direct',
                        'yandex_context',
                        'yandex_apartments',
                        'ya_apartments',
                        'realtyyandex'
                    )
                    THEN 1

                    WHEN LOWER(COALESCE(u.utm_campaign, '')) LIKE '%cid:%'
                      OR LOWER(COALESCE(u.utm_content, '')) LIKE '%cid:%'
                      OR LOWER(COALESCE(u.utm_campaign, '')) LIKE '%gid:%'
                      OR LOWER(COALESCE(u.utm_content, '')) LIKE '%gid:%'
                    THEN 1

                    ELSE 2
                END,
                u.date_added DESC NULLS LAST,
                u.updated_at DESC NULLS LAST,
                u.id DESC NULLS LAST
        ) AS rn

    FROM macro_estate_buys_utm u
),

utm_best AS (
    SELECT *
    FROM utm_ranked
    WHERE rn = 1
),

crm_base AS (
    SELECT
        b.estate_buy_id AS buy_id,

        b.created_at::date AS lead_date,
        b.created_at AS lead_created_at,
        b.date_modified,
        b.updated_at AS buy_updated_at,

        COALESCE(
            NULLIF(b.custom_status_name, ''),
            NULLIF(b.status_name, ''),
            ''
        )::text AS status_human,

        LOWER(
            TRIM(
                COALESCE(
                    NULLIF(b.custom_status_name, ''),
                    NULLIF(b.status_name, ''),
                    ''
                )
            )
        )::text AS status_norm,

        CASE
            WHEN LOWER(
                TRIM(
                    COALESCE(
                        NULLIF(b.custom_status_name, ''),
                        NULLIF(b.status_name, ''),
                        ''
                    )
                )
            ) IN (
                'подбор',
                'бронь',
                'сделка в работе',
                'сделка проведена',
                'сделка совершена'
            )
            THEN 1
            ELSE 0
        END::int AS is_target,

        b.deal_id,
        COALESCE(b.deal_sum, 0)::numeric AS deal_sum,

        b.channel_type AS buy_channel_type,
        b.channel_name AS buy_channel_name,
        b.channel_medium AS buy_channel_medium,

        b.utm_source AS buy_utm_source,
        b.utm_medium AS buy_utm_medium,
        b.utm_campaign AS buy_utm_campaign,
        b.utm_content AS buy_utm_content,

        u.id AS utm_row_id,
        u.date_added AS utm_date_added,
        u.updated_at AS utm_updated_at,

        u.channel_type,
        u.channel_name,
        u.channel_medium,

        u.utm_source,
        u.utm_medium,
        u.utm_campaign,
        u.utm_content,
        u.utm_term,
        u.utm_keyword,
        u.utm_campaign_id,
        u.utm_ad_id,
        u.utm_phrase_id,

        u.utm_source_norm,
        u.utm_medium_norm,
        u.utm_campaign_norm,
        u.utm_content_norm,
        u.utm_term_norm,
        u.all_utm_text_norm

    FROM macro_estate_buys b

    LEFT JOIN utm_best u
        ON u.estate_buy_id = b.estate_buy_id

    WHERE b.created_at IS NOT NULL
),

parsed AS (
    SELECT
        cb.*,

        NULLIF(
            COALESCE(
                substring(cb.all_utm_text_norm FROM 'cid:([0-9]+)'),
                substring(cb.all_utm_text_norm FROM 'campaign_id:([0-9]+)'),
                NULLIF(cb.utm_campaign_id, '')
            ),
            ''
        )::bigint AS parsed_campaign_id,

        NULLIF(
            substring(cb.all_utm_text_norm FROM 'gid:([0-9]+)'),
            ''
        )::bigint AS parsed_ad_group_id,

        NULLIF(
            COALESCE(
                substring(cb.all_utm_text_norm FROM 'phid:([0-9]+)'),
                NULLIF(cb.utm_phrase_id, '')
            ),
            ''
        )::bigint AS parsed_phrase_id,

        CASE
            WHEN cb.utm_source_norm IN (
                'yandex',
                'ya.direct',
                'yandex_context',
                'yandex_apartments',
                'ya_apartments',
                'realtyyandex'
            )
            AND cb.utm_medium_norm IN (
                'cpc',
                'cpc_search',
                'cpc_mk',
                'context',
                'контекст'
            )
            THEN 1

            WHEN cb.all_utm_text_norm LIKE '%cid:%'
              OR cb.all_utm_text_norm LIKE '%gid:%'
              OR cb.all_utm_text_norm LIKE '%phid:%'
              OR cb.all_utm_text_norm LIKE '%aid:%'
            THEN 1

            ELSE 0
        END AS is_yandex_candidate

    FROM crm_base cb
),

matched_campaign AS (
    SELECT
        p.*,

        yc.campaign_id AS matched_campaign_id,
        yc.campaign_name AS matched_campaign_name,
        yc.cabinet_id::text AS matched_cabinet_id,

        cn.cabinet_name AS matched_cabinet_name

    FROM parsed p

    LEFT JOIN yandex_campaigns yc
        ON yc.campaign_id = p.parsed_campaign_id

    LEFT JOIN cabinet_names cn
        ON cn.cabinet_id = yc.cabinet_id::text

    WHERE p.is_yandex_candidate = 1
),

matched_with_project AS (
    SELECT
        mc.*,

        pcm.project_key,
        pcm.project_name,
        pcm.cabinet_id::text AS project_cabinet_id,
        pcm.cabinet_name::text AS project_cabinet_name

    FROM matched_campaign mc

    LEFT JOIN LATERAL (
        SELECT
            pmap.project_key,
            pmap.project_name,
            pmap.cabinet_id,
            pmap.cabinet_name
        FROM project_cabinet_map pmap
        WHERE
            (
                mc.utm_term_norm = pmap.project_key
                OR mc.utm_term_norm LIKE '%' || pmap.project_key || '%'
                OR mc.utm_campaign_norm LIKE '%' || pmap.project_key || '%'
                OR mc.utm_content_norm LIKE '%' || pmap.project_key || '%'
                OR mc.all_utm_text_norm LIKE '%' || pmap.project_key || '%'
                OR LOWER(TRIM(COALESCE(mc.channel_name, ''))) LIKE '%' || pmap.project_key || '%'
            )
        ORDER BY
            CASE
                WHEN mc.utm_term_norm = pmap.project_key THEN 1
                WHEN mc.utm_term_norm LIKE '%' || pmap.project_key || '%' THEN 2
                WHEN mc.utm_campaign_norm LIKE '%' || pmap.project_key || '%' THEN 3
                WHEN mc.utm_content_norm LIKE '%' || pmap.project_key || '%' THEN 4
                WHEN mc.all_utm_text_norm LIKE '%' || pmap.project_key || '%' THEN 5
                ELSE 9
            END,
            LENGTH(pmap.project_key) DESC
        LIMIT 1
    ) pcm ON TRUE
)

SELECT
    buy_id,
    lead_date,
    lead_created_at,
    date_modified,
    buy_updated_at,

    COALESCE(
        matched_campaign_id,
        NULL
    )::bigint AS campaign_id,

    CASE
        WHEN matched_campaign_id IS NOT NULL
            THEN matched_campaign_name
        ELSE 'Yandex / без распознанной кампании'
    END::text AS campaign_name,

    COALESCE(
        matched_cabinet_id,
        project_cabinet_id
    )::text AS cabinet_id,

    COALESCE(
        matched_cabinet_name,
        project_cabinet_name
    )::text AS cabinet_name,

    COALESCE(
        project_name,
        matched_cabinet_name,
        project_cabinet_name
    )::text AS project_name,

    status_human,
    status_norm,
    is_target,

    deal_id,
    deal_sum,

    channel_type,
    channel_name,
    channel_medium,

    utm_source,
    utm_medium,
    utm_campaign,
    utm_content,
    utm_term,
    utm_keyword,
    utm_campaign_id,
    utm_ad_id,
    utm_phrase_id,

    parsed_campaign_id,
    parsed_ad_group_id,
    parsed_phrase_id,

    all_utm_text_norm AS all_utm_text,

    CASE
        WHEN matched_campaign_id IS NOT NULL
            THEN 'matched_campaign'

        WHEN matched_campaign_id IS NULL
             AND project_cabinet_id IS NOT NULL
            THEN 'unresolved_campaign_cabinet_matched'

        ELSE 'unresolved_campaign'
    END::text AS match_status,

    CASE
        WHEN matched_campaign_id IS NOT NULL
            THEN 'campaign_id_from_utm_tokens'

        WHEN matched_campaign_id IS NULL
             AND project_cabinet_id IS NOT NULL
            THEN 'campaign_not_resolved / cabinet_by_project_key'

        ELSE 'campaign_not_resolved / cabinet_not_resolved'
    END::text AS match_method,

    'macro_estate_buys_utm'::text AS utm_source_table,

    NOW() AS loaded_at

FROM matched_with_project;
"""


CREATE_INDEXES_SQL = """
CREATE INDEX IF NOT EXISTS idx_crm_yandex_leads_matched_lead_date
    ON crm_yandex_leads_matched (lead_date);

CREATE INDEX IF NOT EXISTS idx_crm_yandex_leads_matched_buy_id
    ON crm_yandex_leads_matched (buy_id);

CREATE INDEX IF NOT EXISTS idx_crm_yandex_leads_matched_campaign_id
    ON crm_yandex_leads_matched (campaign_id);

CREATE INDEX IF NOT EXISTS idx_crm_yandex_leads_matched_cabinet_id
    ON crm_yandex_leads_matched (cabinet_id);

CREATE INDEX IF NOT EXISTS idx_crm_yandex_leads_matched_cabinet_name
    ON crm_yandex_leads_matched (cabinet_name);

CREATE INDEX IF NOT EXISTS idx_crm_yandex_leads_matched_match_status
    ON crm_yandex_leads_matched (match_status);

CREATE INDEX IF NOT EXISTS idx_crm_yandex_leads_matched_status_norm
    ON crm_yandex_leads_matched (status_norm);

CREATE INDEX IF NOT EXISTS idx_crm_yandex_leads_matched_parsed_campaign
    ON crm_yandex_leads_matched (parsed_campaign_id);
"""


CHECK_SQL = """
SELECT
    MIN(lead_date) AS min_date,
    MAX(lead_date) AS max_date,
    COUNT(*) AS rows_count,
    COUNT(*) FILTER (
        WHERE lead_date BETWEEN CURRENT_DATE - INTERVAL '14 days' AND CURRENT_DATE
    ) AS last_14_days_rows
FROM crm_yandex_leads_matched;
"""


CHECK_JULY_SQL = """
SELECT
    lead_date,
    cabinet_name,
    COUNT(*) AS leads,
    SUM(is_target) AS target_leads
FROM crm_yandex_leads_matched
WHERE lead_date >= DATE '2026-07-01'
GROUP BY
    lead_date,
    cabinet_name
ORDER BY
    lead_date,
    cabinet_name;
"""


def rebuild_crm_yandex_leads_matched() -> None:
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    logging.info("Пересоздаем crm_yandex_leads_matched")
    pg_hook.run(CREATE_TABLE_SQL)

    logging.info("Создаем индексы crm_yandex_leads_matched")
    pg_hook.run(CREATE_INDEXES_SQL)

    logging.info("Проверяем crm_yandex_leads_matched")
    rows = pg_hook.get_records(CHECK_SQL)
    for row in rows:
        logging.info("crm_yandex_leads_matched summary: %s", row)

    july_rows = pg_hook.get_records(CHECK_JULY_SQL)
    if july_rows:
        logging.info("Июльские Яндекс-лиды:")
        for row in july_rows:
            logging.info(row)
    else:
        logging.warning("Июльских строк в crm_yandex_leads_matched нет. Проверь UTM/дату/фильтр кандидатов.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
}


with DAG(
    dag_id="crm_yandex_leads_matched_loader",
    default_args=default_args,
    schedule_interval="15 1 * * *",
    catchup=False,
    tags=["crm", "yandex", "matched"],
    description="Аудитная таблица CRM-лидов Яндекса: crm_yandex_leads_matched",
) as dag:

    rebuild_task = PythonOperator(
        task_id="rebuild_crm_yandex_leads_matched",
        python_callable=rebuild_crm_yandex_leads_matched,
    )

    rebuild_task