#!/usr/bin/env python3
"""
DAG для ежедневной загрузки данных из Яндекс.Директа в PostgreSQL.

ИТОГОВАЯ ЛОГИКА:

1. yandex_campaign_costs_daily
   Главная таблица для Superset / сводной аналитики.
   Зерно: date + cabinet_id + campaign_id.
   Источник: CAMPAIGN_PERFORMANCE_REPORT БЕЗ Placement.
   Использовать для:
   - показы
   - клики
   - расход
   - CTR
   - CPC
   - CPL после соединения с CRM

2. yandex_placement_costs_daily
   Отдельная таблица для чистки площадок РСЯ.
   Зерно: date + cabinet_id + campaign_id + placement.
   Источник: CAMPAIGN_PERFORMANCE_REPORT С Placement.
   Использовать только для анализа площадок.

3. yandex_adgroup_criteria_costs_daily
   Детализация по группам, условиям показа и устройствам.
   Зерно: date + cabinet_id + campaign_id + ad_group_id + criterion_id + device.
   Источник: CUSTOM_REPORT.
   Использовать для анализа:
   - кампания → группа → criterion → device
   - показы
   - клики
   - расход

4. Режимы загрузки:
   macro_v2_full_load = true:
       грузим историю с START_DATE_HISTORICAL до target_date.

   macro_v2_full_load = false:
       грузим текущий месяц с 1-го числа месяца target_date до target_date.

5. yandex_costs больше НЕ используем как источник основной сводной.
"""

import logging
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert


POSTGRES_CONN_ID = "postgres_default"
START_DATE_HISTORICAL = "2026-01-01"


class YandexDirectReporter:
    API_URL = "https://api.direct.yandex.com/json/v5/reports"

    def __init__(self, client_login: str, token: str):
        self.client_login = client_login
        self.token = token
        self.headers = {
            "Client-Login": client_login,
            "Authorization": f"Bearer {token}",
            "Accept-Language": "ru",
            "Content-Type": "application/json; charset=utf-8",
        }

    def _make_request(self, body: dict, max_retries: int = 30, delay: int = 10) -> str | None:
        for attempt in range(max_retries):
            try:
                resp = requests.post(
                    self.API_URL,
                    headers=self.headers,
                    json=body,
                    timeout=180,
                )

                if resp.status_code == 200:
                    return resp.text

                if resp.status_code in (201, 202):
                    logging.info(
                        "Отчет формируется: попытка %s/%s",
                        attempt + 1,
                        max_retries,
                    )
                    time.sleep(delay)
                    continue

                logging.error(
                    "Ошибка API Яндекс.Директа HTTP %s: %s",
                    resp.status_code,
                    resp.text,
                )
                return None

            except Exception as e:
                logging.exception("Ошибка соединения с API Яндекс.Директа: %s", e)
                return None

        logging.error("Превышено количество попыток получения отчета")
        return None

    def get_report(
        self,
        fields: list[str],
        date_from: str,
        date_to: str,
        report_type: str = "CAMPAIGN_PERFORMANCE_REPORT",
    ) -> pd.DataFrame | None:
        body = {
            "params": {
                "SelectionCriteria": {
                    "DateFrom": date_from,
                    "DateTo": date_to,
                },
                "FieldNames": fields,
                "ReportName": f"{self.client_login}_{report_type}_{date_from}_{int(time.time())}",
                "ReportType": report_type,
                "DateRangeType": "CUSTOM_DATE",
                "Format": "TSV",
                "IncludeVAT": "NO",
                "IncludeDiscount": "NO",
            }
        }

        tsv_data = self._make_request(body)
        if not tsv_data:
            return None

        lines = [
            line.rstrip("\r")
            for line in tsv_data.strip().split("\n")
            if line.strip()
            and not line.startswith("Total rows")
            and not line.startswith("Report name")
            and not line.startswith("Report type")
            and not line.startswith("Date range")
        ]

        if not lines:
            return pd.DataFrame()

        header_idx = next((i for i, line in enumerate(lines) if "\t" in line), None)
        if header_idx is None:
            logging.error("Не найден заголовок TSV-отчета")
            return None

        header = lines[header_idx].split("\t")
        data_lines = lines[header_idx + 1:]

        valid_rows = []
        for line in data_lines:
            parts = line.split("\t")
            if len(parts) == len(header):
                valid_rows.append(parts)
            else:
                logging.warning("Пропущена строка с неверным числом колонок: %s", line)

        if not valid_rows:
            return pd.DataFrame()

        df = pd.DataFrame(valid_rows, columns=header)

        for col in ["Impressions", "Clicks", "Cost", "Bounces"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        # Яндекс.Директ отдает Cost в микроденежных единицах.
        if "Cost" in df.columns:
            df["Cost"] = df["Cost"] / 1_000_000.0

        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

        return df


def ensure_tables(pg_hook: PostgresHook) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS yandex_campaign_costs_daily (
        date DATE NOT NULL,
        cabinet_id VARCHAR(50) NOT NULL,
        campaign_id BIGINT NOT NULL,
        campaign_name TEXT,
        campaign_type VARCHAR(100),
        impressions BIGINT DEFAULT 0,
        clicks BIGINT DEFAULT 0,
        cost NUMERIC DEFAULT 0,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (date, cabinet_id, campaign_id)
    );

    CREATE INDEX IF NOT EXISTS idx_yandex_campaign_costs_daily_date
        ON yandex_campaign_costs_daily (date);

    CREATE INDEX IF NOT EXISTS idx_yandex_campaign_costs_daily_campaign
        ON yandex_campaign_costs_daily (campaign_id);

    CREATE INDEX IF NOT EXISTS idx_yandex_campaign_costs_daily_cabinet
        ON yandex_campaign_costs_daily (cabinet_id);

    CREATE TABLE IF NOT EXISTS yandex_placement_costs_daily (
        date DATE NOT NULL,
        cabinet_id VARCHAR(50) NOT NULL,
        campaign_id BIGINT NOT NULL,
        campaign_name TEXT,
        campaign_type VARCHAR(100),
        placement TEXT NOT NULL DEFAULT '',
        impressions BIGINT DEFAULT 0,
        clicks BIGINT DEFAULT 0,
        cost NUMERIC DEFAULT 0,
        bounces BIGINT DEFAULT 0,
        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (date, cabinet_id, campaign_id, placement)
    );

    CREATE INDEX IF NOT EXISTS idx_yandex_placement_costs_daily_date
        ON yandex_placement_costs_daily (date);

    CREATE INDEX IF NOT EXISTS idx_yandex_placement_costs_daily_campaign
        ON yandex_placement_costs_daily (campaign_id);

    CREATE INDEX IF NOT EXISTS idx_yandex_placement_costs_daily_cabinet
        ON yandex_placement_costs_daily (cabinet_id);

    CREATE INDEX IF NOT EXISTS idx_yandex_placement_costs_daily_placement
        ON yandex_placement_costs_daily (placement);

    CREATE TABLE IF NOT EXISTS yandex_adgroup_criteria_costs_daily (
        date DATE NOT NULL,
        cabinet_id VARCHAR(50) NOT NULL,

        campaign_id BIGINT NOT NULL,
        campaign_name TEXT,
        campaign_type VARCHAR(100),

        ad_group_id BIGINT DEFAULT 0,
        ad_group_name TEXT,

        criterion_id BIGINT DEFAULT 0,
        criterion TEXT,

        device VARCHAR(100) DEFAULT '',

        impressions BIGINT DEFAULT 0,
        clicks BIGINT DEFAULT 0,
        cost NUMERIC DEFAULT 0,

        created_at TIMESTAMP DEFAULT NOW(),
        updated_at TIMESTAMP DEFAULT NOW(),

        PRIMARY KEY (
            date,
            cabinet_id,
            campaign_id,
            ad_group_id,
            criterion_id,
            device
        )
    );

    CREATE INDEX IF NOT EXISTS idx_yandex_adgroup_criteria_date
        ON yandex_adgroup_criteria_costs_daily (date);

    CREATE INDEX IF NOT EXISTS idx_yandex_adgroup_criteria_campaign
        ON yandex_adgroup_criteria_costs_daily (campaign_id);

    CREATE INDEX IF NOT EXISTS idx_yandex_adgroup_criteria_cabinet
        ON yandex_adgroup_criteria_costs_daily (cabinet_id);

    CREATE INDEX IF NOT EXISTS idx_yandex_adgroup_criteria_group
        ON yandex_adgroup_criteria_costs_daily (ad_group_id);

    CREATE INDEX IF NOT EXISTS idx_yandex_adgroup_criteria_criterion
        ON yandex_adgroup_criteria_costs_daily (criterion_id);
    """
    pg_hook.run(ddl)


def clear_day_for_account(pg_hook: PostgresHook, cabinet_id: str, day_date: str) -> None:
    """
    Очищаем данные за конкретный день и кабинет перед свежей вставкой.
    Важно: чистим только после успешного получения отчетов.
    """
    sql = """
    DELETE FROM yandex_campaign_costs_daily
    WHERE cabinet_id = %(cabinet_id)s
      AND date = %(day_date)s;

    DELETE FROM yandex_placement_costs_daily
    WHERE cabinet_id = %(cabinet_id)s
      AND date = %(day_date)s;

    DELETE FROM yandex_adgroup_criteria_costs_daily
    WHERE cabinet_id = %(cabinet_id)s
      AND date = %(day_date)s;
    """
    pg_hook.run(sql, parameters={"cabinet_id": cabinet_id, "day_date": day_date})


def upsert_dataframe_to_postgres(
    pg_hook: PostgresHook,
    table_name: str,
    df: pd.DataFrame,
    index_elements: list[str],
    conflict_update: list[str],
) -> None:
    if df is None or df.empty:
        logging.info("Нет данных для вставки в %s", table_name)
        return

    engine = pg_hook.get_sqlalchemy_engine()
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    with engine.begin() as conn:
        for _, row in df.iterrows():
            values = {k: v for k, v in row.to_dict().items() if pd.notna(v)}
            values["updated_at"] = datetime.now()

            stmt = insert(table).values(**values)

            update_dict = {
                col: stmt.excluded[col]
                for col in conflict_update
                if col in values
            }
            update_dict["updated_at"] = datetime.now()

            stmt = stmt.on_conflict_do_update(
                index_elements=index_elements,
                set_=update_dict,
            )

            conn.execute(stmt)


def normalize_campaign_daily_df(df: pd.DataFrame, cabinet_id: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    required_columns = [
        "Date",
        "CampaignId",
        "CampaignName",
        "CampaignType",
        "Impressions",
        "Clicks",
        "Cost",
    ]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"В кампанийном отчете не хватает колонок: {missing}")

    result = df[required_columns].copy()

    result = result.groupby(
        ["Date", "CampaignId", "CampaignName", "CampaignType"],
        as_index=False,
    ).agg(
        {
            "Impressions": "sum",
            "Clicks": "sum",
            "Cost": "sum",
        }
    )

    result["cabinet_id"] = cabinet_id

    result = result.rename(
        columns={
            "Date": "date",
            "CampaignId": "campaign_id",
            "CampaignName": "campaign_name",
            "CampaignType": "campaign_type",
            "Impressions": "impressions",
            "Clicks": "clicks",
            "Cost": "cost",
        }
    )

    result["date"] = pd.to_datetime(result["date"], errors="coerce").dt.date
    result["campaign_id"] = pd.to_numeric(result["campaign_id"], errors="coerce").fillna(0).astype("int64")
    result["campaign_name"] = result["campaign_name"].fillna("").astype(str)
    result["campaign_type"] = result["campaign_type"].fillna("").astype(str)
    result["cabinet_id"] = result["cabinet_id"].fillna("").astype(str)
    result["impressions"] = pd.to_numeric(result["impressions"], errors="coerce").fillna(0).astype("int64")
    result["clicks"] = pd.to_numeric(result["clicks"], errors="coerce").fillna(0).astype("int64")
    result["cost"] = pd.to_numeric(result["cost"], errors="coerce").fillna(0.0)

    return result[
        [
            "date",
            "cabinet_id",
            "campaign_id",
            "campaign_name",
            "campaign_type",
            "impressions",
            "clicks",
            "cost",
        ]
    ]


def normalize_placement_daily_df(df: pd.DataFrame, cabinet_id: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    required_columns = [
        "Date",
        "CampaignId",
        "CampaignName",
        "CampaignType",
        "Placement",
        "Impressions",
        "Clicks",
        "Cost",
    ]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"В отчете по площадкам не хватает колонок: {missing}")

    if "Bounces" not in df.columns:
        df["Bounces"] = 0

    result = df[
        [
            "Date",
            "CampaignId",
            "CampaignName",
            "CampaignType",
            "Placement",
            "Impressions",
            "Clicks",
            "Cost",
            "Bounces",
        ]
    ].copy()

    result["Placement"] = result["Placement"].fillna("").astype(str).str.strip()

    result = result.groupby(
        ["Date", "CampaignId", "CampaignName", "CampaignType", "Placement"],
        as_index=False,
    ).agg(
        {
            "Impressions": "sum",
            "Clicks": "sum",
            "Cost": "sum",
            "Bounces": "sum",
        }
    )

    result["cabinet_id"] = cabinet_id

    result = result.rename(
        columns={
            "Date": "date",
            "CampaignId": "campaign_id",
            "CampaignName": "campaign_name",
            "CampaignType": "campaign_type",
            "Placement": "placement",
            "Impressions": "impressions",
            "Clicks": "clicks",
            "Cost": "cost",
            "Bounces": "bounces",
        }
    )

    result["date"] = pd.to_datetime(result["date"], errors="coerce").dt.date
    result["campaign_id"] = pd.to_numeric(result["campaign_id"], errors="coerce").fillna(0).astype("int64")
    result["campaign_name"] = result["campaign_name"].fillna("").astype(str)
    result["campaign_type"] = result["campaign_type"].fillna("").astype(str)
    result["cabinet_id"] = result["cabinet_id"].fillna("").astype(str)
    result["placement"] = result["placement"].fillna("").astype(str)
    result["impressions"] = pd.to_numeric(result["impressions"], errors="coerce").fillna(0).astype("int64")
    result["clicks"] = pd.to_numeric(result["clicks"], errors="coerce").fillna(0).astype("int64")
    result["cost"] = pd.to_numeric(result["cost"], errors="coerce").fillna(0.0)
    result["bounces"] = pd.to_numeric(result["bounces"], errors="coerce").fillna(0).astype("int64")

    return result[
        [
            "date",
            "cabinet_id",
            "campaign_id",
            "campaign_name",
            "campaign_type",
            "placement",
            "impressions",
            "clicks",
            "cost",
            "bounces",
        ]
    ]


def normalize_adgroup_criteria_daily_df(df: pd.DataFrame, cabinet_id: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    required_columns = [
        "Date",
        "CampaignId",
        "CampaignName",
        "CampaignType",
        "AdGroupId",
        "AdGroupName",
        "CriterionId",
        "Criterion",
        "Device",
        "Impressions",
        "Clicks",
        "Cost",
    ]

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"В отчете по группам/условиям не хватает колонок: {missing}")

    result = df[required_columns].copy()

    result = result.groupby(
        [
            "Date",
            "CampaignId",
            "CampaignName",
            "CampaignType",
            "AdGroupId",
            "AdGroupName",
            "CriterionId",
            "Criterion",
            "Device",
        ],
        as_index=False,
    ).agg(
        {
            "Impressions": "sum",
            "Clicks": "sum",
            "Cost": "sum",
        }
    )

    result["cabinet_id"] = cabinet_id

    result = result.rename(
        columns={
            "Date": "date",
            "CampaignId": "campaign_id",
            "CampaignName": "campaign_name",
            "CampaignType": "campaign_type",
            "AdGroupId": "ad_group_id",
            "AdGroupName": "ad_group_name",
            "CriterionId": "criterion_id",
            "Criterion": "criterion",
            "Device": "device",
            "Impressions": "impressions",
            "Clicks": "clicks",
            "Cost": "cost",
        }
    )

    result["date"] = pd.to_datetime(result["date"], errors="coerce").dt.date
    result["cabinet_id"] = result["cabinet_id"].fillna("").astype(str)

    result["campaign_id"] = pd.to_numeric(result["campaign_id"], errors="coerce").fillna(0).astype("int64")
    result["campaign_name"] = result["campaign_name"].fillna("").astype(str)
    result["campaign_type"] = result["campaign_type"].fillna("").astype(str)

    result["ad_group_id"] = pd.to_numeric(result["ad_group_id"], errors="coerce").fillna(0).astype("int64")
    result["ad_group_name"] = result["ad_group_name"].fillna("").astype(str)

    result["criterion_id"] = pd.to_numeric(result["criterion_id"], errors="coerce").fillna(0).astype("int64")
    result["criterion"] = result["criterion"].fillna("").astype(str)

    result["device"] = result["device"].fillna("").astype(str)

    result["impressions"] = pd.to_numeric(result["impressions"], errors="coerce").fillna(0).astype("int64")
    result["clicks"] = pd.to_numeric(result["clicks"], errors="coerce").fillna(0).astype("int64")
    result["cost"] = pd.to_numeric(result["cost"], errors="coerce").fillna(0.0)

    return result[
        [
            "date",
            "cabinet_id",
            "campaign_id",
            "campaign_name",
            "campaign_type",
            "ad_group_id",
            "ad_group_name",
            "criterion_id",
            "criterion",
            "device",
            "impressions",
            "clicks",
            "cost",
        ]
    ]


def upsert_campaigns_dictionary(pg_hook: PostgresHook, campaign_df: pd.DataFrame, cabinet_id: str) -> None:
    """
    Обновляем справочник yandex_campaigns, если он используется в других DAG.
    Основной источник для сводной теперь yandex_campaign_costs_daily.
    """
    if campaign_df is None or campaign_df.empty:
        return

    if not {"CampaignId", "CampaignName", "CampaignType"}.issubset(campaign_df.columns):
        return

    campaigns = campaign_df[
        ["CampaignId", "CampaignName", "CampaignType"]
    ].drop_duplicates(subset=["CampaignId"]).copy()

    campaigns["cabinet_id"] = cabinet_id
    campaigns = campaigns.rename(
        columns={
            "CampaignId": "campaign_id",
            "CampaignName": "campaign_name",
            "CampaignType": "ad_network_type",
        }
    )

    campaigns["campaign_id"] = pd.to_numeric(campaigns["campaign_id"], errors="coerce").fillna(0).astype("int64")
    campaigns["campaign_name"] = campaigns["campaign_name"].fillna("").astype(str)
    campaigns["ad_network_type"] = campaigns["ad_network_type"].fillna("").astype(str)
    campaigns["cabinet_id"] = campaigns["cabinet_id"].fillna("").astype(str)

    upsert_dataframe_to_postgres(
        pg_hook=pg_hook,
        table_name="yandex_campaigns",
        df=campaigns,
        index_elements=["campaign_id"],
        conflict_update=["campaign_name", "ad_network_type", "cabinet_id"],
    )


def load_one_day(account: dict, day_date: str) -> None:
    login = account["login"]
    token = account["token"]
    cabinet_id = account.get("cabinet_id", login)

    logging.info(
        "Загрузка Яндекс.Директа: login=%s, cabinet_id=%s, date=%s",
        login,
        cabinet_id,
        day_date,
    )

    reporter = YandexDirectReporter(login, token)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    ensure_tables(pg_hook)

    campaign_fields = [
        "Date",
        "CampaignName",
        "CampaignId",
        "CampaignType",
        "Clicks",
        "Impressions",
        "Cost",
    ]

    campaign_report = reporter.get_report(
        fields=campaign_fields,
        date_from=day_date,
        date_to=day_date,
        report_type="CAMPAIGN_PERFORMANCE_REPORT",
    )

    if campaign_report is None:
        raise RuntimeError(
            f"Не удалось получить кампанийный отчет за {day_date}, cabinet_id={cabinet_id}"
        )

    placement_fields = [
        "Date",
        "CampaignName",
        "CampaignId",
        "CampaignType",
        "Placement",
        "Clicks",
        "Impressions",
        "Cost",
        "Bounces",
    ]

    placement_report = reporter.get_report(
        fields=placement_fields,
        date_from=day_date,
        date_to=day_date,
        report_type="CAMPAIGN_PERFORMANCE_REPORT",
    )

    if placement_report is None:
        raise RuntimeError(
            f"Не удалось получить отчет по площадкам за {day_date}, cabinet_id={cabinet_id}"
        )

    adgroup_criteria_fields = [
        "Date",
        "CampaignName",
        "CampaignId",
        "CampaignType",
        "AdGroupId",
        "AdGroupName",
        "CriterionId",
        "Criterion",
        "Device",
        "Clicks",
        "Impressions",
        "Cost",
    ]

    adgroup_criteria_report = reporter.get_report(
        fields=adgroup_criteria_fields,
        date_from=day_date,
        date_to=day_date,
        report_type="CUSTOM_REPORT",
    )

    if adgroup_criteria_report is None:
        raise RuntimeError(
            f"Не удалось получить отчет по группам/условиям за {day_date}, cabinet_id={cabinet_id}"
        )

    campaign_daily = normalize_campaign_daily_df(campaign_report, cabinet_id)
    placement_daily = normalize_placement_daily_df(placement_report, cabinet_id)
    adgroup_criteria_daily = normalize_adgroup_criteria_daily_df(
        adgroup_criteria_report,
        cabinet_id,
    )

    # Чистим день только после успешного получения всех трех отчетов.
    clear_day_for_account(pg_hook, cabinet_id, day_date)

    if not campaign_daily.empty:
        upsert_dataframe_to_postgres(
            pg_hook=pg_hook,
            table_name="yandex_campaign_costs_daily",
            df=campaign_daily,
            index_elements=["date", "cabinet_id", "campaign_id"],
            conflict_update=[
                "campaign_name",
                "campaign_type",
                "impressions",
                "clicks",
                "cost",
            ],
        )

        upsert_campaigns_dictionary(pg_hook, campaign_report, cabinet_id)

    if not placement_daily.empty:
        upsert_dataframe_to_postgres(
            pg_hook=pg_hook,
            table_name="yandex_placement_costs_daily",
            df=placement_daily,
            index_elements=["date", "cabinet_id", "campaign_id", "placement"],
            conflict_update=[
                "campaign_name",
                "campaign_type",
                "impressions",
                "clicks",
                "cost",
                "bounces",
            ],
        )

    if not adgroup_criteria_daily.empty:
        upsert_dataframe_to_postgres(
            pg_hook=pg_hook,
            table_name="yandex_adgroup_criteria_costs_daily",
            df=adgroup_criteria_daily,
            index_elements=[
                "date",
                "cabinet_id",
                "campaign_id",
                "ad_group_id",
                "criterion_id",
                "device",
            ],
            conflict_update=[
                "campaign_name",
                "campaign_type",
                "ad_group_name",
                "criterion",
                "impressions",
                "clicks",
                "cost",
            ],
        )

    logging.info(
        "Загрузка завершена: login=%s, cabinet_id=%s, date=%s",
        login,
        cabinet_id,
        day_date,
    )


def get_month_start(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.replace(day=1).strftime("%Y-%m-%d")


def load_period(account: dict, date_from: str, date_to: str) -> None:
    start = datetime.strptime(date_from, "%Y-%m-%d")
    end = datetime.strptime(date_to, "%Y-%m-%d")

    current = start
    while current <= end:
        day_str = current.strftime("%Y-%m-%d")
        logging.info("Загрузка периода: %s", day_str)
        load_one_day(account, day_str)
        current += timedelta(days=1)


def load_yandex_direct(account: dict, target_date: str, full_load: bool = False) -> None:
    """
    full_load = True:
        грузим с START_DATE_HISTORICAL до target_date.

    full_load = False:
        грузим текущий месяц с 1-го числа месяца target_date до target_date.
    """
    if full_load:
        date_from = START_DATE_HISTORICAL
        date_to = target_date
        logging.info("Полная историческая загрузка: %s — %s", date_from, date_to)
    else:
        date_from = get_month_start(target_date)
        date_to = target_date
        logging.info("Загрузка текущего месяца: %s — %s", date_from, date_to)

    load_period(account, date_from, date_to)


def parse_bool(value) -> bool:
    if isinstance(value, bool):
        return value

    if value is None:
        return False

    return str(value).strip().lower() in {"1", "true", "yes", "y", "да"}


def get_target_date_from_context(context) -> str:
    """
    Можно вручную задать дату через Airflow Variable:
    yandex_direct_target_date = 2026-06-22

    Если переменная пустая, берем data_interval_end.
    """
    manual_target_date = Variable.get("yandex_direct_target_date", default_var="").strip()

    if manual_target_date:
        datetime.strptime(manual_target_date, "%Y-%m-%d")
        return manual_target_date

    return context["data_interval_end"].strftime("%Y-%m-%d")


def process_accounts(**context) -> None:
    accounts = Variable.get("yandex_direct_accounts", deserialize_json=True)

    full_load_raw = Variable.get("macro_v2_full_load", default_var=False)
    full_load = parse_bool(full_load_raw)

    target_date = get_target_date_from_context(context)

    logging.info(
        "Старт загрузки Яндекс.Директа. target_date=%s, full_load=%s",
        target_date,
        full_load,
    )

    for account in accounts:
        load_yandex_direct(
            account=account,
            target_date=target_date,
            full_load=full_load,
        )

    logging.info("Все аккаунты Яндекс.Директа загружены")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
}


with DAG(
    dag_id="yandex_direct_loader",
    default_args=default_args,
    schedule_interval="0 21 * * *",
    catchup=False,
    tags=["yandex", "direct"],
    description="Загрузка Яндекс.Директа: кампании, площадки, группы и условия",
) as dag:

    run_load = PythonOperator(
        task_id="load_all_accounts",
        python_callable=process_accounts,
        provide_context=True,
    )

    run_load