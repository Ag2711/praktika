#!/usr/bin/env python3
"""
DAG для загрузки данных заявок из MySQL macroData в PostgreSQL.

Читает:
- estate_buys + contacts
- estate_buys_utm

Пишет:
- macro_estate_buys
- macro_estate_buys_utm

Режимы:
1. macro_v2_full_load = true
   Полная загрузка с macro_mysql_full_reload_from, например с 2026-01-01, по target_date.

2. macro_v2_full_load = false
   Загрузка текущего месяца: с 1-го числа месяца target_date по target_date включительно.

Дата target_date определяется так:
1. dag_run.conf["target_date"], если передали при ручном запуске
2. Airflow Variable macro_v2_target_date, если задана
3. Сегодняшняя дата по Asia/Yekaterinburg
"""

import logging
import math
from datetime import datetime, timedelta, date
from decimal import Decimal

import numpy as np
import pandas as pd
import pendulum

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from sqlalchemy import Table, MetaData, BigInteger
from sqlalchemy.dialects.postgresql import insert


MYSQL_CONN_ID = "mysql_macrodata"
POSTGRES_CONN_ID = "postgres_default"

MACRO_BUYS_TABLE = "macro_estate_buys"
MACRO_UTM_TABLE = "macro_estate_buys_utm"

BATCH_SIZE = 500
DEFAULT_FULL_RELOAD_FROM = "2026-01-01 00:00:00"
LOCAL_TZ = "Asia/Yekaterinburg"


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2025, 1, 1),
}


def get_bool_variable(name: str, default: bool = False) -> bool:
    value = Variable.get(name, default_var=str(default).lower())
    return str(value).strip().lower() in ("true", "1", "yes", "y", "да")


def get_target_date_from_context(context) -> str:
    """
    Определяет target_date.

    Приоритет:
    1. dag_run.conf["target_date"]
    2. Airflow Variable macro_v2_target_date
    3. Сегодняшняя дата по Екатеринбургу

    Важно:
    data_interval_end не используем как основной источник,
    потому что Airflow может отдавать конец предыдущего закрытого интервала.
    """

    dag_run = context.get("dag_run")

    if dag_run and dag_run.conf and dag_run.conf.get("target_date"):
        target_date = dag_run.conf["target_date"]
        datetime.strptime(target_date, "%Y-%m-%d")
        return target_date

    manual_target_date = Variable.get("macro_v2_target_date", default_var="").strip()

    if manual_target_date:
        datetime.strptime(manual_target_date, "%Y-%m-%d")
        return manual_target_date

    return pendulum.now(LOCAL_TZ).strftime("%Y-%m-%d")


def get_month_start(date_str: str) -> str:
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return dt.replace(day=1).strftime("%Y-%m-%d")


def get_load_period(**context) -> tuple[str, str]:
    """
    Возвращает период загрузки:
    - date_from: включительно, формат YYYY-MM-DD 00:00:00
    - date_to_exclusive: НЕ включительно, следующий день после target_date
    """

    full_reload = get_bool_variable("macro_v2_full_load", default=False)
    target_date = get_target_date_from_context(context)

    target_dt = datetime.strptime(target_date, "%Y-%m-%d")
    date_to_exclusive = (target_dt + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

    if full_reload:
        date_from = Variable.get(
            "macro_mysql_full_reload_from",
            default_var=DEFAULT_FULL_RELOAD_FROM,
        )
        logging.info(
            "Macro CRM: FULL_RELOAD=true. Загружаем период %s — %s не включительно",
            date_from,
            date_to_exclusive,
        )
        return date_from, date_to_exclusive

    month_start = get_month_start(target_date)
    date_from = f"{month_start} 00:00:00"

    logging.info(
        "Macro CRM: FULL_RELOAD=false. Загружаем текущий месяц %s — %s не включительно",
        date_from,
        date_to_exclusive,
    )

    return date_from, date_to_exclusive


def fetch_estate_buys(
    mysql_hook: MySqlHook,
    date_from: str,
    date_to_exclusive: str,
) -> pd.DataFrame:
    """
    Читает заявки из MySQL, созданные или измененные в периоде:
    [date_from, date_to_exclusive)
    """

    sql = """
        SELECT
            eb.estate_buy_id,
            eb.created_at,
            FROM_UNIXTIME(eb.date_modified) AS date_modified,
            eb.status_name,
            eb.custom_status_name,
            eb.contacts_id,
            c.contacts_buy_phones,
            c.contacts_buy_emails,
            eb.contacts_buy_dob,
            eb.contacts_buy_marital_status,
            eb.contacts_buy_geo_country_name,
            eb.contacts_buy_geo_city_name,
            eb.contacts_buy_geo_region_name,
            eb.manager_id,
            eb.call_center_manager_id,
            eb.agent_name,
            eb.agency_name,
            eb.category,
            eb.deal_id,
            eb.deal_price,
            eb.deal_sum,
            eb.deal_date,
            eb.deal_program_name,
            eb.estate_sell_id,
            eb.channel_type,
            eb.channel_name,
            eb.channel_medium,
            eb.utm_source,
            eb.utm_medium,
            eb.utm_campaign,
            eb.utm_content
        FROM estate_buys eb
        LEFT JOIN contacts c
            ON c.contacts_id = eb.contacts_id
        WHERE
            (
                FROM_UNIXTIME(eb.date_modified) >= %s
                AND FROM_UNIXTIME(eb.date_modified) < %s
            )
            OR
            (
                eb.created_at >= %s
                AND eb.created_at < %s
            )
    """

    logging.info(
        "Загружаем заявки из MySQL за период %s — %s не включительно",
        date_from,
        date_to_exclusive,
    )

    df = mysql_hook.get_pandas_df(
        sql,
        parameters=(
            date_from,
            date_to_exclusive,
            date_from,
            date_to_exclusive,
        ),
    )

    logging.info("Получено заявок из MySQL: %s", len(df))

    return df


def fetch_estate_buys_utm(
    mysql_hook: MySqlHook,
    date_from: str,
    date_to_exclusive: str,
) -> pd.DataFrame:
    """
    Читает UTM-данные из MySQL за период:
    [date_from, date_to_exclusive)
    """

    sql = """
        SELECT
            id,
            estate_buy_id,
            date_added,
            channel_type,
            channel_name,
            channel_medium,
            utm_source,
            utm_medium,
            utm_campaign,
            utm_content,
            utm_term,
            utm_keyword,
            utm_block,
            utm_position_type,
            utm_position,
            utm_campaign_id,
            utm_ad_id,
            utm_phrase_id,
            yandex_cid,
            google_cid,
            roistat_cid,
            calltracking_vendor_name,
            calltracking_vendor_id,
            jivosite_cid,
            carrotquest_cid,
            facebook_id
        FROM estate_buys_utm
        WHERE
            (
                date_added >= %s
                AND date_added < %s
            )
            OR
            (
                updated_at >= %s
                AND updated_at < %s
            )
    """

    logging.info(
        "Загружаем UTM из MySQL за период %s — %s не включительно",
        date_from,
        date_to_exclusive,
    )

    df = mysql_hook.get_pandas_df(
        sql,
        parameters=(
            date_from,
            date_to_exclusive,
            date_from,
            date_to_exclusive,
        ),
    )

    logging.info("Получено UTM-записей из MySQL: %s", len(df))

    return df


def clean_value_for_pg(value):
    """
    Готовит одно значение к загрузке в PostgreSQL.

    Преобразует:
    - pandas NaN / NaT / pd.NA -> None
    - float nan -> None
    - numpy-типы -> обычные Python-типы
    """

    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(value, float):
        if math.isnan(value):
            return None
        return value

    if isinstance(value, np.floating):
        if np.isnan(value):
            return None
        return float(value)

    if isinstance(value, np.integer):
        return int(value)

    if isinstance(value, np.bool_):
        return bool(value)

    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.to_pydatetime()

    if isinstance(value, (datetime, date, Decimal, int, str, bool)):
        return value

    return value


def dataframe_to_pg_records(df: pd.DataFrame) -> list[dict]:
    raw_records = df.to_dict(orient="records")

    clean_records = []

    for row in raw_records:
        clean_row = {
            key: clean_value_for_pg(value)
            for key, value in row.items()
        }
        clean_records.append(clean_row)

    return clean_records


def get_pg_table(pg_hook: PostgresHook, table_name: str):
    engine = pg_hook.get_sqlalchemy_engine()
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    return engine, table


def align_dataframe_to_pg_table(df: pd.DataFrame, table: Table) -> pd.DataFrame:
    pg_columns = [column.name for column in table.columns]
    df_columns = list(df.columns)

    extra_columns = [
        column for column in df_columns
        if column not in pg_columns
    ]

    missing_columns = [
        column for column in pg_columns
        if column not in df_columns
    ]

    if extra_columns:
        logging.warning(
            "Колонки есть в DataFrame, но отсутствуют в PostgreSQL и будут отброшены: %s",
            extra_columns,
        )

    if missing_columns:
        logging.info(
            "Колонки PostgreSQL, которых нет в DataFrame: %s",
            missing_columns,
        )

    result_columns = [
        column for column in df_columns
        if column in pg_columns
    ]

    return df[result_columns]


def log_nulls(df: pd.DataFrame, table_name: str):
    for column_name in df.columns:
        null_count = df[column_name].isna().sum()

        if null_count > 0:
            logging.info(
                "%s.%s: пустых значений = %s",
                table_name,
                column_name,
                null_count,
            )


def log_bigint_problems(df: pd.DataFrame, table: Table, table_name: str):
    bigint_min = -9223372036854775808
    bigint_max = 9223372036854775807

    bigint_columns = [
        column.name
        for column in table.columns
        if isinstance(column.type, BigInteger)
    ]

    for column_name in bigint_columns:
        if column_name not in df.columns:
            continue

        numeric_series = pd.to_numeric(df[column_name], errors="coerce")

        bad_mask = (
            numeric_series.notna()
            & (
                (numeric_series < bigint_min)
                | (numeric_series > bigint_max)
            )
        )

        if bad_mask.any():
            examples = df.loc[bad_mask, column_name].head(20).tolist()

            logging.error(
                "%s.%s: значения не помещаются в BIGINT. Примеры: %s",
                table_name,
                column_name,
                examples,
            )


def deduplicate_dataframe_by_conflict_column(
    df: pd.DataFrame,
    conflict_column: str,
    table_name: str,
) -> pd.DataFrame:
    """
    Убирает дубли по ключу upsert до отправки batch в PostgreSQL.
    """

    if df.empty:
        return df

    if conflict_column not in df.columns:
        raise ValueError(
            f"Конфликтная колонка {conflict_column} отсутствует в DataFrame "
            f"для таблицы {table_name}"
        )

    null_key_count = df[conflict_column].isna().sum()

    if null_key_count > 0:
        raise ValueError(
            f"{table_name}: найдено пустых значений в ключевой колонке "
            f"{conflict_column}: {null_key_count}"
        )

    duplicate_mask = df[conflict_column].duplicated(keep=False)

    if not duplicate_mask.any():
        return df

    duplicate_rows_count = int(duplicate_mask.sum())
    duplicate_keys_count = int(df.loc[duplicate_mask, conflict_column].nunique())

    logging.warning(
        "%s: найдены дубли по %s перед upsert. Строк-дублей: %s, "
        "уникальных ключей-дублей: %s",
        table_name,
        conflict_column,
        duplicate_rows_count,
        duplicate_keys_count,
    )

    diagnostic_columns = [
        column for column in [
            conflict_column,
            "created_at",
            "date_modified",
            "date_added",
            "status_name",
            "custom_status_name",
            "updated_at",
        ]
        if column in df.columns
    ]

    logging.warning(
        "%s: примеры дублей перед дедупликацией: %s",
        table_name,
        df.loc[duplicate_mask, diagnostic_columns]
        .sort_values(by=[conflict_column])
        .head(50)
        .to_dict(orient="records"),
    )

    df = df.copy()
    df["__source_row_order"] = range(len(df))

    priority_date_columns = [
        column for column in [
            "date_modified",
            "date_added",
            "created_at",
            "updated_at",
        ]
        if column in df.columns
    ]

    sort_columns = [conflict_column] + priority_date_columns + ["__source_row_order"]

    df = df.sort_values(
        by=sort_columns,
        ascending=True,
        na_position="first",
    )

    before_count = len(df)

    df = df.drop_duplicates(
        subset=[conflict_column],
        keep="last",
    )

    df = df.drop(columns=["__source_row_order"])

    after_count = len(df)

    logging.warning(
        "%s: после дедупликации по %s осталось %s/%s строк. Удалено: %s",
        table_name,
        conflict_column,
        after_count,
        before_count,
        before_count - after_count,
    )

    return df


def assert_records_have_no_nan(records: list[dict], table_name: str):
    for row_idx, row in enumerate(records):
        for key, value in row.items():
            if isinstance(value, float) and math.isnan(value):
                raise ValueError(
                    f"{table_name}: обнаружен NaN перед вставкой. "
                    f"row={row_idx}, column={key}"
                )

            if isinstance(value, np.floating) and np.isnan(value):
                raise ValueError(
                    f"{table_name}: обнаружен numpy NaN перед вставкой. "
                    f"row={row_idx}, column={key}"
                )


def upsert_dataframe(
    pg_hook: PostgresHook,
    table_name: str,
    df: pd.DataFrame,
    conflict_column: str,
):
    if df.empty:
        logging.warning("Нет данных для загрузки в %s", table_name)
        return

    engine, table = get_pg_table(pg_hook, table_name)

    df = align_dataframe_to_pg_table(df, table)

    df = deduplicate_dataframe_by_conflict_column(
        df=df,
        conflict_column=conflict_column,
        table_name=table_name,
    )

    if conflict_column not in df.columns:
        raise ValueError(
            f"Конфликтная колонка {conflict_column} отсутствует в DataFrame "
            f"для таблицы {table_name}"
        )

    log_nulls(df, table_name)
    log_bigint_problems(df, table, table_name)

    df = df.astype(object)

    records = dataframe_to_pg_records(df)
    assert_records_have_no_nan(records, table_name)

    if not records:
        logging.warning(
            "После подготовки не осталось записей для загрузки в %s",
            table_name,
        )
        return

    total = 0

    logging.info(
        "Начинаем upsert в %s. Всего записей: %s",
        table_name,
        len(records),
    )

    with engine.begin() as conn:
        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i:i + BATCH_SIZE]

            stmt = insert(table).values(batch)

            update_set = {
                column_name: stmt.excluded[column_name]
                for column_name in df.columns
                if column_name != conflict_column
            }

            stmt = stmt.on_conflict_do_update(
                index_elements=[conflict_column],
                set_=update_set,
            )

            try:
                conn.execute(stmt)
            except Exception:
                logging.exception(
                    "Ошибка при загрузке batch в %s. Диапазон строк: %s–%s. "
                    "Для точной диагностики временно поставь BATCH_SIZE = 1.",
                    table_name,
                    i,
                    i + len(batch) - 1,
                )
                raise

            total += len(batch)

            logging.info(
                "%s: загружено %s/%s",
                table_name,
                total,
                len(records),
            )

    logging.info(
        "✓ %s: итого загружено/обновлено %s записей",
        table_name,
        total,
    )


def load_mysql_to_pg(**context):
    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN_ID)
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    date_from, date_to_exclusive = get_load_period(**context)

    logging.info(
        "Период загрузки Macro CRM: %s — %s не включительно",
        date_from,
        date_to_exclusive,
    )

    df_buys = fetch_estate_buys(
        mysql_hook=mysql_hook,
        date_from=date_from,
        date_to_exclusive=date_to_exclusive,
    )

    if not df_buys.empty:
        df_buys["updated_at"] = datetime.now()

    upsert_dataframe(
        pg_hook=pg_hook,
        table_name=MACRO_BUYS_TABLE,
        df=df_buys,
        conflict_column="estate_buy_id",
    )

    df_utm = fetch_estate_buys_utm(
        mysql_hook=mysql_hook,
        date_from=date_from,
        date_to_exclusive=date_to_exclusive,
    )

    if not df_utm.empty:
        df_utm["updated_at"] = datetime.now()

    upsert_dataframe(
        pg_hook=pg_hook,
        table_name=MACRO_UTM_TABLE,
        df=df_utm,
        conflict_column="id",
    )


with DAG(
    dag_id="macro_mysql_to_pg_loader",
    default_args=default_args,
    schedule_interval="45 21 * * *",
    catchup=False,
    tags=["macro", "mysql", "etl"],
    description="Загрузка заявок и UTM из MySQL macroData в PostgreSQL",
) as dag:

    load_task = PythonOperator(
        task_id="load_mysql_to_pg",
        python_callable=load_mysql_to_pg,
        provide_context=True,
    )

    load_task