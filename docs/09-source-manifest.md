# Манифест исходных файлов

Код DAG-ов не редактировался. Для чистой структуры репозитория нормализованы только имена двух файлов.

| Исходный файл | Файл в комплекте | DAG ID |
|---|---|---|
| `yandex_direct_dag.py` | `dags/yandex_direct_loader.py` | `yandex_direct_loader` |
| `macro_mysql_to_pg_loader.py` | `dags/macro_mysql_to_pg_loader.py` | `macro_mysql_to_pg_loader` |
| `crm_yandex_leads_matched_loader(1).py` | `dags/crm_yandex_leads_matched_loader.py` | `crm_yandex_leads_matched_loader` |
| `daily_deal_aggregates_loader.py` | `dags/daily_deal_aggregates_loader.py` | `daily_deal_aggregates_loader` |
| `yandex_criterion_performance_loader.py` | `dags/yandex_criterion_performance_loader.py` | `yandex_criterion_performance_loader` |

Проверка:

```bash
python -m py_compile dags/*.py
```

Проверка `py_compile` подтверждает синтаксис Python, но не проверяет доступность Airflow providers, Connections, Variables, таблиц и API.
