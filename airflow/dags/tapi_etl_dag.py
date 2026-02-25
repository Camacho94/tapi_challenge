"""
tapi_etl_dag.py — Pipeline ETL diario de Tapi

Orquesta el flujo completo desde la extracción del OLTP hasta los marts del DW:

  [load_dimensions]  ──┐
                        ├──► [dbt_snapshot] ──► [dbt_run]
  [extract_payments] ──┘

Paso 1 — load_dimensions:
    Full refresh de providers_commission y clients_revenue_share
    desde el OLTP hacia raw.stg_providers_commission y raw.stg_clients_revenue_share.

Paso 2 — extract_payments (paralelo con load_dimensions):
    Carga incremental de process_payments_challenge usando watermark en updated_at.
    Captura filas nuevas Y transacciones que cambiaron de status.

Paso 3 — dbt_snapshot:
    Detecta cambios en las dimensiones y versiona con SCD Type 2.

Paso 4 — dbt_run:
    Ejecuta todos los modelos en orden:
    staging → intermediate → marts (fact_payments + rpt_daily_revenue)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

# ── Configuración del DAG ─────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "tapi-data",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

DBT_DIR = "/opt/airflow/dbt"
SCRIPTS_DIR = "/opt/airflow/scripts"

with DAG(
    dag_id="tapi_etl_dag",
    description="Pipeline ETL diario: OLTP → staging → snapshots → marts",
    schedule_interval="0 3 * * *",   # todos los días a las 03:00 UTC
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["tapi", "etl"],
) as dag:

    # ── Paso 1: carga dimensiones (full refresh) ──────────────────────────────
    load_dimensions = BashOperator(
        task_id="load_dimensions",
        bash_command=f"python {SCRIPTS_DIR}/load.py",
    )

    # ── Paso 2: extracción incremental de pagos ───────────────────────────────
    extract_payments = BashOperator(
        task_id="extract_payments",
        bash_command=f"python {SCRIPTS_DIR}/extract.py",
    )

    # ── Paso 3: snapshots SCD Type 2 ─────────────────────────────────────────
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt snapshot --profiles-dir . --project-dir ."
        ),
    )

    # ── Paso 4: transformaciones dbt (staging → intermediate → marts) ─────────
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --profiles-dir . --project-dir ."
        ),
    )

    # ── Dependencias ──────────────────────────────────────────────────────────
    # load_dimensions y extract_payments corren en paralelo.
    # Ambos deben completarse antes de ejecutar los snapshots y los modelos dbt.
    [load_dimensions, extract_payments] >> dbt_snapshot >> dbt_run
