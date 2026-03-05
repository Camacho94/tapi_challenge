"""
load.py — Full refresh de tablas dimensionales OLTP → staging DW

Carga providers_commission y clients_revenue_share en las tablas de staging.
dbt se encarga luego de aplicar SCD Type 2 a partir de esas tablas.

Estrategia:
  - TRUNCATE + INSERT para garantizar consistencia con la fuente
  - Se ejecuta antes de dbt run para que los snapshots detecten cambios
"""

import logging
import os
from datetime import datetime

import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

OLTP_DSN = {
    "host":     os.getenv("OLTP_HOST",     "localhost"),
    "port":     int(os.getenv("OLTP_PORT", 5433)),   # 5433 = puerto expuesto en docker-compose
    "dbname":   os.getenv("OLTP_DB",       "oltp_db"),
    "user":     os.getenv("OLTP_USER",     "oltp_user"),
    "password": os.getenv("OLTP_PASSWORD", "oltp_pass"),
}

DW_DSN = {
    "host":     os.getenv("DW_HOST",     "localhost"),
    "port":     int(os.getenv("DW_PORT", 5434)),     # 5434 = puerto expuesto en docker-compose (5432 ocupado por PG local)
    "dbname":   os.getenv("DW_DB",       "dw_db"),
    "user":     os.getenv("DW_USER",     "dw_user"),
    "password": os.getenv("DW_PASSWORD", "dw_pass"),
}


# ── Loaders ──────────────────────────────────────────────────────────────────

def load_providers_commission() -> int:
    """
    Full refresh de providers_commission → staging.stg_providers_commission.
    Solo carga comisiones de tipo 'amount', según especificación del challenge.
    Retorna cantidad de filas cargadas.
    """
    with (
        psycopg2.connect(**OLTP_DSN) as oltp_conn,
        psycopg2.connect(**DW_DSN) as dw_conn,
    ):
        with oltp_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as oltp_cur:
            oltp_cur.execute(
                """
                SELECT external_provider_id, company_code, tapi_commission, commission_type
                FROM providers_commission
                WHERE commission_type = 'amount'
                """
            )
            rows = oltp_cur.fetchall()

        with dw_conn.cursor() as dw_cur:
            dw_cur.execute("TRUNCATE TABLE raw.stg_providers_commission")
            if rows:
                psycopg2.extras.execute_values(
                    dw_cur,
                    """
                    INSERT INTO raw.stg_providers_commission (
                        external_provider_id, company_code,
                        tapi_commission, commission_type, loaded_at
                    ) VALUES %s
                    """,
                    [
                        (
                            r["external_provider_id"],
                            r["company_code"],
                            r["tapi_commission"],
                            r["commission_type"],
                            datetime.utcnow(),
                        )
                        for r in rows
                    ],
                )
        dw_conn.commit()

    log.info("providers_commission cargado: %d filas", len(rows))
    return len(rows)


def load_clients_revenue_share() -> int:
    """
    Full refresh de clients_revenue_share → staging.stg_clients_revenue_share.
    Solo carga acuerdos de tipo 'revenue_share', según especificación del challenge.
    Retorna cantidad de filas cargadas.
    """
    with (
        psycopg2.connect(**OLTP_DSN) as oltp_conn,
        psycopg2.connect(**DW_DSN) as dw_conn,
    ):
        with oltp_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as oltp_cur:
            oltp_cur.execute(
                """
                SELECT client_id, revenue_share, commission_type
                FROM clients_revenue_share
                WHERE commission_type = 'revenue_share'
                """
            )
            rows = oltp_cur.fetchall()

        with dw_conn.cursor() as dw_cur:
            dw_cur.execute("TRUNCATE TABLE raw.stg_clients_revenue_share")
            if rows:
                psycopg2.extras.execute_values(
                    dw_cur,
                    """
                    INSERT INTO raw.stg_clients_revenue_share (
                        client_id, revenue_share, commission_type, loaded_at
                    ) VALUES %s
                    """,
                    [
                        (
                            r["client_id"],
                            r["revenue_share"],
                            r["commission_type"],
                            datetime.utcnow(),
                        )
                        for r in rows
                    ],
                )
        dw_conn.commit()

    log.info("clients_revenue_share cargado: %d filas", len(rows))
    return len(rows)


# ── Entry point ──────────────────────────────────────────────────────────────

def load_dimensions() -> None:
    """Ejecuta el full refresh de todas las tablas dimensionales."""
    load_providers_commission()
    load_clients_revenue_share()
    log.info("Carga de dimensiones completada.")


if __name__ == "__main__":
    load_dimensions()
