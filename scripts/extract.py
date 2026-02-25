"""
extract.py — Extracción incremental de process_payments_challenge

Lee desde el OLTP las filas nuevas O actualizadas usando watermark basado
en updated_at, y las inserta en raw.stg_payments del DW (append-only).

Estrategia:
  - Solo se extraen transacciones de tipo 'recarga'
  - El watermark se guarda en raw.pipeline_watermark (campo: last_loaded_at)
  - Filtra por updated_at > watermark: captura tanto filas nuevas como
    filas cuyo status cambió (pending → confirmed / rejected) después
    de haber sido cargadas por primera vez
  - raw.stg_payments es append-only: nunca se sobreescribe. Cada versión de
    una transacción queda registrada como una fila separada, preservando el
    historial completo de cambios de status (Bronze/raw inmutable).
  - La deduplicación ocurre en la capa staging de dbt (ROW_NUMBER).
  - Se procesa en batches para soportar volúmenes grandes
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

BATCH_SIZE = int(os.getenv("EXTRACT_BATCH_SIZE", 1000))
TABLE_NAME = "process_payments_challenge"

OLTP_DSN = {
    "host":     os.getenv("OLTP_HOST",     "localhost"),
    "port":     int(os.getenv("OLTP_PORT", 5433)),   # 5433 = puerto expuesto en docker-compose
    "dbname":   os.getenv("OLTP_DB",       "oltp_db"),
    "user":     os.getenv("OLTP_USER",     "oltp_user"),
    "password": os.getenv("OLTP_PASSWORD", "oltp_pass"),
}

DW_DSN = {
    "host":     os.getenv("DW_HOST",     "localhost"),
    "port":     int(os.getenv("DW_PORT", 5432)),     # 5432 = puerto expuesto en docker-compose
    "dbname":   os.getenv("DW_DB",       "dw_db"),
    "user":     os.getenv("DW_USER",     "dw_user"),
    "password": os.getenv("DW_PASSWORD", "dw_pass"),
}


# ── Watermark ────────────────────────────────────────────────────────────────

def get_watermark(dw_conn) -> datetime:
    with dw_conn.cursor() as cur:
        cur.execute(
            "SELECT last_loaded_at FROM raw.pipeline_watermark WHERE table_name = %s",
            (TABLE_NAME,),
        )
        row = cur.fetchone()
    return row[0] if row else datetime(1970, 1, 1)


def update_watermark(dw_conn, new_watermark: datetime) -> None:
    with dw_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO raw.pipeline_watermark (table_name, last_loaded_at)
            VALUES (%s, %s)
            ON CONFLICT (table_name) DO UPDATE
                SET last_loaded_at = EXCLUDED.last_loaded_at
            """,
            (TABLE_NAME, new_watermark),
        )
    dw_conn.commit()


# ── Extract & Load ───────────────────────────────────────────────────────────

def extract_and_load_payments() -> int:
    """
    Extrae filas nuevas o actualizadas de process_payments_challenge
    usando updated_at como watermark, y las carga en raw.stg_payments del DW.

    Retorna la cantidad de filas procesadas.
    """
    with (
        psycopg2.connect(**OLTP_DSN) as oltp_conn,
        psycopg2.connect(**DW_DSN) as dw_conn,
    ):
        watermark = get_watermark(dw_conn)
        log.info("Watermark actual (updated_at): %s", watermark)

        with oltp_conn.cursor(
            name="payments_cursor",
            cursor_factory=psycopg2.extras.RealDictCursor,
        ) as oltp_cur:

            oltp_cur.execute(
                """
                SELECT
                    transaction_id,
                    created_at,
                    updated_at,
                    status,
                    amount,
                    type,
                    company_code,
                    external_provider_id,
                    client_id,
                    external_client_id
                FROM process_payments_challenge
                WHERE type = 'recarga'
                  AND updated_at > %s
                ORDER BY updated_at ASC
                """,
                (watermark,),
            )

            rows_loaded = 0
            max_updated_at = watermark

            while True:
                batch = oltp_cur.fetchmany(BATCH_SIZE)
                if not batch:
                    break

                with dw_conn.cursor() as dw_cur:
                    psycopg2.extras.execute_values(
                        dw_cur,
                        """
                        INSERT INTO raw.stg_payments (
                            transaction_id, created_at, updated_at, status, amount, type,
                            company_code, external_provider_id, client_id,
                            external_client_id, loaded_at
                        ) VALUES %s
                        """,
                        [
                            (
                                r["transaction_id"],
                                r["created_at"],
                                r["updated_at"],
                                r["status"],
                                r["amount"],
                                r["type"],
                                r["company_code"],
                                r["external_provider_id"],
                                r["client_id"],
                                r["external_client_id"],
                                datetime.utcnow(),
                            )
                            for r in batch
                        ],
                    )
                dw_conn.commit()

                batch_max = max(r["updated_at"] for r in batch)
                if batch_max > max_updated_at:
                    max_updated_at = batch_max

                rows_loaded += len(batch)
                log.info(
                    "Batch insertado: %d filas (acumulado: %d)",
                    len(batch),
                    rows_loaded,
                )

        if rows_loaded > 0:
            update_watermark(dw_conn, max_updated_at)
            log.info("Watermark actualizado a: %s", max_updated_at)
        else:
            log.info("Sin filas nuevas ni actualizadas desde el watermark actual.")

        log.info("Extracción completada. Total filas procesadas: %d", rows_loaded)
        return rows_loaded


if __name__ == "__main__":
    extract_and_load_payments()
