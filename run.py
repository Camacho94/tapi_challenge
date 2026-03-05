"""
run.py — Inicialización y ejecución completa del pipeline Tapi

Ejecuta todo en orden con un solo comando:
    python run.py

Pasos:
    1. docker-compose up -d
    2. Espera que OLTP y DW estén listos
    3. load.py       (dimensiones al DW, full refresh)
    4. extract.py    (pagos al DW, incremental por updated_at)
    5. dbt deps      (descarga paquetes dbt si hace falta)
    6. dbt snapshot  (SCD Type 2)
    7. dbt run       (staging → intermediate → marts)

Nota: seed_data.py es independiente y debe correrse por separado
para poblar el OLTP con datos de prueba antes de ejecutar este script.

Flag opcional:
    --no-docker   Omite docker-compose up (si los contenedores ya corren)
"""

import argparse
import subprocess
import sys
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger(__name__)


# ── Configuración ─────────────────────────────────────────────────────────────

OLTP_DSN = dict(host="localhost", port=5433, dbname="oltp_db", user="oltp_user", password="oltp_pass")
DW_DSN   = dict(host="localhost", port=5434, dbname="dw_db",   user="dw_user",   password="dw_pass")

DBT_DIR = "dbt"


# ── Helpers ───────────────────────────────────────────────────────────────────

def wait_for_postgres(dsn: dict, name: str, retries: int = 30, delay: float = 2.0) -> None:
    """Espera hasta que PostgreSQL acepte conexiones."""
    import psycopg2
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**dsn, connect_timeout=3)
            conn.close()
            log.info("%s listo.", name)
            return
        except Exception:
            log.info("Esperando %s... (%d/%d)", name, attempt, retries)
            time.sleep(delay)
    log.error("%s no respondio en %d intentos. Abortando.", name, retries)
    sys.exit(1)


def run_python(script: str) -> None:
    """Ejecuta un script Python del proyecto."""
    log.info("Ejecutando %s ...", script)
    result = subprocess.run([sys.executable, script], check=False)
    if result.returncode != 0:
        log.error("%s fallo con codigo %d", script, result.returncode)
        sys.exit(result.returncode)


def run_dbt(*args) -> None:
    """Ejecuta un comando dbt via dbt_run.py (maneja os._exit de dbt 1.11)."""
    log.info("dbt %s", " ".join(args))
    result = subprocess.run([sys.executable, "dbt_run.py"] + list(args), check=False)
    if result.returncode != 0:
        log.error("dbt %s fallo.", args[0])
        sys.exit(result.returncode)


# ── Pipeline ──────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Corre el pipeline Tapi de punta a punta.")
    parser.add_argument("--no-docker", action="store_true", help="Omite docker-compose up")
    args = parser.parse_args()

    # 1. Levantar Docker
    if not args.no_docker:
        log.info("Levantando contenedores Docker...")
        subprocess.run(["docker-compose", "up", "-d"], check=True)

    # 2. Esperar a que las DBs estén listas
    wait_for_postgres(OLTP_DSN, "OLTP (puerto 5433)")
    wait_for_postgres(DW_DSN,   "DW   (puerto 5434)")

    # 3. Cargar dimensiones al DW
    run_python("scripts/load.py")

    # 4. Extraer pagos al DW
    run_python("scripts/extract.py")

    # 5-7. dbt
    run_dbt("deps")
    run_dbt("snapshot")
    run_dbt("run")

    log.info("Pipeline completado. Datos disponibles en marts.fact_payments y marts.rpt_daily_revenue")
    log.info("Conexion DW: host=localhost port=5434 dbname=dw_db user=dw_user password=dw_pass")


if __name__ == "__main__":
    main()
