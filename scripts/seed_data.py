"""
seed_data.py — Carga de datos de prueba en el OLTP

Genera datos realistas pero acotados para el challenge:
  - ~10 registros en providers_commission
  - ~10 registros en clients_revenue_share
  - ~100 transacciones en process_payments_challenge

Uso:
  python seed_data.py             # carga todo desde cero (trunca primero)
  python seed_data.py --add 10    # agrega 10 transacciones nuevas (live demo: filas nuevas)
  python seed_data.py --update 5  # cambia 5 pending → confirmed (live demo: cambio de status)
"""

import argparse
import logging
import os
import random
import uuid
from datetime import datetime, timedelta, timezone

import psycopg2
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)

OLTP_DSN = {
    "host":     os.getenv("OLTP_HOST",     "localhost"),
    "port":     int(os.getenv("OLTP_PORT", 5433)),
    "dbname":   os.getenv("OLTP_DB",       "oltp_db"),
    "user":     os.getenv("OLTP_USER",     "oltp_user"),
    "password": os.getenv("OLTP_PASSWORD", "oltp_pass"),
}

# ── Datos de referencia ───────────────────────────────────────────────────────

CLIENTS = [
    ("CLIENT_MERCADO",  0.60),
    ("CLIENT_UALA",     0.55),
    ("CLIENT_NARANJA",  0.65),
    ("CLIENT_BRUBANK",  0.50),
]

# Comisiones por proveedor/biller: (provider, biller, commission)
# Proveedores: procesadoras de pago LATAM (Prisma, Paganza, PlacetoPay, PayRetailers)
# Billers: empresas de servicios que reciben los pagos (telefonía, cable, utilities)
PROVIDER_BILLER_COMMISSIONS = [
    ("PROV_PRISMA",       "BILLER_MOVISTAR", 0.0500),
    ("PROV_PRISMA",       "BILLER_CLARO",    0.0480),
    ("PROV_PAGANZA",      "BILLER_CLARO",    0.0520),
    ("PROV_PAGANZA",      "BILLER_PERSONAL", 0.0450),
    ("PROV_PAGANZA",      "BILLER_DIRECTV",  0.0400),
    ("PROV_PLACETOPAY",   "BILLER_PERSONAL", 0.0550),
    ("PROV_PLACETOPAY",   "BILLER_MOVISTAR", 0.0490),
    ("PROV_PAYRETAILERS", "BILLER_TELECOM",  0.0600),
    ("PROV_PAYRETAILERS", "BILLER_DIRECTV",  0.0420),
    ("PROV_PAYRETAILERS", "BILLER_CLARO",    0.0510),
]

STATUSES = ["confirmed", "confirmed", "confirmed", "confirmed", "pending", "rejected"]
# distribución: ~67% confirmed, ~17% pending, ~17% rejected

AMOUNTS = [50, 100, 150, 200, 250, 300, 500]


# ── Helpers ───────────────────────────────────────────────────────────────────

def random_transaction(days_back_max: int = 90) -> dict:
    provider, biller, _ = random.choice(PROVIDER_BILLER_COMMISSIONS)
    client_id, _ = random.choice(CLIENTS)
    created_at = datetime.now(timezone.utc) - timedelta(
        days=random.randint(0, days_back_max),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )
    return {
        "transaction_id":       str(uuid.uuid4()),
        "created_at":           created_at,
        "updated_at":           created_at,   # updated_at = created_at en estado inicial
        "status":               random.choice(STATUSES),
        "amount":               random.choice(AMOUNTS),
        "type":                 "recarga",
        "company_code":         biller,
        "external_provider_id": provider,
        "client_id":            client_id,
        "external_client_id":   f"USER_{random.randint(1000, 9999)}",
    }


# ── Seeders ───────────────────────────────────────────────────────────────────

def seed_dimensions(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE providers_commission RESTART IDENTITY CASCADE")
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO providers_commission
                (external_provider_id, company_code, tapi_commission, commission_type)
            VALUES %s
            """,
            [
                (provider, biller, commission, "amount")
                for provider, biller, commission in PROVIDER_BILLER_COMMISSIONS
            ],
        )
        log.info("providers_commission: %d registros insertados", len(PROVIDER_BILLER_COMMISSIONS))

        cur.execute("TRUNCATE TABLE clients_revenue_share RESTART IDENTITY CASCADE")
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO clients_revenue_share (client_id, revenue_share, commission_type)
            VALUES %s
            """,
            [
                (client_id, revenue_share, "revenue_share")
                for client_id, revenue_share in CLIENTS
            ],
        )
        log.info("clients_revenue_share: %d registros insertados", len(CLIENTS))

    conn.commit()


def seed_transactions(conn, n: int = 100) -> None:
    rows = [random_transaction() for _ in range(n)]
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE process_payments_challenge")
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO process_payments_challenge (
                transaction_id, created_at, updated_at, status, amount, type,
                company_code, external_provider_id, client_id, external_client_id
            ) VALUES %s
            """,
            [
                (
                    r["transaction_id"], r["created_at"], r["updated_at"], r["status"],
                    r["amount"], r["type"], r["company_code"],
                    r["external_provider_id"], r["client_id"], r["external_client_id"],
                )
                for r in rows
            ],
        )
    conn.commit()
    log.info("process_payments_challenge: %d transacciones insertadas", n)


def add_new_transactions(conn, n: int = 10) -> None:
    """Agrega N transacciones recientes al OLTP (live demo: filas nuevas)."""
    rows = [random_transaction(days_back_max=0) for _ in range(n)]
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO process_payments_challenge (
                transaction_id, created_at, updated_at, status, amount, type,
                company_code, external_provider_id, client_id, external_client_id
            ) VALUES %s
            ON CONFLICT (transaction_id) DO NOTHING
            """,
            [
                (
                    r["transaction_id"], r["created_at"], r["updated_at"], r["status"],
                    r["amount"], r["type"], r["company_code"],
                    r["external_provider_id"], r["client_id"], r["external_client_id"],
                )
                for r in rows
            ],
        )
    conn.commit()
    log.info("%d transacciones nuevas agregadas al OLTP", n)


def update_pending_transactions(conn, n: int = 5) -> None:
    """
    Cambia N transacciones pending → confirmed (live demo: cambio de status).
    El trigger trg_payments_updated_at actualiza updated_at automáticamente,
    lo que hace que extract.py las detecte en el siguiente run del pipeline.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE process_payments_challenge
            SET status = 'confirmed'
            WHERE transaction_id IN (
                SELECT transaction_id
                FROM process_payments_challenge
                WHERE status = 'pending'
                ORDER BY created_at DESC
                LIMIT %s
            )
            """,
            (n,),
        )
        updated = cur.rowcount
    conn.commit()
    log.info("%d transacciones actualizadas: pending → confirmed", updated)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seed data para el OLTP de Tapi")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--add",
        type=int,
        metavar="N",
        help="Agrega N transacciones nuevas (live demo: filas nuevas)",
    )
    group.add_argument(
        "--update",
        type=int,
        metavar="N",
        help="Cambia N transacciones pending → confirmed (live demo: cambio de status)",
    )
    args = parser.parse_args()

    with psycopg2.connect(**OLTP_DSN) as conn:
        if args.add:
            add_new_transactions(conn, n=args.add)
        elif args.update:
            update_pending_transactions(conn, n=args.update)
        else:
            log.info("Iniciando seed completo del OLTP...")
            seed_dimensions(conn)
            seed_transactions(conn, n=100)
            log.info("Seed completo finalizado.")
