# Tapi — Data Engineering Challenge

Pipeline ETL completo desde tablas productivas OLTP hasta visualización de KPIs, implementado con Python, dbt y Airflow sobre PostgreSQL.

---

## Arquitectura

```
┌─────────────────────────────────────────────────────────────────┐
│  OLTP (PostgreSQL)                                              │
│  process_payments_challenge  providers_commission               │
│  clients_revenue_share                                          │
└────────────────────┬───────────────────┬────────────────────────┘
                     │ extract.py         │ load.py
                     │ (incremental       │ (full refresh
                     │  por updated_at)   │  dimensiones)
                     ▼                   ▼
┌─────────────────────────────────────────────────────────────────┐
│  DW — schema raw (PostgreSQL)                                   │
│  raw.stg_payments   raw.stg_providers_commission                │
│  raw.stg_clients_revenue_share   raw.pipeline_watermark         │
└──────────────────────────┬──────────────────────────────────────┘
                           │ dbt run / dbt snapshot
          ┌────────────────┼────────────────────┐
          ▼                ▼                    ▼
     staging.*        snapshots.*         intermediate.*
     (views 1:1)      (SCD Type 2)        int_payments_enriched
                                          (join SCD2 + revenue KPIs)
                                               │
                                               ▼
                                          marts.*
                                          fact_payments
                                          rpt_daily_revenue
                                               │
                                               ▼
                                       Power BI / Dashboard
```

### Capas del Data Warehouse

| Schema | Gestionado por | Propósito |
|---|---|---|
| `raw` | Python scripts | Landing zone — copia fiel del OLTP |
| `staging` | dbt (views) | Limpieza básica y renombre de columnas |
| `snapshots` | dbt snapshot | SCD Type 2 para providers y clients |
| `intermediate` | dbt (views) | Joins enriquecidos + cálculo de revenue |
| `marts` | dbt (tables) | Tablas finales listas para el dashboard |

### KPIs calculados

| KPI | Fórmula | Nivel |
|---|---|---|
| `tapi_gross_revenue` | `amount × tapi_commission` | Por transacción confirmed |
| `client_payout` | `tapi_gross_revenue × revenue_share` | Por transacción confirmed |
| `tapi_net_revenue` | `tapi_gross_revenue − client_payout` | Por transacción confirmed |

---

## Requisitos

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Python 3.10+
- dbt-postgres (`pip install -r requirements.txt`)

---

## Setup

### 1. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 2. Correr el pipeline

```bash
python run.py
```

El script hace todo en orden:

1. `docker-compose up -d` — levanta OLTP, DW y Airflow
2. Espera que PostgreSQL esté listo
3. `load.py` — carga dimensiones al DW
4. `extract.py` — extrae pagos al DW (incremental)
5. `dbt deps` + `dbt snapshot` + `dbt run` — construye todos los modelos

Al finalizar, los datos están disponibles en `marts.fact_payments` y `marts.rpt_daily_revenue`.

**Flag opcional:**

```bash
python run.py --no-docker  # omite docker-compose up (si los contenedores ya corren)
```

### 3. Poblar el OLTP con datos de prueba

El OLTP arranca vacío. Para generar datos de prueba:

```bash
python scripts/seed_data.py
```

Genera:
- 10 combinaciones proveedor/biller con comisiones
- 4 clientes fintech con revenue share
- 100 transacciones históricas (últimos 90 días)

Luego volvé a correr el pipeline para procesar los datos:

```bash
python run.py --no-docker
```

### Infraestructura levantada

| Servicio | Puerto local | Propósito |
|---|---|---|
| `oltp-db` | 5433 | Base de datos fuente (OLTP) |
| `dw-db` | 5432 | Data Warehouse |
| `airflow-db` | — (interno) | Metadata de Airflow |

Airflow disponible en `http://localhost:8081` (user: `admin`, pass: `admin`).

### Correr pasos individualmente

Si preferís correr paso a paso (o usar la CLI de dbt directamente):

```bash
python scripts/seed_data.py
python scripts/load.py
python scripts/extract.py

# dbt via Python API (funciona sin dbt en el PATH — recomendado en Windows)
python -m dbt deps     --project-dir dbt --profiles-dir dbt
python -m dbt snapshot --project-dir dbt --profiles-dir dbt
python -m dbt run      --project-dir dbt --profiles-dir dbt
python -m dbt test     --project-dir dbt --profiles-dir dbt
```

### Orquestación con Airflow

El DAG `tapi_etl_dag` ejecuta los 4 pasos anteriores automáticamente cada día a las 03:00 UTC. Se puede disparar manualmente desde `http://localhost:8081`.

```
[load_dimensions]  ──┐
                      ├──► [dbt_snapshot] ──► [dbt_run]
[extract_payments] ──┘
```

---

## Demo en vivo — carga incremental

El challenge requiere demostrar que el pipeline captura cambios en tiempo real.

### Caso 1: nuevas transacciones

```bash
# Agregar 10 transacciones nuevas al OLTP
python scripts/seed_data.py --add 10

# Re-correr el pipeline — extract.py detecta las filas nuevas por updated_at
python scripts/extract.py
cd dbt && dbt run --profiles-dir . --project-dir .
```

### Caso 2: cambio de status (pending → confirmed)

```bash
# Cambiar 5 transacciones de pending a confirmed en el OLTP
# El trigger de PostgreSQL actualiza updated_at automáticamente
python scripts/seed_data.py --update 5

# Re-correr el pipeline — extract.py detecta los updated_at modificados
python scripts/extract.py
cd dbt && dbt run --profiles-dir . --project-dir .
```

En ambos casos, `rpt_daily_revenue` refleja los nuevos valores sin necesidad de recargar el histórico completo.

---

## Estrategias de carga

| Tabla | Estrategia | Detalle |
|---|---|---|
| `process_payments_challenge` | Incremental | Watermark en `updated_at` — captura filas nuevas y cambios de status |
| `providers_commission` | Full refresh | Truncate + insert en cada ejecución |
| `clients_revenue_share` | Full refresh | Truncate + insert en cada ejecución |
| `fact_payments` (DW) | Incremental upsert | `delete+insert` por `transaction_id` usando `loaded_at` |
| `rpt_daily_revenue` (DW) | Table rebuild | Recalculo completo — refleja siempre el último estado |

### SCD Type 2

Las comisiones de proveedores y acuerdos de revenue share cambian con el tiempo. En lugar de sobreescribir, se versiona cada cambio:

```
providers_commission_snapshot:
  valid_from  │  valid_to   │  tapi_commission
  2025-01-01  │  2025-06-01 │  0.0500   ← histórico
  2025-06-01  │  NULL       │  0.0480   ← vigente
```

El modelo `int_payments_enriched` aplica el valor vigente **en el momento de cada transacción**, permitiendo recalcular revenue histórico correctamente.

---

## Estructura del repositorio

```
tapi_challenge/
├── docker-compose.yml
├── requirements.txt
├── .env
├── sql/
│   ├── oltp_init.sql           # Schema OLTP con updated_at y trigger
│   └── dw_init.sql             # Schema DW con capas raw/staging/marts
├── scripts/
│   ├── extract.py              # EL incremental de pagos
│   ├── load.py                 # Full refresh de dimensiones
│   └── seed_data.py            # Datos de prueba
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── macros/
│   │   └── generate_schema_name.sql
│   ├── models/
│   │   ├── staging/            # sources.yml + 3 vistas
│   │   ├── intermediate/       # int_payments_enriched
│   │   └── marts/              # fact_payments + rpt_daily_revenue
│   └── snapshots/              # SCD Type 2
└── airflow/
    └── dags/
        └── tapi_etl_dag.py
```
