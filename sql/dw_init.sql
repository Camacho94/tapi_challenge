-- ============================================================
-- DW Init — Schemas y tablas del Data Warehouse
-- ============================================================

-- Schemas por capa
-- raw:          tablas donde Python deposita los datos crudos
-- staging:      vistas dbt con limpieza básica sobre raw
-- snapshots:    tablas SCD Type 2 gestionadas por dbt snapshots
-- intermediate: modelos dbt con joins y enriquecimiento
-- marts:        tablas finales con métricas calculadas (fact + rpt)
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS snapshots;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- ── RAW — tablas destino de los scripts Python ────────────────

-- raw.stg_payments es append-only: nunca se sobreescribe un registro.
-- Cada ejecución de extract.py inserta las filas nuevas o actualizadas como
-- filas adicionales, preservando el historial completo de cambios de status.
-- La deduplicación (quedarse con la versión más reciente) ocurre en la capa
-- staging de dbt (stg_payments.sql) mediante ROW_NUMBER().
CREATE TABLE IF NOT EXISTS raw.stg_payments (
    transaction_id       VARCHAR(64)     NOT NULL,
    created_at           TIMESTAMP       NOT NULL,
    updated_at           TIMESTAMP,
    status               VARCHAR(20),
    amount               DECIMAL(12, 2),
    type                 VARCHAR(30),
    company_code         VARCHAR(50),
    external_provider_id VARCHAR(50),
    client_id            VARCHAR(50),
    external_client_id   VARCHAR(50),
    loaded_at            TIMESTAMP       DEFAULT NOW()
);

-- Índice para la deduplicación en dbt y para el filtro de watermark en extract.py
CREATE INDEX IF NOT EXISTS idx_stg_payments_txn_updated
    ON raw.stg_payments (transaction_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS raw.stg_providers_commission (
    external_provider_id VARCHAR(50)     NOT NULL,
    company_code         VARCHAR(50)     NOT NULL,
    tapi_commission      DECIMAL(5, 4),
    commission_type      VARCHAR(30),
    loaded_at            TIMESTAMP       DEFAULT NOW(),
    PRIMARY KEY (external_provider_id, company_code)
);

CREATE TABLE IF NOT EXISTS raw.stg_clients_revenue_share (
    client_id        VARCHAR(50)     PRIMARY KEY,
    revenue_share    DECIMAL(5, 4),
    commission_type  VARCHAR(30),
    loaded_at        TIMESTAMP       DEFAULT NOW()
);

-- ── CONTROL TABLE (watermark incremental) ────────────────────

CREATE TABLE IF NOT EXISTS raw.pipeline_watermark (
    table_name       VARCHAR(100)    PRIMARY KEY,
    last_loaded_at   TIMESTAMP       NOT NULL DEFAULT '1970-01-01'
);

INSERT INTO raw.pipeline_watermark (table_name, last_loaded_at)
VALUES ('process_payments_challenge', '1970-01-01')
ON CONFLICT (table_name) DO NOTHING;

-- ── STAGING, SNAPSHOTS, INTERMEDIATE, MARTS ──────────────────
-- Las tablas de estas capas son gestionadas íntegramente por dbt.
-- dbt crea y mantiene los objetos al ejecutar:
--   dbt snapshot   → snapshots.*
--   dbt run        → staging.*, intermediate.*, marts.*
