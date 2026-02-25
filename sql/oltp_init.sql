-- ============================================================
-- OLTP Init — Tablas fuente productivas de Tapi
-- ============================================================

-- Tabla principal de transacciones
CREATE TABLE IF NOT EXISTS process_payments_challenge (
    transaction_id       VARCHAR(64)     PRIMARY KEY,
    created_at           TIMESTAMP       NOT NULL,
    updated_at           TIMESTAMP       NOT NULL DEFAULT NOW(),  -- se actualiza al cambiar status
    status               VARCHAR(20)     NOT NULL,   -- confirmed / pending / rejected
    amount               DECIMAL(12, 2)  NOT NULL,
    type                 VARCHAR(30)     NOT NULL,   -- recarga / other
    company_code         VARCHAR(50)     NOT NULL,
    external_provider_id VARCHAR(50)     NOT NULL,
    client_id            VARCHAR(50)     NOT NULL,
    external_client_id   VARCHAR(50)     NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_payments_created_at
    ON process_payments_challenge (created_at);

CREATE INDEX IF NOT EXISTS idx_payments_updated_at
    ON process_payments_challenge (updated_at);

CREATE INDEX IF NOT EXISTS idx_payments_type_status
    ON process_payments_challenge (type, status);

-- Trigger para mantener updated_at sincronizado automáticamente
CREATE OR REPLACE FUNCTION fn_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_payments_updated_at
BEFORE UPDATE ON process_payments_challenge
FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- Comisiones por proveedor/biller
CREATE TABLE IF NOT EXISTS providers_commission (
    id                   SERIAL          PRIMARY KEY,
    external_provider_id VARCHAR(50)     NOT NULL,
    company_code         VARCHAR(50)     NOT NULL,
    tapi_commission      DECIMAL(5, 4)   NOT NULL,   -- e.g. 0.0500 = 5%
    commission_type      VARCHAR(30)     NOT NULL,   -- amount / fixed
    UNIQUE (external_provider_id, company_code)
);

-- Revenue share por cliente fintech
CREATE TABLE IF NOT EXISTS clients_revenue_share (
    id               SERIAL          PRIMARY KEY,
    client_id        VARCHAR(50)     NOT NULL UNIQUE,
    revenue_share    DECIMAL(5, 4)   NOT NULL,       -- e.g. 0.6000 = 60%
    commission_type  VARCHAR(30)     NOT NULL        -- revenue_share
);
