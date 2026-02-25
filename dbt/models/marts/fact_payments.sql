-- fact_payments
-- Tabla de hechos con todas las transacciones enriquecidas y sus métricas.
-- Granularidad: una fila por transacción.
--
-- Materialización: incremental con upsert por transaction_id.
-- En cada run solo procesa registros nuevos o actualizados (por loaded_at),
-- evitando recargar el histórico completo y garantizando idempotencia.

{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='delete+insert'
    )
}}

with source as (
    select * from {{ ref('int_payments_enriched') }}

    {% if is_incremental() %}
    -- Solo trae filas más recientes que el máximo loaded_at ya cargado
    where loaded_at > (select max(loaded_at) from {{ this }})
    {% endif %}
)

select
    transaction_id,
    transaction_date,
    transaction_timestamp,
    status,
    amount,
    tapi_gross_revenue,
    client_payout,
    tapi_net_revenue,
    client_id,
    provider_id,
    biller_id,
    external_client_id,
    tapi_commission,
    revenue_share,
    loaded_at
from source
