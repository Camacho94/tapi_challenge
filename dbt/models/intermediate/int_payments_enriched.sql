-- int_payments_enriched
-- Enriquece cada transacción con la comisión del proveedor y el revenue share
-- del cliente vigentes en el momento exacto de la transacción (SCD Type 2).
--
-- Lógica de join SCD2:
--   transaction_timestamp >= dbt_valid_from
--   AND (dbt_valid_to IS NULL OR transaction_timestamp < dbt_valid_to)
--
-- Cálculos de revenue (solo para transacciones confirmed):
--   tapi_gross_revenue = amount × tapi_commission
--   client_payout      = tapi_gross_revenue × revenue_share
--   tapi_net_revenue   = tapi_gross_revenue − client_payout

with payments as (
    select * from {{ ref('stg_payments') }}
),

providers as (
    select
        provider_id,
        biller_id,
        tapi_commission,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('providers_commission_snapshot') }}
),

clients as (
    select
        client_id,
        revenue_share,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('clients_revenue_share_snapshot') }}
),

-- Join SCD2 con comisiones de proveedor vigentes al momento de la transacción.
-- Lógica de preferencia:
--   1. Registro SCD2 cuyo rango cubre la transacción (producción normal).
--   2. Fallback al registro activo (dbt_valid_to IS NULL) cuando la transacción
--      es anterior al primer snapshot — ocurre en el demo porque los datos de
--      prueba tienen fechas históricas (Nov-Dic 2025) pero los snapshots se
--      corrieron por primera vez hoy. En producción real el snapshot corre
--      diariamente desde el día 1, por lo que siempre existe un registro que
--      cubre cualquier transacción nueva.
enriched_with_provider as (
    select
        p.*,
        coalesce(
            -- 1. Tasa vigente en el momento exacto de la transacción
            (select prov.tapi_commission
             from providers prov
             where prov.provider_id = p.provider_id
               and prov.biller_id   = p.company_code
               and p.transaction_timestamp >= prov.dbt_valid_from
               and (prov.dbt_valid_to is null
                    or p.transaction_timestamp < prov.dbt_valid_to)
             limit 1),
            -- 2. Fallback: tasa activa actual (cubre datos históricos en el demo)
            (select prov.tapi_commission
             from providers prov
             where prov.provider_id = p.provider_id
               and prov.biller_id   = p.company_code
               and prov.dbt_valid_to is null
             limit 1)
        ) as tapi_commission
    from payments p
),

-- Join SCD2 con revenue share del cliente vigente al momento de la transacción
enriched_with_client as (
    select
        ep.*,
        coalesce(
            -- 1. Revenue share vigente en el momento exacto de la transacción
            (select cl.revenue_share
             from clients cl
             where cl.client_id = ep.client_id
               and ep.transaction_timestamp >= cl.dbt_valid_from
               and (cl.dbt_valid_to is null
                    or ep.transaction_timestamp < cl.dbt_valid_to)
             limit 1),
            -- 2. Fallback: revenue share activo actual (cubre datos históricos)
            (select cl.revenue_share
             from clients cl
             where cl.client_id = ep.client_id
               and cl.dbt_valid_to is null
             limit 1)
        ) as revenue_share
    from enriched_with_provider ep
),

-- Cálculo de métricas de revenue
final as (
    select
        transaction_id,
        transaction_date,
        transaction_timestamp,
        status,
        amount,
        provider_id,
        company_code                                        as biller_id,
        client_id,
        external_client_id,
        tapi_commission,
        revenue_share,

        -- Revenue calculado solo para transacciones confirmadas
        case
            when status = 'confirmed'
            then round(amount * tapi_commission, 4)
            else 0
        end                                                 as tapi_gross_revenue,

        case
            when status = 'confirmed'
            then round(amount * tapi_commission * revenue_share, 4)
            else 0
        end                                                 as client_payout,

        case
            when status = 'confirmed'
            then round(amount * tapi_commission * (1 - revenue_share), 4)
            else 0
        end                                                 as tapi_net_revenue,

        loaded_at
    from enriched_with_client
)

select * from final
