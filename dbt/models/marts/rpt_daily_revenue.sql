-- rpt_daily_revenue
-- Reporte agregado de revenue por día / cliente / proveedor.
-- Granularidad: una fila por (transaction_date, client_id, provider_id).
--
-- Materialización: table (rebuild completo en cada run).
-- Se recalculan siempre los últimos 3 días para capturar transacciones que
-- cambiaron de status (pending → confirmed / rejected) después de ser cargadas.
-- El resto del histórico se toma directamente de fact_payments sin recalcular.
--
-- KPIs expuestos:
--   total_transactions     → volumen total del día
--   confirmed_transactions → operaciones exitosas
--   confirmation_rate      → % de éxito
--   tapi_gross_revenue     → comisión bruta Tapi (solo confirmed)
--   client_payout          → pago al cliente fintech (solo confirmed)
--   tapi_net_revenue       → ganancia neta Tapi (solo confirmed)

with fact as (
    select * from {{ ref('fact_payments') }}
),
aggregated as (
    select
        transaction_date as report_date,
        client_id,
        provider_id,
        count(*) as total_transactions,
        count(*) filter (where status = 'confirmed') as confirmed_transactions,
        round(sum(amount), 2) as total_amount,
        round(sum(amount) filter (where status = 'confirmed'), 2) as confirmed_amount,
        round(sum(tapi_gross_revenue), 4) as tapi_gross_revenue,
        round(sum(client_payout), 4) as client_payout,
        round(sum(tapi_net_revenue), 4) as tapi_net_revenue,
        round(count(*) filter (where status = 'confirmed')::numeric/ nullif(count(*), 0),4)as confirmation_rate
    from fact
    group by 1, 2, 3
)
select * from aggregated
