-- dim_clients
-- Dimensión de clientes fintech con el revenue share vigente (is_current = TRUE).
-- Filtra el snapshot SCD Type 2 quedándose solo con el registro activo
-- (dbt_valid_to IS NULL), exponiendo la tasa actual de cada cliente.
--
-- Materializada como table: consulta directa desde el dashboard sin pasar
-- por la lógica de versionado del snapshot.

with snapshot as (
    select * from {{ ref('clients_revenue_share_snapshot') }}
    where dbt_valid_to is null
)

select
    client_id,
    revenue_share,
    commission_type,
    dbt_valid_from      as valid_from
from snapshot
