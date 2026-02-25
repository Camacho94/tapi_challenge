-- dim_providers
-- Dimensión de proveedores con las comisiones vigentes (is_current = TRUE).
-- Filtra el snapshot SCD Type 2 quedándose solo con el registro activo
-- (dbt_valid_to IS NULL), exponiendo la tasa actual de cada par proveedor/biller.
--
-- Materializada como table: consulta directa desde el dashboard sin pasar
-- por la lógica de versionado del snapshot.

with snapshot as (
    select * from {{ ref('providers_commission_snapshot') }}
    where dbt_valid_to is null
)

select
    provider_id,
    biller_id,
    tapi_commission,
    commission_type,
    dbt_valid_from      as valid_from
from snapshot
