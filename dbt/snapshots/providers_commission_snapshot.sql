{% snapshot providers_commission_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key="provider_id || '_' || biller_id",
        strategy='check',
        check_cols=['tapi_commission', 'commission_type'],
    )
}}

-- Lee directamente desde raw (no desde la vista staging) para que el snapshot
-- pueda correr antes de dbt run. La fuente raw siempre existe porque la
-- carga load.py antes de ejecutar dbt.
-- Cada vez que cambia tapi_commission o commission_type, dbt cierra el
-- registro anterior e inserta uno nuevo (SCD Type 2).

select
    external_provider_id    as provider_id,
    company_code            as biller_id,
    tapi_commission::numeric(5,4)   as tapi_commission,
    commission_type
from {{ source('raw', 'stg_providers_commission') }}
where commission_type = 'amount'

{% endsnapshot %}
