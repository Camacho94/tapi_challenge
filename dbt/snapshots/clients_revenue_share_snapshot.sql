{% snapshot clients_revenue_share_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='client_id',
        strategy='check',
        check_cols=['revenue_share', 'commission_type'],
    )
}}

-- Lee directamente desde raw (no desde la vista staging) para que el snapshot
-- pueda correr antes de dbt run. La fuente raw siempre existe porque la
-- carga load.py antes de ejecutar dbt.
-- Cada vez que cambia revenue_share, dbt cierra el registro anterior
-- e inserta uno nuevo (SCD Type 2).

select
    client_id,
    revenue_share::numeric(5,4),
    commission_type
from {{ source('raw', 'stg_clients_revenue_share') }}
where commission_type = 'revenue_share'

{% endsnapshot %}
