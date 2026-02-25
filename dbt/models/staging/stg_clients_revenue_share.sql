-- stg_clients_revenue_share
-- Copia limpia de raw.stg_clients_revenue_share.
-- Solo incluye registros de tipo 'revenue_share' (único tipo válido según el challenge).
-- Materializada como view sobre la tabla raw que dbt snapshot luego versiona.

with source as (
    select * from {{ source('raw', 'stg_clients_revenue_share') }}
),

renamed as (
    select
        client_id,
        revenue_share::numeric(5, 4)    as revenue_share,
        commission_type,
        loaded_at
    from source
    where commission_type = 'revenue_share'
)

select * from renamed
