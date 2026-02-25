-- stg_providers_commission
-- Copia limpia de raw.stg_providers_commission.
-- Solo incluye registros de tipo 'amount' (único tipo válido según el challenge).
-- Materializada como view sobre la tabla raw que dbt snapshot luego versiona.

with source as (
    select * from {{ source('raw', 'stg_providers_commission') }}
),

renamed as (
    select
        external_provider_id    as provider_id,
        company_code            as biller_id,
        tapi_commission::numeric(5, 4)  as tapi_commission,
        commission_type,
        loaded_at
    from source
    where commission_type = 'amount'
)

select * from renamed
