-- stg_payments
-- Copia limpia de raw.stg_payments con tipos explícitos y columnas renombradas
-- para estandarizar hacia las capas superiores.
--
-- raw.stg_payments es append-only: puede tener múltiples filas por transaction_id
-- cuando el status cambia (ej: pending → confirmed). Este modelo deduplica usando
-- ROW_NUMBER(), conservando únicamente la versión más reciente de cada transacción
-- (mayor updated_at; en caso de empate, mayor loaded_at).
--
-- Materializada como view: siempre refleja el último estado de raw.

with source as (
    select * from {{ source('raw', 'stg_payments') }}
),

-- Ante múltiples versiones del mismo transaction_id, nos quedamos con la más
-- reciente. rn = 1 es la última versión conocida.
deduped as (
    select *,
        row_number() over (
            partition by transaction_id
            order by updated_at desc nulls last, loaded_at desc
        ) as _rn
    from source
),

renamed as (
    select
        transaction_id,
        created_at                          as transaction_timestamp,
        created_at::date                    as transaction_date,
        status,
        amount::numeric(12, 2)              as amount,
        type,
        company_code,
        external_provider_id                as provider_id,
        client_id,
        external_client_id,
        loaded_at
    from deduped
    where _rn = 1
)

select * from renamed
