{{ config(materialized='view') }}

with source as (
    select * from `{{ env_var('GCP_PROJECT_ID') }}.bronze.erp_clientes`
),

deduped as (
    select *,
        row_number() over (partition by id order by _loaded_at desc) as rn
    from source
),

final as (
    select
        id          as cliente_id,
        nome        as cliente_nome,
        email,
        cidade,
        uf
    from deduped
    where rn = 1
)

select * from final
