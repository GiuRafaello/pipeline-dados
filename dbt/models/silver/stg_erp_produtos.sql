{{ config(materialized='view') }}

with source as (
    select * from `{{ env_var('GCP_PROJECT_ID') }}.bronze.erp_produtos`
),

final as (
    select
        id                          as produto_id,
        sku,
        nome                        as produto_nome,
        categoria,
        cast(custo as float64)      as custo,
        cast(preco as float64)      as preco
    from source
)

select * from final
