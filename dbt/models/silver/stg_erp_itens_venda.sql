{{ config(materialized='view') }}

with source as (
    select * from `{{ env_var('GCP_PROJECT_ID') }}.bronze.erp_itens_venda`
),

deduped as (
    select *,
        row_number() over (partition by id order by _loaded_at desc) as rn
    from source
),

final as (
    select
        id                              as item_id,
        venda_id,
        produto_id,
        cast(quantidade as int64)       as quantidade,
        cast(preco_unit as float64)     as preco_unit,
        cast(subtotal as float64)       as subtotal
    from deduped
    where rn = 1
)

select * from final
