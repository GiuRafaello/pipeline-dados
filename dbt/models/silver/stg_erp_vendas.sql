{{ config(materialized='view') }}

with source as (
    select * from `{{ env_var('GCP_PROJECT_ID') }}.bronze.erp_vendas`
),

deduped as (
    select *,
        row_number() over (partition by id order by _loaded_at desc) as rn
    from source
),

final as (
    select
        id                                          as venda_id,
        cliente_id,
        loja_id,
        cast(data_venda as timestamp)               as data_venda,
        date(cast(data_venda as timestamp))         as data_venda_date,
        status,
        cast(total as float64)                      as total,
        cast(updated_at as timestamp)               as updated_at
    from deduped
    where rn = 1
      and status = 'concluida'
)

select * from final
