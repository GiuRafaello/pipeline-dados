{{ config(materialized='view') }}

with source as (
    select * from `{{ env_var('GCP_PROJECT_ID') }}.bronze.drive_metas_vendas`
),

final as (
    select
        cast(ano as int64)              as ano,
        cast(mes as int64)              as mes,
        cast(loja_id as int64)          as loja_id,
        categoria,
        cast(meta_valor as float64)     as meta_valor,
        date(cast(ano as int64), cast(mes as int64), 1) as data_mes
    from source
)

select * from final
