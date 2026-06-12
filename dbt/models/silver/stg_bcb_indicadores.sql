{{ config(materialized='view') }}

with dolar as (
    select
        parse_date('%d/%m/%Y', data)    as data,
        cast(valor as float64)          as dolar
    from `{{ env_var('GCP_PROJECT_ID') }}.bronze.bcb_dolar`
),

selic as (
    select
        parse_date('%d/%m/%Y', data)    as data,
        cast(valor as float64)          as selic
    from `{{ env_var('GCP_PROJECT_ID') }}.bronze.bcb_selic`
),

ipca as (
    select
        parse_date('%d/%m/%Y', data)    as data,
        cast(valor as float64)          as ipca
    from `{{ env_var('GCP_PROJECT_ID') }}.bronze.bcb_ipca`
),

final as (
    select
        coalesce(d.data, s.data)    as data,
        d.dolar,
        s.selic,
        i.ipca
    from dolar d
    full outer join selic s on d.data = s.data
    full outer join ipca i on coalesce(d.data, s.data) = i.data
)

select * from final
