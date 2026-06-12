{{ config(materialized='table') }}

with vendas_mes as (
    select
        ano,
        mes,
        loja_id,
        sum(total) as total_vendas
    from {{ ref('fct_vendas') }}
    group by ano, mes, loja_id
),

metas as (
    select
        ano,
        mes,
        loja_id,
        sum(meta_valor) as meta_total
    from {{ ref('stg_drive_metas') }}
    group by ano, mes, loja_id
),

final as (
    select
        coalesce(v.ano, m.ano)          as ano,
        coalesce(v.mes, m.mes)          as mes,
        coalesce(v.loja_id, m.loja_id)  as loja_id,
        v.total_vendas,
        m.meta_total,
        round(safe_divide(v.total_vendas, m.meta_total) * 100, 2) as pct_meta
    from vendas_mes v
    full outer join metas m
        on v.ano = m.ano and v.mes = m.mes and v.loja_id = m.loja_id
)

select * from final
