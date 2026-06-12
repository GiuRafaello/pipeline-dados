{{ config(materialized='table') }}

with vendas as (
    select * from {{ ref('stg_erp_vendas') }}
),

itens as (
    select
        venda_id,
        sum(subtotal)       as total_itens,
        sum(quantidade)     as total_quantidade,
        count(*)            as qtd_itens
    from {{ ref('stg_erp_itens_venda') }}
    group by venda_id
),

final as (
    select
        v.venda_id,
        v.cliente_id,
        v.loja_id,
        v.data_venda_date                           as data,
        extract(year  from v.data_venda_date)       as ano,
        extract(month from v.data_venda_date)       as mes,
        v.status,
        v.total,
        i.total_itens,
        i.total_quantidade,
        i.qtd_itens
    from vendas v
    left join itens i on v.venda_id = i.venda_id
)

select * from final
