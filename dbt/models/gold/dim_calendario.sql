{{ config(materialized='table') }}

with datas as (
    select date_add('2023-01-01', interval n day) as data
    from unnest(generate_array(0, 1095)) as n
),

final as (
    select
        data,
        extract(year  from data)    as ano,
        extract(month from data)    as mes,
        extract(day   from data)    as dia,
        extract(week  from data)    as semana,
        extract(quarter from data)  as trimestre,
        format_date('%B', data)     as mes_nome,
        format_date('%A', data)     as dia_semana,
        case when extract(dayofweek from data) in (1,7) then true else false end as fim_de_semana
    from datas
)

select * from final
