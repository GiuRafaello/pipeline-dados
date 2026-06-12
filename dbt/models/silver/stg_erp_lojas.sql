{{ config(materialized='view') }}

with source as (
    select * from `{{ env_var('GCP_PROJECT_ID') }}.bronze.erp_lojas`
),

final as (
    select
        id          as loja_id,
        nome        as loja_nome,
        cidade,
        uf,
        regiao
    from source
)

select * from final
