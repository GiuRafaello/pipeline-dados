{{ config(materialized='table') }}

select *
from {{ ref('stg_erp_lojas') }}
qualify row_number() over (partition by loja_id order by loja_id) = 1
