{{ config(materialized='table') }}

select *
from {{ ref('stg_erp_produtos') }}
qualify row_number() over (partition by produto_id order by produto_id) = 1
