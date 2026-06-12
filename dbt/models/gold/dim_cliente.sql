{{ config(materialized='table') }}

select *
from {{ ref('stg_erp_clientes') }}
qualify row_number() over (partition by cliente_id order by cliente_id) = 1
