{{ config(materialized='table') }}

select * from {{ ref('stg_erp_lojas') }}
