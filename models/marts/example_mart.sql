{{ config(
    materialized = 'table'
) }}

with stg_users as (
    select *
    from {{ ref('stg_integrationtests__users') }}
)

select
    user_id,
    username,
    email,
    country,
    city,
    is_active,
    ingestion_date
from stg_users
where is_active = true