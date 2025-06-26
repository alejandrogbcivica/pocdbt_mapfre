{{ config(
    materialized = 'incremental',
    unique_key= 'user_id',
    merge_update_columns = ['email', 'last_login_date', 'is_active','ingestion_date']
) }}

with source as (

    select *
    from {{ source('integrationtests', 'users') }}
    
    {% if is_incremental() %}
      where ingestion_date > (select max(ingestion_date) from {{ this }})
    {% endif %}

),

transformed_source as (

    select
        user_id,
        lower(trim(username)) as username,
        lower(trim(email)) as email,
        initcap(trim(first_name)) as first_name,
        initcap(trim(last_name)) as last_name,
        date_of_birth,
        trim(phone_number) as phone_number,
        initcap(trim(country)) as country,
        initcap(trim(city)) as city,
        registration_date,
        last_login_date,
        is_active,
        user_type,
        profile_picture_url,
        bio,
        ingestion_date
    from source

)

select *
from transformed_source