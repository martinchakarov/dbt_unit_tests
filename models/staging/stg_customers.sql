with customers as (
    select
        id::int as id,
        nullif(trim(first_name::string), '') as first_name,
        nullif(trim(last_name::string), '') as last_name,
        email::string as email,
        gender::string as gender,
        ip_address::string as ip_address,
        region::string as region
    from {{ ref('customers') }}
),

final as (
    select
        id as customer_id,
        case 
            when first_name is null and last_name is null
            then null
            when first_name is not null and last_name is not null
            then first_name || ' ' || last_name
            when first_name is null and last_name is not null
            then last_name
            when first_name is not null and last_name is null 
            then first_name
            else null
        end as customer_name,
        email as customer_email,
        case 
            when contains(email, '@') then split(email, '@')[array_size(split(email, '@'))-1]::string 
            else null 
        end as email_domain,
        case
            when gender not ilike 'male' and gender not ilike 'female'
            then 'Other'
            else gender
        end as customer_gender,
        ip_address as ip_address,
        region as customer_region
    from customers
)

select * from final