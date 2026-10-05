INSERT INTO staging.stg_customers (
    raw_record_id,
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address,
    city,
    country,
    segment,
    birthdate,
    created_at,
    updated_at,
    source_system,
    is_valid,
    validation_reason
)
SELECT
    raw_record_id,
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    address,
    city,
    country,
    LOWER(segment),
    birthdate,
    created_at,
    updated_at,
    CASE
        WHEN LOWER(source_system) = 'crm' THEN 'CRM'
        ELSE source_system
    END,
    CASE
        WHEN customer_id IS NULL THEN FALSE
        ELSE TRUE
    END,
    CASE
        WHEN customer_id IS NULL THEN 'missing customer_id'
        ELSE NULL
    END
FROM raw.customers;