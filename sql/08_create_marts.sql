-- ============================================================
-- POWER BI MARTS
-- Run after sql/07_create_business_tables.sql and load_business.py.
-- Fact grains:
--   mart.fact_sales: one row per business.fact_order_item row
--   mart.fact_orders: one row per business.fact_order row
-- ============================================================

CREATE SCHEMA IF NOT EXISTS mart;


-- ============================================================
-- CONFORMED DIMENSIONS
-- ============================================================

CREATE OR REPLACE VIEW mart.dim_date AS
WITH bounds AS (
    SELECT
        MIN(order_date)::date AS first_date,
        MAX(order_date)::date AS last_date
    FROM business.fact_order
    WHERE order_date IS NOT NULL
), calendar AS (
    SELECT generated_date::date AS calendar_date
    FROM bounds
    CROSS JOIN LATERAL generate_series(
        first_date::timestamp,
        last_date::timestamp,
        INTERVAL '1 day'
    ) AS generated_date
    WHERE first_date IS NOT NULL
)
SELECT
    TO_CHAR(calendar_date, 'YYYYMMDD')::integer AS date_key,
    calendar_date,
    EXTRACT(DAY FROM calendar_date)::integer AS day_of_month,
    EXTRACT(DOW FROM calendar_date)::integer AS day_of_week,
    TO_CHAR(calendar_date, 'FMDay') AS day_name,
    EXTRACT(ISODOW FROM calendar_date)::integer AS iso_day_of_week,
    EXTRACT(WEEK FROM calendar_date)::integer AS week_of_year,
    EXTRACT(MONTH FROM calendar_date)::integer AS month_number,
    TO_CHAR(calendar_date, 'FMMonth') AS month_name,
    TO_CHAR(calendar_date, 'Mon') AS month_short_name,
    EXTRACT(QUARTER FROM calendar_date)::integer AS quarter_number,
    'Q' || EXTRACT(QUARTER FROM calendar_date)::integer::text AS quarter_name,
    EXTRACT(YEAR FROM calendar_date)::integer AS year_number,
    (EXTRACT(ISODOW FROM calendar_date) IN (6, 7)) AS is_weekend
FROM calendar;


CREATE OR REPLACE VIEW mart.dim_customer AS
SELECT
    customer_sk,
    customer_id,
    first_name,
    last_name,
    NULLIF(BTRIM(CONCAT_WS(' ', first_name, last_name)), '') AS full_name,
    email,
    phone,
    address,
    city,
    country,
    segment,
    birthdate,
    source_system,
    valid_from,
    valid_to,
    is_current
FROM business.dim_customer;


CREATE OR REPLACE VIEW mart.dim_product AS
SELECT
    product_sk,
    product_id,
    product_name,
    category,
    price AS catalog_price,
    currency AS currency_code,
    stock_quantity,
    supplier,
    valid_from,
    valid_to,
    is_current
FROM business.dim_product;


CREATE OR REPLACE VIEW mart.dim_order_status AS
SELECT
    status_key,
    INITCAP(status_key) AS status_name
FROM (
    SELECT DISTINCT COALESCE(NULLIF(BTRIM(status), ''), 'unknown') AS status_key
    FROM business.fact_order
    UNION
    SELECT 'unknown'
) AS statuses;


CREATE OR REPLACE VIEW mart.dim_payment_method AS
SELECT
    payment_method_key,
    INITCAP(payment_method_key) AS payment_method_name
FROM (
    SELECT DISTINCT
        COALESCE(NULLIF(BTRIM(payment_method), ''), 'unknown') AS payment_method_key
    FROM business.fact_order
    UNION
    SELECT 'unknown'
) AS payment_methods;


-- ============================================================
-- FACTS
-- ============================================================

CREATE OR REPLACE VIEW mart.fact_sales AS
SELECT
    item.order_item_sk,
    item.order_item_id,
    orders.order_sk,
    orders.order_id,
    TO_CHAR(orders.order_date::date, 'YYYYMMDD')::integer AS order_date_key,
    orders.customer_sk,
    item.product_sk,
    COALESCE(NULLIF(BTRIM(orders.status), ''), 'unknown') AS status_key,
    COALESCE(NULLIF(BTRIM(orders.payment_method), ''), 'unknown')
        AS payment_method_key,
    product.currency_code,
    item.quantity,
    item.unit_price,
    item.discount_pct,
    item.quantity * item.unit_price AS gross_sales,
    item.quantity * item.unit_price
        * COALESCE(item.discount_pct, 0) / 100.0 AS discount_amount,
    item.quantity * item.unit_price
        * (1 - COALESCE(item.discount_pct, 0) / 100.0) AS net_sales,
    orders.order_date,
    orders.status AS order_status,
    orders.payment_method
FROM business.fact_order_item AS item
JOIN business.fact_order AS orders
    ON orders.order_sk = item.order_sk
LEFT JOIN mart.dim_product AS product
    ON product.product_sk = item.product_sk;


CREATE OR REPLACE VIEW mart.fact_orders AS
SELECT
    orders.order_sk,
    orders.order_id,
    TO_CHAR(orders.order_date::date, 'YYYYMMDD')::integer AS order_date_key,
    orders.customer_sk,
    COALESCE(NULLIF(BTRIM(orders.status), ''), 'unknown') AS status_key,
    COALESCE(NULLIF(BTRIM(orders.payment_method), ''), 'unknown')
        AS payment_method_key,
    1::integer AS order_count,
    orders.order_date,
    orders.status AS order_status,
    orders.payment_method,
    orders.source_updated_at
FROM business.fact_order AS orders;


-- ============================================================
-- ANALYTIC MARTS
-- Monetary totals are split by currency; no exchange-rate data exists.
-- ============================================================

CREATE OR REPLACE VIEW mart.daily_sales AS
SELECT
    order_date_key AS date_key,
    currency_code,
    COUNT(DISTINCT order_sk) AS order_count,
    COUNT(*) AS order_line_count,
    SUM(quantity) AS units_sold,
    SUM(gross_sales) AS gross_sales,
    SUM(discount_amount) AS discount_amount,
    SUM(net_sales) AS net_sales
FROM mart.fact_sales
GROUP BY order_date_key, currency_code;


CREATE OR REPLACE VIEW mart.customer_performance AS
SELECT
    customer.customer_sk,
    customer.customer_id,
    customer.full_name,
    customer.segment,
    customer.city,
    customer.country,
    sales.currency_code,
    COUNT(DISTINCT sales.order_sk) AS order_count,
    COUNT(*) AS order_line_count,
    SUM(sales.net_sales) AS net_sales,
    MIN(sales.order_date) AS first_order_date,
    MAX(sales.order_date) AS most_recent_order_date
FROM mart.fact_sales AS sales
JOIN mart.dim_customer AS customer
    ON customer.customer_sk = sales.customer_sk
GROUP BY
    customer.customer_sk,
    customer.customer_id,
    customer.full_name,
    customer.segment,
    customer.city,
    customer.country,
    sales.currency_code;


CREATE OR REPLACE VIEW mart.product_performance AS
SELECT
    product.product_sk,
    product.product_id,
    product.product_name,
    product.category,
    product.supplier,
    sales.currency_code,
    COUNT(DISTINCT sales.order_sk) AS order_count,
    COUNT(*) AS order_line_count,
    SUM(sales.quantity) AS units_sold,
    SUM(sales.gross_sales) AS gross_sales,
    SUM(sales.discount_amount) AS discount_amount,
    SUM(sales.net_sales) AS net_sales
FROM mart.fact_sales AS sales
JOIN mart.dim_product AS product
    ON product.product_sk = sales.product_sk
GROUP BY
    product.product_sk,
    product.product_id,
    product.product_name,
    product.category,
    product.supplier,
    sales.currency_code;


CREATE OR REPLACE VIEW mart.order_operations AS
SELECT
    orders.order_date_key AS date_key,
    orders.status_key,
    orders.payment_method_key,
    COUNT(*) AS order_count,
    COUNT(DISTINCT orders.customer_sk) AS customer_count
FROM mart.fact_orders AS orders
GROUP BY
    orders.order_date_key,
    orders.status_key,
    orders.payment_method_key;