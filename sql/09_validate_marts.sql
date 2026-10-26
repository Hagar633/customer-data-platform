-- ============================================================
-- MART INTEGRITY CHECKS
-- Each query should return zero rows. Any returned rows identify a defect.
-- Run after sql/08_create_marts.sql and after loading business data.
-- ============================================================

-- Duplicate order-item grain in the sales fact.
SELECT order_item_sk, COUNT(*) AS row_count
FROM mart.fact_sales
GROUP BY order_item_sk
HAVING COUNT(*) <> 1;


-- Sales facts without a matching customer dimension version.
SELECT sales.order_item_id, sales.customer_sk
FROM mart.fact_sales AS sales
LEFT JOIN mart.dim_customer AS customer
    ON customer.customer_sk = sales.customer_sk
WHERE sales.customer_sk IS NULL
   OR customer.customer_sk IS NULL;


-- Sales facts without a matching product dimension version.
SELECT sales.order_item_id, sales.product_sk
FROM mart.fact_sales AS sales
LEFT JOIN mart.dim_product AS product
    ON product.product_sk = sales.product_sk
WHERE sales.product_sk IS NULL
   OR product.product_sk IS NULL;


-- Facts whose date key is missing from the date dimension.
SELECT sales.order_item_id, sales.order_date_key
FROM mart.fact_sales AS sales
LEFT JOIN mart.dim_date AS calendar
    ON calendar.date_key = sales.order_date_key
WHERE sales.order_date_key IS NULL
   OR calendar.date_key IS NULL;


-- Facts whose dimension version does not cover the order timestamp.
SELECT
    sales.order_item_id,
    sales.order_date,
    customer.valid_from AS customer_valid_from,
    customer.valid_to AS customer_valid_to,
    product.valid_from AS product_valid_from,
    product.valid_to AS product_valid_to
FROM mart.fact_sales AS sales
JOIN mart.dim_customer AS customer
    ON customer.customer_sk = sales.customer_sk
JOIN mart.dim_product AS product
    ON product.product_sk = sales.product_sk
WHERE sales.order_date IS NOT NULL
  AND (
      sales.order_date < customer.valid_from
      OR (customer.valid_to IS NOT NULL AND sales.order_date >= customer.valid_to)
      OR sales.order_date < product.valid_from
      OR (product.valid_to IS NOT NULL AND sales.order_date >= product.valid_to)
  );


-- Sales and order fact row counts should match their source fact grains.
SELECT 'fact_sales' AS mart_object,
       (SELECT COUNT(*) FROM mart.fact_sales) AS mart_rows,
       (SELECT COUNT(*) FROM business.fact_order_item) AS source_rows
WHERE (SELECT COUNT(*) FROM mart.fact_sales)
   <> (SELECT COUNT(*) FROM business.fact_order_item)
UNION ALL
SELECT 'fact_orders',
       (SELECT COUNT(*) FROM mart.fact_orders),
       (SELECT COUNT(*) FROM business.fact_order)
WHERE (SELECT COUNT(*) FROM mart.fact_orders)
   <> (SELECT COUNT(*) FROM business.fact_order);