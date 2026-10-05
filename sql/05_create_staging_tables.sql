-- ============================================================
-- STAGING CUSTOMERS
-- ============================================================

CREATE TABLE IF NOT EXISTS staging.stg_customers (
    staging_record_id BIGSERIAL PRIMARY KEY,

    raw_record_id BIGINT NOT NULL,
    customer_id TEXT,

    first_name TEXT,
    last_name TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    country TEXT,
    segment TEXT,

    birthdate TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    source_system TEXT,

    is_valid BOOLEAN NOT NULL DEFAULT TRUE,
    validation_reason TEXT,

    staged_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- STAGING PRODUCTS
-- ============================================================

CREATE TABLE IF NOT EXISTS staging.stg_products (
    staging_record_id BIGSERIAL PRIMARY KEY,

    raw_record_id BIGINT NOT NULL,
    product_id TEXT,

    product_name TEXT,
    category TEXT,
    price NUMERIC(12, 2),
    currency TEXT,
    stock_quantity NUMERIC,
    supplier TEXT,

    created_at TIMESTAMP,
    updated_at TIMESTAMP,

    is_valid BOOLEAN NOT NULL DEFAULT TRUE,
    validation_reason TEXT,

    staged_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- STAGING ORDERS
-- ============================================================

CREATE TABLE IF NOT EXISTS staging.stg_orders (
    staging_record_id BIGSERIAL PRIMARY KEY,

    raw_record_id BIGINT NOT NULL,
    order_id TEXT,
    customer_id TEXT,

    order_date TIMESTAMP,
    status TEXT,
    payment_method TEXT,
    updated_at TIMESTAMP,

    is_valid BOOLEAN NOT NULL DEFAULT TRUE,
    validation_reason TEXT,

    staged_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- STAGING ORDER ITEMS
-- ============================================================

CREATE TABLE IF NOT EXISTS staging.stg_order_items (
    staging_record_id BIGSERIAL PRIMARY KEY,

    raw_record_id BIGINT NOT NULL,
    order_item_id TEXT,
    order_id TEXT,
    product_id TEXT,

    quantity NUMERIC,
    unit_price NUMERIC(12, 2),
    discount_pct NUMERIC(5, 2),

    is_valid BOOLEAN NOT NULL DEFAULT TRUE,
    validation_reason TEXT,

    staged_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- INDEXES
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_stg_customers_customer_id
    ON staging.stg_customers(customer_id);

CREATE INDEX IF NOT EXISTS idx_stg_customers_raw_record_id
    ON staging.stg_customers(raw_record_id);

CREATE INDEX IF NOT EXISTS idx_stg_products_product_id
    ON staging.stg_products(product_id);

CREATE INDEX IF NOT EXISTS idx_stg_products_raw_record_id
    ON staging.stg_products(raw_record_id);

CREATE INDEX IF NOT EXISTS idx_stg_orders_order_id
    ON staging.stg_orders(order_id);

CREATE INDEX IF NOT EXISTS idx_stg_orders_customer_id
    ON staging.stg_orders(customer_id);

CREATE INDEX IF NOT EXISTS idx_stg_orders_raw_record_id
    ON staging.stg_orders(raw_record_id);

CREATE INDEX IF NOT EXISTS idx_stg_order_items_order_item_id
    ON staging.stg_order_items(order_item_id);

CREATE INDEX IF NOT EXISTS idx_stg_order_items_order_id
    ON staging.stg_order_items(order_id);

CREATE INDEX IF NOT EXISTS idx_stg_order_items_product_id
    ON staging.stg_order_items(product_id);

CREATE INDEX IF NOT EXISTS idx_stg_order_items_raw_record_id
    ON staging.stg_order_items(raw_record_id);