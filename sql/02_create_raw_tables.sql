
-- ============================================================
-- RAW CUSTOMERS
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.customers (
    raw_record_id BIGSERIAL PRIMARY KEY,
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
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- RAW PRODUCTS
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.products (
    raw_record_id BIGSERIAL PRIMARY KEY,
    product_id TEXT,
    product_name TEXT,
    category TEXT,
    price NUMERIC(12, 2),
    currency TEXT,
    stock_quantity NUMERIC,
    supplier TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- RAW ORDERS
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.orders (
    raw_record_id BIGSERIAL PRIMARY KEY,
    order_id TEXT,
    customer_id TEXT,
    order_date TIMESTAMP,
    status TEXT,
    payment_method TEXT,
    updated_at TIMESTAMP,
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- RAW ORDER ITEMS
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.order_items (
    raw_record_id BIGSERIAL PRIMARY KEY,
    order_item_id TEXT,
    order_id TEXT,
    product_id TEXT,
    quantity NUMERIC,
    unit_price NUMERIC(12, 2),
    discount_pct NUMERIC(5, 2),
    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);


-- ============================================================
-- INDEXES ON BUSINESS KEYS
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_raw_customers_customer_id
    ON raw.customers(customer_id);

CREATE INDEX IF NOT EXISTS idx_raw_products_product_id
    ON raw.products(product_id);

CREATE INDEX IF NOT EXISTS idx_raw_orders_order_id
    ON raw.orders(order_id);

CREATE INDEX IF NOT EXISTS idx_raw_orders_customer_id
    ON raw.orders(customer_id);

CREATE INDEX IF NOT EXISTS idx_raw_order_items_order_item_id
    ON raw.order_items(order_item_id);

CREATE INDEX IF NOT EXISTS idx_raw_order_items_order_id
    ON raw.order_items(order_id);

CREATE INDEX IF NOT EXISTS idx_raw_order_items_product_id
    ON raw.order_items(product_id);

