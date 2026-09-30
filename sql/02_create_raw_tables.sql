-- sql/02_create_raw_tables.sql

-- ============================================================
-- RAW CUSTOMERS
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id TEXT PRIMARY KEY,
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
    source_system TEXT
);


-- ============================================================
-- RAW PRODUCTS
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    price NUMERIC(12, 2),
    currency TEXT,
    stock_quantity NUMERIC,
    supplier TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);


-- ============================================================
-- RAW ORDERS
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT,
    order_date TIMESTAMP,
    status TEXT,
    payment_method TEXT,
    updated_at TIMESTAMP
);


-- ============================================================
-- RAW ORDER ITEMS
-- ============================================================

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_item_id TEXT PRIMARY KEY,
    order_id TEXT,
    product_id TEXT,
    quantity NUMERIC,
    unit_price NUMERIC(12, 2),
    discount_pct NUMERIC(5, 2)
);