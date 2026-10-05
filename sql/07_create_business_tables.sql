-- ============================================================
-- BUSINESS LAYER TABLES
-- ============================================================
-- Customer:
--   SCD Type 2 -> segment, address
--   Type 1     -> other descriptive attributes
--
-- Product:
--   SCD Type 2 -> price, category
--   Type 1     -> other descriptive attributes
--
-- ============================================================

CREATE TABLE IF NOT EXISTS business.dim_customer (
    customer_sk BIGSERIAL PRIMARY KEY,

    customer_id TEXT NOT NULL,

    first_name TEXT,
    last_name TEXT,
    email TEXT,
    phone TEXT,
    address TEXT,
    city TEXT,
    country TEXT,
    segment TEXT,
    birthdate TIMESTAMP,
    source_system TEXT,

    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,

    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMP,
    updated_at TIMESTAMP,

    CONSTRAINT uq_dim_customer_version
        UNIQUE (customer_id, valid_from)
);

CREATE INDEX IF NOT EXISTS idx_dim_customer_customer_id
    ON business.dim_customer(customer_id);

CREATE INDEX IF NOT EXISTS idx_dim_customer_current
    ON business.dim_customer(customer_id, is_current);


CREATE TABLE IF NOT EXISTS business.dim_product (
    product_sk BIGSERIAL PRIMARY KEY,

    product_id TEXT NOT NULL,

    product_name TEXT,
    category TEXT,
    price NUMERIC(12, 2),
    currency TEXT,
    stock_quantity NUMERIC,
    supplier TEXT,

    valid_from TIMESTAMP NOT NULL,
    valid_to TIMESTAMP,

    is_current BOOLEAN NOT NULL DEFAULT TRUE,

    created_at TIMESTAMP,
    updated_at TIMESTAMP,

    CONSTRAINT uq_dim_product_version
        UNIQUE (product_id, valid_from)
);

CREATE INDEX IF NOT EXISTS idx_dim_product_product_id
    ON business.dim_product(product_id);

CREATE INDEX IF NOT EXISTS idx_dim_product_current
    ON business.dim_product(product_id, is_current);


CREATE TABLE IF NOT EXISTS business.fact_order (
    order_sk BIGSERIAL PRIMARY KEY,

    order_id TEXT NOT NULL,

    customer_sk BIGINT,

    order_date TIMESTAMP,
    status TEXT,
    payment_method TEXT,
    source_updated_at TIMESTAMP,

    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_fact_order_customer
        FOREIGN KEY (customer_sk)
        REFERENCES business.dim_customer(customer_sk),

    CONSTRAINT uq_fact_order
        UNIQUE (order_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_order_order_id
    ON business.fact_order(order_id);

CREATE INDEX IF NOT EXISTS idx_fact_order_customer_sk
    ON business.fact_order(customer_sk);

CREATE INDEX IF NOT EXISTS idx_fact_order_order_date
    ON business.fact_order(order_date);


CREATE TABLE IF NOT EXISTS business.fact_order_item (
    order_item_sk BIGSERIAL PRIMARY KEY,

    order_item_id TEXT NOT NULL,

    order_sk BIGINT NOT NULL,

    product_sk BIGINT,

    quantity NUMERIC,
    unit_price NUMERIC(12, 2),
    discount_pct NUMERIC(5, 2),

    loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_fact_order_item_order
        FOREIGN KEY (order_sk)
        REFERENCES business.fact_order(order_sk),

    CONSTRAINT fk_fact_order_item_product
        FOREIGN KEY (product_sk)
        REFERENCES business.dim_product(product_sk),

    CONSTRAINT uq_fact_order_item
        UNIQUE (order_item_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_order_item_order_sk
    ON business.fact_order_item(order_sk);

CREATE INDEX IF NOT EXISTS idx_fact_order_item_product_sk
    ON business.fact_order_item(product_sk);

CREATE INDEX IF NOT EXISTS idx_fact_order_item_order_item_id
    ON business.fact_order_item(order_item_id);