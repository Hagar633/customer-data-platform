# PostgreSQL Marts for Power BI

## Model scope

The project currently has customer, product, order, and order-item source data. The reporting layer is in the `mart` schema and is defined by `sql/08_create_marts.sql`.

| Object | Grain / purpose |
| --- | --- |
| `mart.dim_date` | One row per calendar date from the earliest to latest dated order |
| `mart.dim_customer` | One row per customer SCD Type 2 version |
| `mart.dim_product` | One row per product SCD Type 2 version |
| `mart.dim_order_status` | One row per order status, including `unknown` |
| `mart.dim_payment_method` | One row per payment method, including `unknown` |
| `mart.fact_sales` | One row per order item; includes gross sales, discount, and net sales |
| `mart.fact_orders` | One row per order; includes an additive order-count measure |
| `mart.daily_sales` | Daily sales totals by currency |
| `mart.customer_performance` | Customer-version performance by currency |
| `mart.product_performance` | Product-version performance by currency |
| `mart.order_operations` | Order and customer counts by date, status, and payment method |

Both dimensions retain SCD Type 2 versions, so facts point to the customer and product attributes valid at the order date. The loader resolves those surrogate keys using `order_date`; if an order has no date, it falls back to the current dimension version.

## Deploy

Run the existing schema and table scripts in their current pipeline order, through `sql/07_create_business_tables.sql`. Load and validate the data through staging, then run `src/staging/load_business.py`. Finally, run `sql/08_create_marts.sql` and `sql/09_validate_marts.sql` against the same PostgreSQL database.

The mart objects are views, so they reflect the business tables on each query and do not need a separate mart refresh job. Run the validation script after loading and after changes to the loader or model.

## Power BI relationships

Use single-direction filtering from dimensions to facts, with one-to-many cardinality:

- `mart.dim_date.date_key` → `mart.fact_sales.order_date_key`
- `mart.dim_customer.customer_sk` → `mart.fact_sales.customer_sk`
- `mart.dim_product.product_sk` → `mart.fact_sales.product_sk`
- `mart.dim_order_status.status_key` → `mart.fact_sales.status_key`
- `mart.dim_payment_method.payment_method_key` → `mart.fact_sales.payment_method_key`
- Create the equivalent date, customer, status, and payment-method relationships to `mart.fact_orders` when using order-level measures.

Do not join `fact_sales` to `fact_orders` in Power BI. Both are facts at different grains; use the shared dimensions for filtering. Use `mart.daily_sales`, `mart.customer_performance`, `mart.product_performance`, and `mart.order_operations` as pre-aggregated reporting tables when their grain matches the visual. Avoid loading both an aggregate view and its underlying fact into the same visual, as that can double-count measures.

## Measures and limits

`discount_pct` is treated as a percentage from 0 to 100, as enforced by data validation. Thus net sales are `quantity * unit_price * (1 - discount_pct / 100)`, with missing discounts treated as zero. `unit_price` is the transaction line price; product catalog price is descriptive and should not replace it in sales calculations.

Product currency is used to label line sales. There is no exchange-rate data, so monetary totals must be filtered or grouped by `currency_code`; do not add different currencies together. Returns, shipping, payment amounts, inventory snapshots, and profit cannot be modeled from the current source tables. `stock_quantity` is a product attribute, not a historical inventory fact.

Connect Power BI to the `mart` schema with a read-only database account. Import mode with scheduled refresh is a good starting point. Power BI Service may require an on-premises data gateway if it cannot reach the PostgreSQL host directly.