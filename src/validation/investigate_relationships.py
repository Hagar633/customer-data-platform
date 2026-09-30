from pathlib import Path
import pandas as pd


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


# ============================================================
# LOAD DATA
# ============================================================

def load_data():
    customers = pd.read_parquet(RAW_DATA_DIR / "customers.parquet")
    products = pd.read_parquet(RAW_DATA_DIR / "products.parquet")
    orders = pd.read_parquet(RAW_DATA_DIR / "orders.parquet")
    order_items = pd.read_parquet(RAW_DATA_DIR / "order_items.parquet")

    return customers, products, orders, order_items


# ============================================================
# HELPER
# ============================================================

def print_section(title):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)


# ============================================================
# PRIMARY KEY INVESTIGATION
# ============================================================

def investigate_primary_keys(customers, products, orders, order_items):

    print_section("PRIMARY KEY INVESTIGATION")

    datasets = {
        "customers": (customers, "customer_id"),
        "products": (products, "product_id"),
        "orders": (orders, "order_id"),
        "order_items": (order_items, "order_item_id"),
    }

    for name, (df, key) in datasets.items():

        null_count = df[key].isna().sum()
        duplicate_count = df[key].duplicated().sum()
        unique_count = df[key].nunique()

        print(f"\n{name}")
        print(f"  Rows: {len(df):,}")
        print(f"  {key} nulls: {null_count:,}")
        print(f"  {key} duplicates: {duplicate_count:,}")
        print(f"  Unique {key}: {unique_count:,}")


# ============================================================
# FOREIGN KEY / ORPHAN INVESTIGATION
# ============================================================

def investigate_relationships(
    customers,
    products,
    orders,
    order_items
):

    print_section("FOREIGN KEY / RELATIONSHIP INVESTIGATION")

    # --------------------------------------------------------
    # orders.customer_id -> customers.customer_id
    # --------------------------------------------------------

    valid_customers = set(customers["customer_id"].dropna())

    orphan_orders = orders[
        ~orders["customer_id"].isin(valid_customers)
        & orders["customer_id"].notna()
    ]

    missing_customer_id = orders["customer_id"].isna().sum()

    print("\norders -> customers")
    print(f"  Orders with missing customer_id: {missing_customer_id:,}")
    print(f"  Orders with unknown customer_id: {len(orphan_orders):,}")

    # --------------------------------------------------------
    # order_items.order_id -> orders.order_id
    # --------------------------------------------------------

    valid_orders = set(orders["order_id"].dropna())

    orphan_order_items = order_items[
        ~order_items["order_id"].isin(valid_orders)
        & order_items["order_id"].notna()
    ]

    missing_order_id = order_items["order_id"].isna().sum()

    print("\norder_items -> orders")
    print(f"  Items with missing order_id: {missing_order_id:,}")
    print(f"  Items with unknown order_id: {len(orphan_order_items):,}")

    # --------------------------------------------------------
    # order_items.product_id -> products.product_id
    # --------------------------------------------------------

    valid_products = set(products["product_id"].dropna())

    orphan_products = order_items[
        ~order_items["product_id"].isin(valid_products)
        & order_items["product_id"].notna()
    ]

    missing_product_id = order_items["product_id"].isna().sum()

    print("\norder_items -> products")
    print(f"  Items with missing product_id: {missing_product_id:,}")
    print(f"  Items with unknown product_id: {len(orphan_products):,}")


# ============================================================
# ORDER ITEM VALUE INVESTIGATION
# ============================================================

def investigate_order_items(order_items):

    print_section("ORDER ITEM VALUE INVESTIGATION")

    quantity_null = order_items["quantity"].isna().sum()
    quantity_zero_or_negative = (
        order_items["quantity"].notna()
        & (order_items["quantity"] <= 0)
    ).sum()

    unit_price_null = order_items["unit_price"].isna().sum()
    unit_price_negative = (
        order_items["unit_price"].notna()
        & (order_items["unit_price"] < 0)
    ).sum()

    discount_null = order_items["discount_pct"].isna().sum()
    discount_negative = (
        order_items["discount_pct"].notna()
        & (order_items["discount_pct"] < 0)
    ).sum()

    discount_over_100 = (
        order_items["discount_pct"].notna()
        & (order_items["discount_pct"] > 100)
    ).sum()

    print("\nquantity")
    print(f"  Missing: {quantity_null:,}")
    print(f"  Zero/negative: {quantity_zero_or_negative:,}")

    print("\nunit_price")
    print(f"  Missing: {unit_price_null:,}")
    print(f"  Negative: {unit_price_negative:,}")

    print("\ndiscount_pct")
    print(f"  Missing: {discount_null:,}")
    print(f"  Negative: {discount_negative:,}")
    print(f"  Greater than 100: {discount_over_100:,}")


# ============================================================
# ORDER INVESTIGATION
# ============================================================

def investigate_orders(orders):

    print_section("ORDER INVESTIGATION")

    print(f"\nDuplicate complete rows: {orders.duplicated().sum():,}")
    print(f"Duplicate order IDs: {orders['order_id'].duplicated().sum():,}")

    print("\nMissing values:")
    missing = orders.isna().sum()
    print(missing[missing > 0])

    print("\nStatus values:")
    print(orders["status"].value_counts(dropna=False))

    print("\nPayment method values:")
    print(orders["payment_method"].value_counts(dropna=False))

    print("\nOrder date parsing:")
    parsed_dates = pd.to_datetime(
        orders["order_date"],
        errors="coerce"
    )

    invalid_dates = (
        orders["order_date"].notna()
        & parsed_dates.isna()
    ).sum()

    print(f"  Invalid dates: {invalid_dates:,}")

    print("\nUpdated_at:")
    print(f"  Missing: {orders['updated_at'].isna().sum():,}")


# ============================================================
# CUSTOMER INVESTIGATION
# ============================================================

def investigate_customers(customers):

    print_section("CUSTOMER INVESTIGATION")

    print("\nMissing values:")
    missing = customers.isna().sum()
    print(missing[missing > 0])

    print("\nSegment values:")
    print(customers["segment"].value_counts(dropna=False))

    print("\nSource system values:")
    print(customers["source_system"].value_counts(dropna=False))

    print("\nEmail values:")
    print(f"  Missing emails: {customers['email'].isna().sum():,}")

    print("\nCustomer IDs:")
    print(f"  Missing: {customers['customer_id'].isna().sum():,}")
    print(
        f"  Duplicates: "
        f"{customers['customer_id'].duplicated().sum():,}"
    )


# ============================================================
# PRODUCT INVESTIGATION
# ============================================================

def investigate_products(products):

    print_section("PRODUCT INVESTIGATION")

    print("\nMissing values:")
    missing = products.isna().sum()
    print(missing[missing > 0])

    print("\nCategory values:")
    print(products["category"].value_counts(dropna=False))

    print("\nCurrency values:")
    print(products["currency"].value_counts(dropna=False))

    price_negative = (
        products["price"].notna()
        & (products["price"] < 0)
    ).sum()

    stock_negative = (
        products["stock_quantity"].notna()
        & (products["stock_quantity"] < 0)
    ).sum()

    print("\nPrice:")
    print(f"  Missing: {products['price'].isna().sum():,}")
    print(f"  Negative: {price_negative:,}")

    print("\nStock quantity:")
    print(f"  Missing: {products['stock_quantity'].isna().sum():,}")
    print(f"  Negative: {stock_negative:,}")


# ============================================================
# MAIN
# ============================================================

def main():

    print("Loading raw Parquet files...")

    customers, products, orders, order_items = load_data()

    print("Data loaded successfully.")

    investigate_primary_keys(
        customers,
        products,
        orders,
        order_items
    )

    investigate_relationships(
        customers,
        products,
        orders,
        order_items
    )

    investigate_order_items(order_items)

    investigate_orders(orders)

    investigate_customers(customers)

    investigate_products(products)

    print_section("INVESTIGATION COMPLETE")


if __name__ == "__main__":
    main()