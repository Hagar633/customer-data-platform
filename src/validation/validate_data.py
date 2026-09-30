from pathlib import Path
import pandas as pd


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
CLEAN_DATA_DIR = PROJECT_ROOT / "data" / "clean"


# ============================================================
# SETUP
# ============================================================

def setup_directories():
    CLEAN_DATA_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# CUSTOMERS
# ============================================================

def clean_customers(df):
    df = df.copy()

    # Normalize source system
    df["source_system"] = df["source_system"].apply(
        lambda x: "CRM" if isinstance(x, str) and x.strip().lower() == "crm"
        else x.strip() if isinstance(x, str)
        else x
    )

    # Normalize segment without filling NULLs
    df["segment"] = df["segment"].apply(
        lambda x: x.strip().lower()
        if isinstance(x, str)
        else x
    )

    # Validation flags
    df["is_valid"] = True
    df["validation_reason"] = None

    # Missing customer ID
    mask = df["customer_id"].isna()

    df.loc[mask, "is_valid"] = False
    df.loc[mask, "validation_reason"] = "missing_customer_id"

    return df


# ============================================================
# PRODUCTS
# ============================================================

def clean_products(df):
    df = df.copy()

    # Normalize category without filling NULLs
    df["category"] = df["category"].apply(
        lambda x: x.strip().lower()
        if isinstance(x, str)
        else x
    )

    # Normalize currency without filling NULLs
    df["currency"] = df["currency"].apply(
        lambda x: x.strip().upper()
        if isinstance(x, str)
        else x
    )

    # Validation flags
    df["is_valid"] = True
    df["validation_reason"] = None

    # Missing product ID
    mask = df["product_id"].isna()

    df.loc[mask, "is_valid"] = False
    df.loc[mask, "validation_reason"] = "missing_product_id"

    # Invalid price
    mask = df["price"].notna() & (df["price"] < 0)

    df.loc[mask, "is_valid"] = False
    df.loc[mask, "validation_reason"] = "negative_price"

    # Invalid stock
    mask = (
        df["stock_quantity"].notna()
        & (df["stock_quantity"] < 0)
    )

    df.loc[mask, "is_valid"] = False
    df.loc[mask, "validation_reason"] = "negative_stock_quantity"

    return df


# ============================================================
# ORDERS
# ============================================================

def clean_orders(df, customers):
    df = df.copy()

    # Normalize status
    # NULL status becomes "unknown" as requested.
    df["status"] = df["status"].apply(
        lambda x: x.strip().lower()
        if isinstance(x, str)
        else "unknown"
    )

    # Normalize payment method without filling NULLs
    df["payment_method"] = df["payment_method"].apply(
        lambda x: x.strip().lower()
        if isinstance(x, str)
        else x
    )

    # Validation flags
    df["is_valid"] = True
    df["validation_reason"] = None

    # Missing order ID
    mask = df["order_id"].isna()

    df.loc[mask, "is_valid"] = False
    df.loc[mask, "validation_reason"] = "missing_order_id"

    # Check customer relationship
    valid_customer_ids = set(
        customers["customer_id"].dropna()
    )

    mask = (
        df["customer_id"].notna()
        & ~df["customer_id"].isin(valid_customer_ids)
    )

    df.loc[mask, "is_valid"] = False
    df.loc[mask, "validation_reason"] = "unknown_customer"

    return df


# ============================================================
# ORDER ITEMS
# ============================================================

def clean_order_items(df, orders, products):
    df = df.copy()

    # Validation flags
    df["is_valid"] = True
    df["validation_reason"] = None

    # Missing order item ID
    mask = df["order_item_id"].isna()

    df.loc[mask, "is_valid"] = False
    df.loc[mask, "validation_reason"] = "missing_order_item_id"

    # Missing quantity
    mask = df["quantity"].isna()

    df.loc[mask, "is_valid"] = False
    df.loc[mask, "validation_reason"] = "missing_quantity"

    # Invalid quantity
    mask = (
        df["quantity"].notna()
        & (df["quantity"] <= 0)
    )

    df.loc[mask, "is_valid"] = False
    df.loc[mask, "validation_reason"] = "invalid_quantity"

    # Invalid unit price
    mask = (
        df["unit_price"].notna()
        & (df["unit_price"] < 0)
    )

    df.loc[mask, "is_valid"] = False
    df.loc[mask, "validation_reason"] = "negative_unit_price"

    # Invalid discount
    mask = (
        df["discount_pct"].notna()
        & (
            (df["discount_pct"] < 0)
            | (df["discount_pct"] > 100)
        )
    )

    df.loc[mask, "is_valid"] = False
    df.loc[mask, "validation_reason"] = "invalid_discount_pct"

    # Check order relationship
    valid_order_ids = set(
        orders["order_id"].dropna()
    )

    mask = (
        df["order_id"].notna()
        & ~df["order_id"].isin(valid_order_ids)
    )

    df.loc[mask, "is_valid"] = False
    df.loc[mask, "validation_reason"] = "unknown_order"

    # Check product relationship
    valid_product_ids = set(
        products["product_id"].dropna()
    )

    mask = (
        df["product_id"].notna()
        & ~df["product_id"].isin(valid_product_ids)
    )

    df.loc[mask, "is_valid"] = False
    df.loc[mask, "validation_reason"] = "unknown_product"

    return df


# ============================================================
# SAVE RESULTS
# ============================================================

def save_data(df, filename):
    output_path = CLEAN_DATA_DIR / filename
    df.to_parquet(output_path, index=False)

    print(f"Saved: {output_path}")
    print(f"Rows: {len(df):,}")


# ============================================================
# SUMMARY
# ============================================================

def print_summary(name, df):

    print("\n" + "=" * 70)
    print(f"{name.upper()} VALIDATION SUMMARY")
    print("=" * 70)

    total = len(df)
    valid = df["is_valid"].sum()
    invalid = (~df["is_valid"]).sum()

    print(f"Total records:   {total:,}")
    print(f"Valid records:   {valid:,}")
    print(f"Invalid records: {invalid:,}")

    if invalid > 0:
        print("\nValidation reasons:")
        print(
            df.loc[~df["is_valid"], "validation_reason"]
            .value_counts()
        )


# ============================================================
# MAIN
# ============================================================

def main():

    setup_directories()

    print("Loading raw Parquet files...")

    customers = pd.read_parquet(
        RAW_DATA_DIR / "customers.parquet"
    )

    products = pd.read_parquet(
        RAW_DATA_DIR / "products.parquet"
    )

    orders = pd.read_parquet(
        RAW_DATA_DIR / "orders.parquet"
    )

    order_items = pd.read_parquet(
        RAW_DATA_DIR / "order_items.parquet"
    )

    print("Raw data loaded successfully.")

    # --------------------------------------------------------
    # Clean / validate
    # --------------------------------------------------------

    customers_clean = clean_customers(customers)

    products_clean = clean_products(products)

    orders_clean = clean_orders(
        orders,
        customers
    )

    order_items_clean = clean_order_items(
        order_items,
        orders,
        products
    )

    # --------------------------------------------------------
    # Print summaries
    # --------------------------------------------------------

    print_summary("Customers", customers_clean)
    print_summary("Products", products_clean)
    print_summary("Orders", orders_clean)
    print_summary("Order Items", order_items_clean)

    # --------------------------------------------------------
    # Save cleaned/validated copies
    # --------------------------------------------------------

    save_data(
        customers_clean,
        "customers_clean.parquet"
    )

    save_data(
        products_clean,
        "products_clean.parquet"
    )

    save_data(
        orders_clean,
        "orders_clean.parquet"
    )

    save_data(
        order_items_clean,
        "order_items_clean.parquet"
    )

    print("\nCleaning and validation complete.")


if __name__ == "__main__":
    main()
