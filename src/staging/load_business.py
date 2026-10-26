import os
from datetime import datetime
import psycopg2


DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5433"),
    "dbname": os.getenv("POSTGRES_DB", "appdb"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def values_different(a, b):
    """
    NULL-safe comparison.
    """
    if a is None and b is None:
        return False

    if a is None or b is None:
        return True

    return a != b


def customer_scd2_changed(previous, current):
    """
    Customer SCD Type 2 attributes:
        - address
        - segment
    """

    return (
        values_different(previous["address"], current["address"])
        or
        values_different(previous["segment"], current["segment"])
    )


def product_scd2_changed(previous, current):
    """
    Product SCD Type 2 attributes:
        - price
        - category
    """

    return (
        values_different(previous["price"], current["price"])
        or
        values_different(previous["category"], current["category"])
    )

# ============================================================
# CUSTOMER DIMENSION
# ============================================================

def load_customers(cursor):
    cursor.execute(
        """
        SELECT
            staging_record_id,
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
            source_system
        FROM staging.stg_customers
        WHERE is_valid = TRUE
          AND customer_id IS NOT NULL
        ORDER BY
            customer_id,
            updated_at NULLS FIRST,
            raw_record_id
        """
    )

    rows = cursor.fetchall()

    columns = [
        "staging_record_id",
        "raw_record_id",
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "address",
        "city",
        "country",
        "segment",
        "birthdate",
        "created_at",
        "updated_at",
        "source_system",
    ]

    versions = {}

    for row in rows:
        record = dict(zip(columns, row))
        versions.setdefault(record["customer_id"], []).append(record)

    inserted_versions = 0
    type1_updates = 0

    for customer_id, customer_versions in versions.items():

        current_dimension = None

        for version in customer_versions:

            # ------------------------------------------------
            # First observed version
            # ------------------------------------------------

            if current_dimension is None:

                valid_from = (
                    version["updated_at"]
                    or version["created_at"]
                )

                cursor.execute(
                    """
                    INSERT INTO business.dim_customer (
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
                        source_system,
                        valid_from,
                        valid_to,
                        is_current,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, NULL, TRUE,
                        %s, %s
                    )
                    RETURNING customer_sk
                    """,
                    (
                        version["customer_id"],
                        version["first_name"],
                        version["last_name"],
                        version["email"],
                        version["phone"],
                        version["address"],
                        version["city"],
                        version["country"],
                        version["segment"],
                        version["birthdate"],
                        version["source_system"],
                        valid_from,
                        version["created_at"],
                        version["updated_at"],
                    ),
                )

                customer_sk = cursor.fetchone()[0]

                current_dimension = {
                    **version,
                    "customer_sk": customer_sk,
                    "valid_from": valid_from,
                }

                inserted_versions += 1
                continue

            # ------------------------------------------------
            # SCD2 change
            # ------------------------------------------------

            if customer_scd2_changed(current_dimension, version):

                new_valid_from = (
                    version["updated_at"]
                    or version["created_at"]
                )

                cursor.execute(
                    """
                    UPDATE business.dim_customer
                    SET
                        valid_to = %s,
                        is_current = FALSE
                    WHERE customer_sk = %s
                    """,
                    (
                        new_valid_from,
                        current_dimension["customer_sk"],
                    ),
                )

                cursor.execute(
                    """
                    INSERT INTO business.dim_customer (
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
                        source_system,
                        valid_from,
                        valid_to,
                        is_current,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, NULL,
                        TRUE, %s, %s
                    )
                    RETURNING customer_sk
                    """,
                    (
                        version["customer_id"],
                        version["first_name"],
                        version["last_name"],
                        version["email"],
                        version["phone"],
                        version["address"],
                        version["city"],
                        version["country"],
                        version["segment"],
                        version["birthdate"],
                        version["source_system"],
                        new_valid_from,
                        version["created_at"],
                        version["updated_at"],
                    ),
                )

                customer_sk = cursor.fetchone()[0]

                current_dimension = {
                    **version,
                    "customer_sk": customer_sk,
                    "valid_from": new_valid_from,
                }

                inserted_versions += 1

            else:

                # ------------------------------------------------
                # Type 1 correction
                #
                # No new historical version.
                # Overwrite the current descriptive attributes.
                # ------------------------------------------------

                cursor.execute(
                    """
                    UPDATE business.dim_customer
                    SET
                        first_name = %s,
                        last_name = %s,
                        email = %s,
                        phone = %s,
                        city = %s,
                        country = %s,
                        birthdate = %s,
                        source_system = %s,
                        created_at = %s,
                        updated_at = %s
                    WHERE customer_sk = %s
                    """,
                    (
                        version["first_name"],
                        version["last_name"],
                        version["email"],
                        version["phone"],
                        version["city"],
                        version["country"],
                        version["birthdate"],
                        version["source_system"],
                        version["created_at"],
                        version["updated_at"],
                        current_dimension["customer_sk"],
                    ),
                )

                type1_updates += 1

                current_dimension.update(
                    {
                        "first_name": version["first_name"],
                        "last_name": version["last_name"],
                        "email": version["email"],
                        "phone": version["phone"],
                        "city": version["city"],
                        "country": version["country"],
                        "birthdate": version["birthdate"],
                        "source_system": version["source_system"],
                        "created_at": version["created_at"],
                        "updated_at": version["updated_at"],
                    }
                )

    return inserted_versions, type1_updates


# ============================================================
# PRODUCT DIMENSION
# ============================================================

def load_products(cursor):
    cursor.execute(
        """
        SELECT
            staging_record_id,
            raw_record_id,
            product_id,
            product_name,
            category,
            price,
            currency,
            stock_quantity,
            supplier,
            created_at,
            updated_at
        FROM staging.stg_products
        WHERE is_valid = TRUE
          AND product_id IS NOT NULL
        ORDER BY
            product_id,
            updated_at NULLS FIRST,
            raw_record_id
        """
    )

    rows = cursor.fetchall()

    columns = [
        "staging_record_id",
        "raw_record_id",
        "product_id",
        "product_name",
        "category",
        "price",
        "currency",
        "stock_quantity",
        "supplier",
        "created_at",
        "updated_at",
    ]

    versions = {}

    for row in rows:
        record = dict(zip(columns, row))
        versions.setdefault(record["product_id"], []).append(record)

    inserted_versions = 0
    type1_updates = 0

    for product_id, product_versions in versions.items():

        current_dimension = None

        for version in product_versions:

            if current_dimension is None:

                valid_from = (
                    version["updated_at"]
                    or version["created_at"]
                )

                cursor.execute(
                    """
                    INSERT INTO business.dim_product (
                        product_id,
                        product_name,
                        category,
                        price,
                        currency,
                        stock_quantity,
                        supplier,
                        valid_from,
                        valid_to,
                        is_current,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, NULL, TRUE, %s, %s
                    )
                    RETURNING product_sk
                    """,
                    (
                        version["product_id"],
                        version["product_name"],
                        version["category"],
                        version["price"],
                        version["currency"],
                        version["stock_quantity"],
                        version["supplier"],
                        valid_from,
                        version["created_at"],
                        version["updated_at"],
                    ),
                )

                product_sk = cursor.fetchone()[0]

                current_dimension = {
                    **version,
                    "product_sk": product_sk,
                    "valid_from": valid_from,
                }

                inserted_versions += 1
                continue

            # ------------------------------------------------
            # SCD2 change
            # ------------------------------------------------

            if product_scd2_changed(current_dimension, version):

                new_valid_from = (
                    version["updated_at"]
                    or version["created_at"]
                )

                cursor.execute(
                    """
                    UPDATE business.dim_product
                    SET
                        valid_to = %s,
                        is_current = FALSE
                    WHERE product_sk = %s
                    """,
                    (
                        new_valid_from,
                        current_dimension["product_sk"],
                    ),
                )

                cursor.execute(
                    """
                    INSERT INTO business.dim_product (
                        product_id,
                        product_name,
                        category,
                        price,
                        currency,
                        stock_quantity,
                        supplier,
                        valid_from,
                        valid_to,
                        is_current,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, NULL, TRUE, %s, %s
                    )
                    RETURNING product_sk
                    """,
                    (
                        version["product_id"],
                        version["product_name"],
                        version["category"],
                        version["price"],
                        version["currency"],
                        version["stock_quantity"],
                        version["supplier"],
                        new_valid_from,
                        version["created_at"],
                        version["updated_at"],
                    ),
                )

                product_sk = cursor.fetchone()[0]

                current_dimension = {
                    **version,
                    "product_sk": product_sk,
                    "valid_from": new_valid_from,
                }

                inserted_versions += 1

            else:

                # ------------------------------------------------
                # Type 1 correction
                # ------------------------------------------------

                cursor.execute(
                    """
                    UPDATE business.dim_product
                    SET
                        product_name = %s,
                        currency = %s,
                        stock_quantity = %s,
                        supplier = %s,
                        created_at = %s,
                        updated_at = %s
                    WHERE product_sk = %s
                    """,
                    (
                        version["product_name"],
                        version["currency"],
                        version["stock_quantity"],
                        version["supplier"],
                        version["created_at"],
                        version["updated_at"],
                        current_dimension["product_sk"],
                    ),
                )

                type1_updates += 1

                current_dimension.update(
                    {
                        "product_name": version["product_name"],
                        "currency": version["currency"],
                        "stock_quantity": version["stock_quantity"],
                        "supplier": version["supplier"],
                        "created_at": version["created_at"],
                        "updated_at": version["updated_at"],
                    }
                )

    return inserted_versions, type1_updates


# ============================================================
# ORDERS
# ============================================================

def load_orders(cursor):
    """
    Orders are facts, not SCD dimensions.

    Multiple staging versions of the same order_id are collapsed
    to the latest observed version based on updated_at/raw_record_id.
    """

    cursor.execute(
        """
        SELECT DISTINCT ON (order_id)
            staging_record_id,
            raw_record_id,
            order_id,
            customer_id,
            order_date,
            status,
            payment_method,
            updated_at
        FROM staging.stg_orders
        WHERE is_valid = TRUE
          AND order_id IS NOT NULL
        ORDER BY
            order_id,
            updated_at DESC NULLS LAST,
            raw_record_id DESC
        """
    )

    rows = cursor.fetchall()

    inserted = 0

    for row in rows:

        (
            staging_record_id,
            raw_record_id,
            order_id,
            customer_id,
            order_date,
            status,
            payment_method,
            updated_at,
        ) = row

        # Get the current customer dimension version.
        cursor.execute(
            """
            SELECT customer_sk
            FROM business.dim_customer
            WHERE customer_id = %s
                            AND CASE
                                        WHEN %s IS NULL THEN is_current
                                        ELSE valid_from <= %s
                                         AND (valid_to IS NULL OR valid_to > %s)
                                    END
                        ORDER BY valid_from DESC
            LIMIT 1
            """,
                        (customer_id, order_date, order_date, order_date),
        )

        customer_result = cursor.fetchone()

        if customer_result is None:
            continue

        customer_sk = customer_result[0]

        cursor.execute(
            """
            INSERT INTO business.fact_order (
                order_id,
                customer_sk,
                order_date,
                status,
                payment_method,
                source_updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_id)
            DO UPDATE SET
                customer_sk = EXCLUDED.customer_sk,
                order_date = EXCLUDED.order_date,
                status = EXCLUDED.status,
                payment_method = EXCLUDED.payment_method,
                source_updated_at = EXCLUDED.source_updated_at
            """,
            (
                order_id,
                customer_sk,
                order_date,
                status,
                payment_method,
                updated_at,
            ),
        )

        inserted += 1

    return inserted


# ============================================================
# ORDER ITEMS
# ============================================================

def load_order_items(cursor):
    """
    Load valid order items and resolve their surrogate keys.
    """

    cursor.execute(
        """
        SELECT
            order_item_id,
            order_id,
            product_id,
            quantity,
            unit_price,
            discount_pct
        FROM staging.stg_order_items
        WHERE is_valid = TRUE
          AND order_item_id IS NOT NULL
        """
    )

    rows = cursor.fetchall()

    inserted = 0
    skipped = 0

    for row in rows:

        (
            order_item_id,
            order_id,
            product_id,
            quantity,
            unit_price,
            discount_pct,
        ) = row

        # Resolve order surrogate key.
        cursor.execute(
            """
            SELECT order_sk
            FROM business.fact_order
            WHERE order_id = %s
            """,
            (order_id,),
        )

        order_result = cursor.fetchone()

        if order_result is None:
            skipped += 1
            continue

        order_sk = order_result[0]

        # Resolve current product surrogate key.
        cursor.execute(
            """
                        SELECT product.product_sk
                        FROM business.dim_product AS product
                        JOIN business.fact_order AS orders
                            ON orders.order_sk = %s
                        WHERE product.product_id = %s
                            AND CASE
                                        WHEN orders.order_date IS NULL THEN product.is_current
                                        ELSE product.valid_from <= orders.order_date
                                         AND (
                                                 product.valid_to IS NULL
                                                 OR product.valid_to > orders.order_date
                                         )
                                    END
                        ORDER BY product.valid_from DESC
            LIMIT 1
            """,
                        (order_sk, product_id),
        )

        product_result = cursor.fetchone()

        product_sk = (
            product_result[0]
            if product_result is not None
            else None
        )

        cursor.execute(
            """
            INSERT INTO business.fact_order_item (
                order_item_id,
                order_sk,
                product_sk,
                quantity,
                unit_price,
                discount_pct
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (order_item_id)
            DO UPDATE SET
                order_sk = EXCLUDED.order_sk,
                product_sk = EXCLUDED.product_sk,
                quantity = EXCLUDED.quantity,
                unit_price = EXCLUDED.unit_price,
                discount_pct = EXCLUDED.discount_pct
            """,
            (
                order_item_id,
                order_sk,
                product_sk,
                quantity,
                unit_price,
                discount_pct,
            ),
        )

        inserted += 1

    return inserted, skipped


# ============================================================
# MAIN
# ============================================================

def main():

    connection = get_connection()
    cursor = connection.cursor()

    pipeline_name = "load_business"

    cursor.execute(
        """
        INSERT INTO audit.pipeline_runs (
            pipeline_name,
            status
        )
        VALUES (%s, %s)
        RETURNING run_id
        """,
        (pipeline_name, "RUNNING"),
    )

    run_id = cursor.fetchone()[0]

    try:

        print("=" * 70)
        print("STAGING → BUSINESS")
        print("=" * 70)
        print(f"Pipeline run ID: {run_id}")
        print()

        # ----------------------------------------------------
        # Customers
        # ----------------------------------------------------

        customer_versions, customer_type1 = load_customers(cursor)

        print("CUSTOMERS")
        print(f"  SCD2 versions created : {customer_versions}")
        print(f"  Type 1 updates        : {customer_type1}")
        print()

        # ----------------------------------------------------
        # Products
        # ----------------------------------------------------

        product_versions, product_type1 = load_products(cursor)

        print("PRODUCTS")
        print(f"  SCD2 versions created : {product_versions}")
        print(f"  Type 1 updates        : {product_type1}")
        print()

        # ----------------------------------------------------
        # Orders
        # ----------------------------------------------------

        orders_loaded = load_orders(cursor)

        print("ORDERS")
        print(f"  Orders loaded         : {orders_loaded}")
        print()

        # ----------------------------------------------------
        # Order items
        # ----------------------------------------------------

        order_items_loaded, order_items_skipped = load_order_items(cursor)

        print("ORDER ITEMS")
        print(f"  Items loaded          : {order_items_loaded}")
        print(f"  Items skipped         : {order_items_skipped}")
        print()

        total_processed = (
            customer_versions
            + product_versions
            + orders_loaded
            + order_items_loaded
        )

        cursor.execute(
            """
            UPDATE audit.pipeline_runs
            SET
                finished_at = CURRENT_TIMESTAMP,
                status = 'SUCCESS',
                records_processed = %s,
                records_failed = %s
            WHERE run_id = %s
            """,
            (
                total_processed,
                order_items_skipped,
                run_id,
            ),
        )

        connection.commit()

        print("=" * 70)
        print("BUSINESS LOAD COMPLETED")
        print("=" * 70)
        print(f"Pipeline run ID : {run_id}")
        print(f"Records loaded  : {total_processed}")
        print(f"Records skipped : {order_items_skipped}")

    except Exception as exc:

        connection.rollback()

        cursor.execute(
            """
            UPDATE audit.pipeline_runs
            SET
                finished_at = CURRENT_TIMESTAMP,
                status = 'FAILED',
                error_message = %s
            WHERE run_id = %s
            """,
            (
                str(exc),
                run_id,
            ),
        )

        connection.commit()

        print("=" * 70)
        print("BUSINESS LOAD FAILED")
        print("=" * 70)
        print(f"Pipeline run ID: {run_id}")
        print(f"Error: {exc}")

        raise

    finally:
        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()