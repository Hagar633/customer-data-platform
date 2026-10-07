import os
import json

import psycopg2
from psycopg2.extras import execute_values


DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5433"),
    "dbname": os.getenv("POSTGRES_DB", "appdb"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}

BATCH_SIZE = 1000


# ============================================================
# DATABASE CONNECTION
# ============================================================

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


# ============================================================
# NORMALIZATION HELPERS
# ============================================================

def normalize_text(value):
    """
    Trim whitespace.

    Empty strings become NULL.
    Existing NULL values remain NULL.
    """

    if value is None:
        return None

    value = str(value).strip()

    if value == "":
        return None

    return value


def normalize_lower(value):
    """
    Trim text and convert it to lowercase.
    """

    value = normalize_text(value)

    if value is None:
        return None

    return value.lower()


def normalize_upper(value):
    """
    Trim text and convert it to uppercase.
    """

    value = normalize_text(value)

    if value is None:
        return None

    return value.upper()


# ============================================================
# VALIDATION HELPER
# ============================================================

def add_reason(reasons, reason):
    """
    Add a validation reason without creating duplicates.
    """

    if reason not in reasons:
        reasons.append(reason)


# ============================================================
# PIPELINE AUDIT
# ============================================================

def create_pipeline_run(cursor):
    cursor.execute(
        """
        INSERT INTO audit.pipeline_runs (
            pipeline_name,
            status
        )
        VALUES (%s, %s)
        RETURNING run_id;
        """,
        ("validate_stage", "RUNNING"),
    )

    return cursor.fetchone()[0]


def finish_pipeline_run(
    cursor,
    run_id,
    status,
    records_processed,
    records_failed,
    error_message=None,
):
    cursor.execute(
        """
        UPDATE audit.pipeline_runs
        SET
            finished_at = CURRENT_TIMESTAMP,
            status = %s,
            records_processed = %s,
            records_failed = %s,
            error_message = %s
        WHERE run_id = %s;
        """,
        (
            status,
            records_processed,
            records_failed,
            error_message,
            run_id,
        ),
    )


# ============================================================
# CLEAR STAGING
# ============================================================

def clear_previous_staging(cursor):
    """
    STAGING is rebuilt from RAW.

    RAW is never modified.
    """

    cursor.execute(
        """
        TRUNCATE TABLE
            staging.stg_order_items,
            staging.stg_orders,
            staging.stg_products,
            staging.stg_customers
        RESTART IDENTITY;
        """
    )


# ============================================================
# REFERENCE DATA
# ============================================================

def get_reference_ids(cursor):

    cursor.execute(
        """
        SELECT DISTINCT customer_id
        FROM raw.customers
        WHERE customer_id IS NOT NULL;
        """
    )

    customer_ids = {
        row[0]
        for row in cursor.fetchall()
    }

    cursor.execute(
        """
        SELECT DISTINCT product_id
        FROM raw.products
        WHERE product_id IS NOT NULL;
        """
    )

    product_ids = {
        row[0]
        for row in cursor.fetchall()
    }

    cursor.execute(
        """
        SELECT DISTINCT order_id
        FROM raw.orders
        WHERE order_id IS NOT NULL;
        """
    )

    order_ids = {
        row[0]
        for row in cursor.fetchall()
    }

    return customer_ids, product_ids, order_ids


# ============================================================
# CUSTOMERS
# ============================================================

def load_customers(cursor, run_id):

    cursor.execute(
        """
        SELECT
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
        FROM raw.customers
        ORDER BY raw_record_id;
        """
    )

    rows = cursor.fetchall()

    seen = set()
    staging_values = []
    rejected_values = []

    for row in rows:

        (
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
            source_system,
        ) = row

        # ----------------------------------------------------
        # FULL DUPLICATE REMOVAL
        # ----------------------------------------------------

        duplicate_key = (
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
            source_system,
        )

        if duplicate_key in seen:
            continue

        seen.add(duplicate_key)

        # ----------------------------------------------------
        # NORMALIZATION
        # ----------------------------------------------------

        customer_id = normalize_text(customer_id)
        segment = normalize_lower(segment)
        source_system = normalize_upper(source_system)

        # ----------------------------------------------------
        # VALIDATION
        # ----------------------------------------------------

        reasons = []

        if customer_id is None:
            add_reason(
                reasons,
                "missing customer_id",
            )

        is_valid = len(reasons) == 0

        validation_reason = (
            "; ".join(reasons)
            if reasons
            else None
        )

        staging_values.append(
            (
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
                source_system,
                is_valid,
                validation_reason,
            )
        )

        # ----------------------------------------------------
        # REJECTION AUDIT
        # ----------------------------------------------------

        if not is_valid:

            rejected_values.append(
                (
                    run_id,
                    "raw.customers",
                    customer_id,
                    validation_reason,
                    json.dumps(
                        {
                            "raw_record_id": raw_record_id,
                            "customer_id": customer_id,
                            "first_name": first_name,
                            "last_name": last_name,
                            "email": email,
                            "phone": phone,
                            "address": address,
                            "city": city,
                            "country": country,
                            "segment": segment,
                            "birthdate": str(birthdate)
                            if birthdate is not None
                            else None,
                            "created_at": str(created_at)
                            if created_at is not None
                            else None,
                            "updated_at": str(updated_at)
                            if updated_at is not None
                            else None,
                            "source_system": source_system,
                        },
                        default=str,
                    ),
                )
            )

    # --------------------------------------------------------
    # INSERT STAGING
    # --------------------------------------------------------

    if staging_values:

        execute_values(
            cursor,
            """
            INSERT INTO staging.stg_customers (
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
                source_system,
                is_valid,
                validation_reason
            )
            VALUES %s;
            """,
            staging_values,
            page_size=BATCH_SIZE,
        )

    # --------------------------------------------------------
    # INSERT REJECTS
    # --------------------------------------------------------

    if rejected_values:

        execute_values(
            cursor,
            """
            INSERT INTO audit.rejected_records (
                run_id,
                source_table,
                record_key,
                reason,
                record_data
            )
            VALUES %s;
            """,
            rejected_values,
            page_size=BATCH_SIZE,
        )

    return len(staging_values), len(rejected_values)


# ============================================================
# PRODUCTS
# ============================================================

def load_products(cursor, run_id):

    cursor.execute(
        """
        SELECT
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
        FROM raw.products
        ORDER BY raw_record_id;
        """
    )

    rows = cursor.fetchall()

    seen = set()
    staging_values = []
    rejected_values = []

    for row in rows:

        (
            raw_record_id,
            product_id,
            product_name,
            category,
            price,
            currency,
            stock_quantity,
            supplier,
            created_at,
            updated_at,
        ) = row

        # ----------------------------------------------------
        # FULL DUPLICATE REMOVAL
        # ----------------------------------------------------

        duplicate_key = (
            product_id,
            product_name,
            category,
            price,
            currency,
            stock_quantity,
            supplier,
            created_at,
            updated_at,
        )

        if duplicate_key in seen:
            continue

        seen.add(duplicate_key)

        # ----------------------------------------------------
        # NORMALIZATION
        # ----------------------------------------------------

        product_id = normalize_text(product_id)
        category = normalize_lower(category)
        currency = normalize_upper(currency)
        supplier = normalize_text(supplier)

        # ----------------------------------------------------
        # VALIDATION
        # ----------------------------------------------------

        reasons = []

        if product_id is None:
            add_reason(
                reasons,
                "missing product_id",
            )

        if price is None:
            add_reason(
                reasons,
                "missing price",
            )
        elif price < 0:
            add_reason(
                reasons,
                "negative price",
            )

        if stock_quantity is None:
            add_reason(
                reasons,
                "missing stock_quantity",
            )
        elif stock_quantity < 0:
            add_reason(
                reasons,
                "negative stock_quantity",
            )

        is_valid = len(reasons) == 0

        validation_reason = (
            "; ".join(reasons)
            if reasons
            else None
        )

        # ----------------------------------------------------
        # KEEP INVALID VALUE
        # ----------------------------------------------------

        staging_values.append(
            (
                raw_record_id,
                product_id,
                product_name,
                category,
                price,
                currency,
                stock_quantity,
                supplier,
                created_at,
                updated_at,
                is_valid,
                validation_reason,
            )
        )

        # ----------------------------------------------------
        # REJECTION AUDIT
        # ----------------------------------------------------

        if not is_valid:

            rejected_values.append(
                (
                    run_id,
                    "raw.products",
                    product_id,
                    validation_reason,
                    json.dumps(
                        {
                            "raw_record_id": raw_record_id,
                            "product_id": product_id,
                            "product_name": product_name,
                            "category": category,
                            "price": str(price)
                            if price is not None
                            else None,
                            "currency": currency,
                            "stock_quantity": (
                                str(stock_quantity)
                                if stock_quantity is not None
                                else None
                            ),
                            "supplier": supplier,
                            "created_at": str(created_at)
                            if created_at is not None
                            else None,
                            "updated_at": str(updated_at)
                            if updated_at is not None
                            else None,
                        },
                        default=str,
                    ),
                )
            )

    # --------------------------------------------------------
    # INSERT STAGING
    # --------------------------------------------------------

    if staging_values:

        execute_values(
            cursor,
            """
            INSERT INTO staging.stg_products (
                raw_record_id,
                product_id,
                product_name,
                category,
                price,
                currency,
                stock_quantity,
                supplier,
                created_at,
                updated_at,
                is_valid,
                validation_reason
            )
            VALUES %s;
            """,
            staging_values,
            page_size=BATCH_SIZE,
        )

    # --------------------------------------------------------
    # INSERT REJECTS
    # --------------------------------------------------------

    if rejected_values:

        execute_values(
            cursor,
            """
            INSERT INTO audit.rejected_records (
                run_id,
                source_table,
                record_key,
                reason,
                record_data
            )
            VALUES %s;
            """,
            rejected_values,
            page_size=BATCH_SIZE,
        )

    return len(staging_values), len(rejected_values)


# ============================================================
# ORDERS
# ============================================================

def load_orders(cursor, run_id, customer_ids):

    cursor.execute(
        """
        SELECT
            raw_record_id,
            order_id,
            customer_id,
            order_date,
            status,
            payment_method,
            updated_at
        FROM raw.orders
        ORDER BY raw_record_id;
        """
    )

    rows = cursor.fetchall()

    seen = set()
    staging_values = []
    rejected_values = []

    for row in rows:

        (
            raw_record_id,
            order_id,
            customer_id,
            order_date,
            status,
            payment_method,
            updated_at,
        ) = row

        # ----------------------------------------------------
        # FULL DUPLICATE REMOVAL
        # ----------------------------------------------------

        duplicate_key = (
            order_id,
            customer_id,
            order_date,
            status,
            payment_method,
            updated_at,
        )

        if duplicate_key in seen:
            continue

        seen.add(duplicate_key)

        # ----------------------------------------------------
        # NORMALIZATION
        # ----------------------------------------------------

        order_id = normalize_text(order_id)
        customer_id = normalize_text(customer_id)

        status = normalize_lower(status)
        payment_method = normalize_lower(payment_method)

        # NULL STATUS IS ALLOWED AND BECOMES UNKNOWN
        if status is None:
            status = "unknown"

        # ----------------------------------------------------
        # VALIDATION
        # ----------------------------------------------------

        reasons = []

        if order_id is None:
            add_reason(
                reasons,
                "missing order_id",
            )

        if customer_id is None:
            add_reason(
                reasons,
                "missing customer_id",
            )
        elif customer_id not in customer_ids:
            add_reason(
                reasons,
                "unknown customer_id",
            )

        is_valid = len(reasons) == 0

        validation_reason = (
            "; ".join(reasons)
            if reasons
            else None
        )

        staging_values.append(
            (
                raw_record_id,
                order_id,
                customer_id,
                order_date,
                status,
                payment_method,
                updated_at,
                None,
                is_valid,
                validation_reason,
            )
        )

        # ----------------------------------------------------
        # REJECTION AUDIT
        # ----------------------------------------------------

        if not is_valid:

            rejected_values.append(
                (
                    run_id,
                    "raw.orders",
                    order_id,
                    validation_reason,
                    json.dumps(
                        {
                            "raw_record_id": raw_record_id,
                            "order_id": order_id,
                            "customer_id": customer_id,
                            "order_date": str(order_date)
                            if order_date is not None
                            else None,
                            "status": status,
                            "payment_method": payment_method,
                            "updated_at": str(updated_at)
                            if updated_at is not None
                            else None,
                        },
                        default=str,
                    ),
                )
            )

    # --------------------------------------------------------
    # INSERT STAGING
    # --------------------------------------------------------

    if staging_values:

        execute_values(
            cursor,
            """
            INSERT INTO staging.stg_orders (
                raw_record_id,
                order_id,
                customer_id,
                order_date,
                status,
                payment_method,
                updated_at,
                total_amount,
                is_valid,
                validation_reason
            )
            VALUES %s;
            """,
            staging_values,
            page_size=BATCH_SIZE,
        )

    # --------------------------------------------------------
    # INSERT REJECTS
    # --------------------------------------------------------

    if rejected_values:

        execute_values(
            cursor,
            """
            INSERT INTO audit.rejected_records (
                run_id,
                source_table,
                record_key,
                reason,
                record_data
            )
            VALUES %s;
            """,
            rejected_values,
            page_size=BATCH_SIZE,
        )

    return len(staging_values), len(rejected_values)


# ============================================================
# ORDER ITEMS
# ============================================================

def load_order_items(
    cursor,
    run_id,
    order_ids,
    product_ids,
):

    cursor.execute(
        """
        SELECT
            raw_record_id,
            order_item_id,
            order_id,
            product_id,
            quantity,
            unit_price,
            discount_pct
        FROM raw.order_items
        ORDER BY raw_record_id;
        """
    )

    rows = cursor.fetchall()

    seen = set()
    staging_values = []
    rejected_values = []

    for row in rows:

        (
            raw_record_id,
            order_item_id,
            order_id,
            product_id,
            quantity,
            unit_price,
            discount_pct,
        ) = row

        # ----------------------------------------------------
        # FULL DUPLICATE REMOVAL
        # ----------------------------------------------------

        duplicate_key = (
            order_item_id,
            order_id,
            product_id,
            quantity,
            unit_price,
            discount_pct,
        )

        if duplicate_key in seen:
            continue

        seen.add(duplicate_key)

        # ----------------------------------------------------
        # NORMALIZATION
        # ----------------------------------------------------

        order_item_id = normalize_text(order_item_id)
        order_id = normalize_text(order_id)
        product_id = normalize_text(product_id)

        # ----------------------------------------------------
        # VALIDATION
        # ----------------------------------------------------

        reasons = []

        if order_item_id is None:
            add_reason(
                reasons,
                "missing order_item_id",
            )

        if order_id is None:
            add_reason(
                reasons,
                "missing order_id",
            )
        elif order_id not in order_ids:
            add_reason(
                reasons,
                "unknown order_id",
            )

        if product_id is None:
            add_reason(
                reasons,
                "missing product_id",
            )
        elif product_id not in product_ids:
            add_reason(
                reasons,
                "unknown product_id",
            )

        # ----------------------------------------------------
        # QUANTITY
        # ----------------------------------------------------

        if quantity is None:
            add_reason(
                reasons,
                "missing quantity",
            )
        elif quantity <= 0:
            add_reason(
                reasons,
                "quantity must be greater than zero",
            )

        # ----------------------------------------------------
        # UNIT PRICE
        # ----------------------------------------------------

        if unit_price is None:
            add_reason(
                reasons,
                "missing unit_price",
            )
        elif unit_price < 0:
            add_reason(
                reasons,
                "negative unit_price",
            )

        # ----------------------------------------------------
        # DISCOUNT
        # ----------------------------------------------------

        # NULL discount is VALID.
        # It means the discount was not provided.

        if discount_pct is not None:

            if discount_pct < 0:
                add_reason(
                    reasons,
                    "negative discount_pct",
                )

            elif discount_pct > 100:
                add_reason(
                    reasons,
                    "discount_pct greater than 100",
                )

        is_valid = len(reasons) == 0

        validation_reason = (
            "; ".join(reasons)
            if reasons
            else None
        )

        # ----------------------------------------------------
        # KEEP ORIGINAL VALUES
        # ----------------------------------------------------

        staging_values.append(
            (
                raw_record_id,
                order_item_id,
                order_id,
                product_id,
                quantity,
                unit_price,
                discount_pct,
                is_valid,
                validation_reason,
            )
        )

        # ----------------------------------------------------
        # REJECTION AUDIT
        # ----------------------------------------------------

        if not is_valid:

            rejected_values.append(
                (
                    run_id,
                    "raw.order_items",
                    order_item_id,
                    validation_reason,
                    json.dumps(
                        {
                            "raw_record_id": raw_record_id,
                            "order_item_id": order_item_id,
                            "order_id": order_id,
                            "product_id": product_id,
                            "quantity": (
                                str(quantity)
                                if quantity is not None
                                else None
                            ),
                            "unit_price": (
                                str(unit_price)
                                if unit_price is not None
                                else None
                            ),
                            "discount_pct": (
                                str(discount_pct)
                                if discount_pct is not None
                                else None
                            ),
                        },
                        default=str,
                    ),
                )
            )

    # --------------------------------------------------------
    # INSERT STAGING
    # --------------------------------------------------------

    if staging_values:

        execute_values(
            cursor,
            """
            INSERT INTO staging.stg_order_items (
                raw_record_id,
                order_item_id,
                order_id,
                product_id,
                quantity,
                unit_price,
                discount_pct,
                is_valid,
                validation_reason
            )
            VALUES %s;
            """,
            staging_values,
            page_size=BATCH_SIZE,
        )

    # --------------------------------------------------------
    # INSERT REJECTS
    # --------------------------------------------------------

    if rejected_values:

        execute_values(
            cursor,
            """
            INSERT INTO audit.rejected_records (
                run_id,
                source_table,
                record_key,
                reason,
                record_data
            )
            VALUES %s;
            """,
            rejected_values,
            page_size=BATCH_SIZE,
        )

    return len(staging_values), len(rejected_values)


# ============================================================
# CALCULATE ORDER TOTALS
# ============================================================

def calculate_order_totals(cursor):

    """
    Calculate total_amount from VALID order items.

    Formula:

        quantity * unit_price *
        (1 - discount_pct / 100)

    NULL discount_pct is treated as 0%
    ONLY for this calculation.

    The original discount_pct remains NULL.
    """

    cursor.execute(
        """
        UPDATE staging.stg_orders AS orders
        SET total_amount = totals.total_amount
        FROM (
            SELECT
                order_id,
                ROUND(
                    SUM(
                        quantity
                        * unit_price
                        * (
                            1
                            - COALESCE(discount_pct, 0) / 100
                        )
                    ),
                    2
                ) AS total_amount
            FROM staging.stg_order_items
            WHERE is_valid = TRUE
              AND order_id IS NOT NULL
            GROUP BY order_id
        ) AS totals
        WHERE orders.order_id = totals.order_id;
        """
    )

    return cursor.rowcount


# ============================================================
# DATA QUALITY AUDIT
# ============================================================

def insert_dq_results(
    cursor,
    run_id,
    table_name,
    records_checked,
    records_failed,
):

    cursor.execute(
        """
        INSERT INTO audit.data_quality_results (
            run_id,
            table_name,
            rule_name,
            records_checked,
            records_passed,
            records_failed
        )
        VALUES (
            %s,
            %s,
            %s,
            %s,
            %s,
            %s
        );
        """,
        (
            run_id,
            table_name,
            "validation_and_cleaning",
            records_checked,
            records_checked - records_failed,
            records_failed,
        ),
    )


# ============================================================
# MAIN
# ============================================================

def main():

    connection = get_connection()
    cursor = connection.cursor()

    run_id = None

    try:

        # ----------------------------------------------------
        # START PIPELINE RUN
        # ----------------------------------------------------

        run_id = create_pipeline_run(cursor)

        clear_previous_staging(cursor)

        (
            customer_ids,
            product_ids,
            order_ids,
        ) = get_reference_ids(cursor)

        print(f"Pipeline run ID: {run_id}")
        print(
            f"Customer reference IDs: "
            f"{len(customer_ids):,}"
        )
        print(
            f"Product reference IDs: "
            f"{len(product_ids):,}"
        )
        print(
            f"Order reference IDs: "
            f"{len(order_ids):,}"
        )
        print()

        # ----------------------------------------------------
        # CUSTOMERS
        # ----------------------------------------------------

        customers_processed, customers_failed = (
            load_customers(
                cursor,
                run_id,
            )
        )

        print(
            f"customers: "
            f"{customers_processed - customers_failed:,} "
            f"valid, "
            f"{customers_failed:,} invalid"
        )

        insert_dq_results(
            cursor,
            run_id,
            "customers",
            customers_processed,
            customers_failed,
        )

        # ----------------------------------------------------
        # PRODUCTS
        # ----------------------------------------------------

        products_processed, products_failed = (
            load_products(
                cursor,
                run_id,
            )
        )

        print(
            f"products: "
            f"{products_processed - products_failed:,} "
            f"valid, "
            f"{products_failed:,} invalid"
        )

        insert_dq_results(
            cursor,
            run_id,
            "products",
            products_processed,
            products_failed,
        )

        # ----------------------------------------------------
        # ORDERS
        # ----------------------------------------------------

        orders_processed, orders_failed = (
            load_orders(
                cursor,
                run_id,
                customer_ids,
            )
        )

        print(
            f"orders: "
            f"{orders_processed - orders_failed:,} "
            f"valid, "
            f"{orders_failed:,} invalid"
        )

        insert_dq_results(
            cursor,
            run_id,
            "orders",
            orders_processed,
            orders_failed,
        )

        # ----------------------------------------------------
        # ORDER ITEMS
        # ----------------------------------------------------

        order_items_processed, order_items_failed = (
            load_order_items(
                cursor,
                run_id,
                order_ids,
                product_ids,
            )
        )

        print(
            f"order_items: "
            f"{order_items_processed - order_items_failed:,} "
            f"valid, "
            f"{order_items_failed:,} invalid"
        )

        insert_dq_results(
            cursor,
            run_id,
            "order_items",
            order_items_processed,
            order_items_failed,
        )

        # ----------------------------------------------------
        # ORDER TOTALS
        # ----------------------------------------------------

        totals_updated = calculate_order_totals(cursor)

        print()
        print(
            f"Order totals calculated for "
            f"{totals_updated:,} orders"
        )

        # ----------------------------------------------------
        # FINAL AUDIT
        # ----------------------------------------------------

        total_processed = (
            customers_processed
            + products_processed
            + orders_processed
            + order_items_processed
        )

        total_failed = (
            customers_failed
            + products_failed
            + orders_failed
            + order_items_failed
        )

        finish_pipeline_run(
            cursor,
            run_id,
            "SUCCESS",
            total_processed,
            total_failed,
        )

        connection.commit()

        print()
        print("=" * 60)
        print("VALIDATION + CLEANING COMPLETED")
        print("=" * 60)
        print(
            f"Total processed: {total_processed:,}"
        )
        print(
            f"Total rejected:  {total_failed:,}"
        )
        print(
            f"Run ID:          {run_id}"
        )

    except Exception as e:

        connection.rollback()

        if run_id is not None:

            try:

                finish_pipeline_run(
                    cursor,
                    run_id,
                    "FAILED",
                    0,
                    0,
                    str(e),
                )

                connection.commit()

            except Exception:

                connection.rollback()

        raise

    finally:

        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()