import json
import os

import pandas as pd
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


TABLE_CONFIG = {
    "customers": {
        "raw_table": "raw.customers",
        "staging_table": "staging.stg_customers",
        "key_column": "customer_id",
        "columns": [
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
            "is_valid",
            "validation_reason",
        ],
    },
    "products": {
        "raw_table": "raw.products",
        "staging_table": "staging.stg_products",
        "key_column": "product_id",
        "columns": [
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
            "is_valid",
            "validation_reason",
        ],
    },
    "orders": {
        "raw_table": "raw.orders",
        "staging_table": "staging.stg_orders",
        "key_column": "order_id",
        "columns": [
            "raw_record_id",
            "order_id",
            "customer_id",
            "order_date",
            "status",
            "payment_method",
            "updated_at",
            "is_valid",
            "validation_reason",
        ],
    },
    "order_items": {
        "raw_table": "raw.order_items",
        "staging_table": "staging.stg_order_items",
        "key_column": "order_item_id",
        "columns": [
            "raw_record_id",
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            "discount_pct",
            "is_valid",
            "validation_reason",
        ],
    },
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


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


def clean_value(value):
    """
    Convert pandas NULL/Timestamp values into PostgreSQL-compatible values.
    """
    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    return value


def normalize_text(value):
    """
    Normalize text values without filling NULLs.
    """
    if value is None:
        return None

    if pd.isna(value):
        return None

    return str(value).strip()


def add_reason(reasons, message):
    """
    Add a validation reason without overwriting previous reasons.
    """
    if message not in reasons:
        reasons.append(message)


def validate_customers(df):
    """
    Normalize and validate customer records.
    """

    df = df.copy()

    reasons = [[] for _ in range(len(df))]

    # Normalize source_system.
    df["source_system"] = df["source_system"].apply(normalize_text)

    df["source_system"] = df["source_system"].replace(
        {
            "crm": "CRM",
            "CRM": "CRM",
        }
    )

    # Normalize segment.
    df["segment"] = df["segment"].apply(
        lambda value: (
            normalize_text(value).lower()
            if normalize_text(value) is not None
            else None
        )
    )

    # Required customer_id.
    for index, value in df["customer_id"].items():
        if clean_value(value) is None:
            add_reason(reasons[index], "missing customer_id")

    df["is_valid"] = [
        len(reason_list) == 0
        for reason_list in reasons
    ]

    df["validation_reason"] = [
        "; ".join(reason_list)
        if reason_list
        else None
        for reason_list in reasons
    ]

    return df


def validate_products(df):
    """
    Normalize and validate product records.
    """

    df = df.copy()

    reasons = [[] for _ in range(len(df))]

    # Normalize category.
    df["category"] = df["category"].apply(
        lambda value: (
            normalize_text(value).lower()
            if normalize_text(value) is not None
            else None
        )
    )

    # Normalize currency.
    df["currency"] = df["currency"].apply(
        lambda value: (
            normalize_text(value).upper()
            if normalize_text(value) is not None
            else None
        )
    )

    # Required product_id.
    for index, value in df["product_id"].items():
        if clean_value(value) is None:
            add_reason(reasons[index], "missing product_id")

    # Invalid price.
    for index, value in df["price"].items():
        value = clean_value(value)

        if value is not None and value < 0:
            add_reason(reasons[index], "negative price")

    # Invalid stock quantity.
    for index, value in df["stock_quantity"].items():
        value = clean_value(value)

        if value is not None and value < 0:
            add_reason(reasons[index], "negative stock_quantity")

    df["is_valid"] = [
        len(reason_list) == 0
        for reason_list in reasons
    ]

    df["validation_reason"] = [
        "; ".join(reason_list)
        if reason_list
        else None
        for reason_list in reasons
    ]

    return df


def validate_orders(df, valid_customer_ids):
    """
    Normalize and validate order records.
    """

    df = df.copy()

    reasons = [[] for _ in range(len(df))]

    # Normalize status.
    df["status"] = df["status"].apply(
        lambda value: (
            normalize_text(value).lower()
            if normalize_text(value) is not None
            else "unknown"
        )
    )

    # Normalize payment method.
    df["payment_method"] = df["payment_method"].apply(
        lambda value: (
            normalize_text(value).lower()
            if normalize_text(value) is not None
            else None
        )
    )

    # Required order_id.
    for index, value in df["order_id"].items():
        if clean_value(value) is None:
            add_reason(reasons[index], "missing order_id")

    # Broken customer reference.
    for index, value in df["customer_id"].items():
        value = clean_value(value)

        if value is not None and value not in valid_customer_ids:
            add_reason(reasons[index], "unknown customer_id")

    df["is_valid"] = [
        len(reason_list) == 0
        for reason_list in reasons
    ]

    df["validation_reason"] = [
        "; ".join(reason_list)
        if reason_list
        else None
        for reason_list in reasons
    ]

    return df


def validate_order_items(
    df,
    valid_order_ids,
    valid_product_ids,
):
    """
    Normalize and validate order item records.
    """

    df = df.copy()

    reasons = [[] for _ in range(len(df))]

    # Required order_item_id.
    for index, value in df["order_item_id"].items():
        if clean_value(value) is None:
            add_reason(reasons[index], "missing order_item_id")

    # Quantity is required.
    for index, value in df["quantity"].items():
        value = clean_value(value)

        if value is None:
            add_reason(reasons[index], "missing quantity")
        elif value <= 0:
            add_reason(reasons[index], "non-positive quantity")

    # Unit price cannot be negative.
    for index, value in df["unit_price"].items():
        value = clean_value(value)

        if value is not None and value < 0:
            add_reason(reasons[index], "negative unit_price")

    # NULL discount_pct is allowed.
    for index, value in df["discount_pct"].items():
        value = clean_value(value)

        if value is not None and not 0 <= value <= 100:
            add_reason(
                reasons[index],
                "discount_pct outside 0-100 range",
            )

    # Broken order reference.
    for index, value in df["order_id"].items():
        value = clean_value(value)

        if value is not None and value not in valid_order_ids:
            add_reason(reasons[index], "unknown order_id")

    # Broken product reference.
    for index, value in df["product_id"].items():
        value = clean_value(value)

        if value is not None and value not in valid_product_ids:
            add_reason(reasons[index], "unknown product_id")

    df["is_valid"] = [
        len(reason_list) == 0
        for reason_list in reasons
    ]

    df["validation_reason"] = [
        "; ".join(reason_list)
        if reason_list
        else None
        for reason_list in reasons
    ]

    return df


def get_raw_data(cursor, raw_table):
    """
    Read the RAW PostgreSQL table into a DataFrame.
    """

    cursor.execute(f"SELECT * FROM {raw_table};")

    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]

    return pd.DataFrame(rows, columns=column_names)


def get_valid_reference_ids(cursor, table_name, key_column):
    """
    Get non-null IDs from a RAW table for referential validation.
    """

    cursor.execute(
        f"""
        SELECT DISTINCT {key_column}
        FROM {table_name}
        WHERE {key_column} IS NOT NULL;
        """
    )

    return {
        row[0]
        for row in cursor.fetchall()
    }


def get_existing_staging_ids(cursor, staging_table):
    """
    Get RAW record IDs that have already been staged.

    This makes the staging step idempotent.
    """

    cursor.execute(
        f"""
        SELECT raw_record_id
        FROM {staging_table}
        WHERE raw_record_id IS NOT NULL;
        """
    )

    return {
        row[0]
        for row in cursor.fetchall()
    }


def insert_valid_records(cursor, staging_table, columns, df):
    """
    Insert valid records into the staging table in batches.
    """

    if df.empty:
        return

    column_list = ", ".join(columns)

    insert_query = f"""
        INSERT INTO {staging_table} (
            {column_list}
        )
        VALUES %s;
    """

    values = []

    for _, row in df.iterrows():
        values.append(
            tuple(
                clean_value(row[column])
                for column in columns
            )
        )

    for start in range(0, len(values), BATCH_SIZE):
        batch = values[start:start + BATCH_SIZE]

        execute_values(
            cursor,
            insert_query,
            batch,
            page_size=BATCH_SIZE,
        )


def insert_rejected_records(
    cursor,
    run_id,
    raw_table,
    key_column,
    df,
):
    """
    Store invalid records in audit.rejected_records.
    """

    if df.empty:
        return

    rejected_values = []

    for _, row in df.iterrows():

        record_key = clean_value(row.get(key_column))

        record_data = {
            column: clean_value(row[column])
            for column in row.index
        }

        rejected_values.append(
            (
                run_id,
                raw_table,
                str(record_key) if record_key is not None else None,
                str(
                    row.get(
                        "validation_reason",
                        "validation failed",
                    )
                ),
                json.dumps(
                    record_data,
                    default=str,
                ),
            )
        )

    rejected_query = """
        INSERT INTO audit.rejected_records (
            run_id,
            source_table,
            record_key,
            reason,
            record_data
        )
        VALUES %s;
    """

    for start in range(
        0,
        len(rejected_values),
        BATCH_SIZE,
    ):
        batch = rejected_values[
            start:start + BATCH_SIZE
        ]

        execute_values(
            cursor,
            rejected_query,
            batch,
            page_size=BATCH_SIZE,
        )


def record_dq_result(
    cursor,
    run_id,
    table_name,
    total_rows,
    valid_rows,
    invalid_rows,
):
    """
    Store overall validation statistics.
    """

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
        VALUES (%s, %s, %s, %s, %s, %s);
        """,
        (
            run_id,
            table_name,
            "overall_validation",
            total_rows,
            valid_rows,
            invalid_rows,
        ),
    )


def process_table(
    cursor,
    run_id,
    table_name,
    config,
    valid_customer_ids=None,
    valid_order_ids=None,
    valid_product_ids=None,
):
    """
    Read RAW → validate/normalize → STAGING/AUDIT.
    """

    print()
    print("=" * 70)
    print(f"PROCESSING: {table_name}")
    print("=" * 70)

    raw_table = config["raw_table"]
    staging_table = config["staging_table"]

    # ------------------------------------------------------------
    # 1. Read directly from PostgreSQL RAW.
    # ------------------------------------------------------------

    df = get_raw_data(cursor, raw_table)

    source_total = len(df)

    print(f"RAW rows             : {source_total:,}")

    if source_total == 0:
        print("No RAW records found.")

        record_dq_result(
            cursor,
            run_id,
            table_name,
            0,
            0,
            0,
        )

        return 0, 0

    # ------------------------------------------------------------
    # 2. Remove records already processed by STAGING.
    # ------------------------------------------------------------

    existing_raw_ids = get_existing_staging_ids(
        cursor,
        staging_table,
    )

    if existing_raw_ids:
        df = df[
            ~df["raw_record_id"].isin(existing_raw_ids)
        ].copy()

    new_rows = len(df)

    print(f"New RAW rows         : {new_rows:,}")

    if new_rows == 0:
        print("Nothing new to validate/stage.")

        return 0, 0

    # ------------------------------------------------------------
    # 3. Validate and normalize.
    # ------------------------------------------------------------

    if table_name == "customers":

        df = validate_customers(df)

    elif table_name == "products":

        df = validate_products(df)

    elif table_name == "orders":

        df = validate_orders(
            df,
            valid_customer_ids,
        )

    elif table_name == "order_items":

        df = validate_order_items(
            df,
            valid_order_ids,
            valid_product_ids,
        )

    else:
        raise ValueError(
            f"Unknown table: {table_name}"
        )

    # ------------------------------------------------------------
    # 4. Split valid and invalid records.
    # ------------------------------------------------------------

    valid_df = df[
        df["is_valid"].fillna(False)
    ].copy()

    invalid_df = df[
        ~df["is_valid"].fillna(False)
    ].copy()

    valid_count = len(valid_df)
    invalid_count = len(invalid_df)

    print(f"Valid rows           : {valid_count:,}")
    print(f"Invalid rows         : {invalid_count:,}")

    # ------------------------------------------------------------
    # 5. Insert valid records into STAGING.
    # ------------------------------------------------------------

    insert_valid_records(
        cursor,
        staging_table,
        config["columns"],
        valid_df,
    )

    # ------------------------------------------------------------
    # 6. Store invalid records in audit.rejected_records.
    # ------------------------------------------------------------

    insert_rejected_records(
        cursor,
        run_id,
        raw_table,
        config["key_column"],
        invalid_df,
    )

    # ------------------------------------------------------------
    # 7. Store DQ statistics.
    # ------------------------------------------------------------

    record_dq_result(
        cursor,
        run_id,
        table_name,
        new_rows,
        valid_count,
        invalid_count,
    )

    print(f"Staged records       : {valid_count:,}")
    print(f"Rejected records     : {invalid_count:,}")

    return valid_count, invalid_count


def main():

    print("=" * 70)
    print("VALIDATE RAW → STAGING")
    print("=" * 70)

    connection = get_connection()
    cursor = connection.cursor()

    run_id = None

    total_staged = 0
    total_rejected = 0

    try:

        # --------------------------------------------------------
        # Create pipeline run.
        # --------------------------------------------------------

        run_id = create_pipeline_run(cursor)

        print(f"Pipeline run ID      : {run_id}")

        # --------------------------------------------------------
        # Reference IDs.
        #
        # These are read from RAW, not from Parquet.
        # --------------------------------------------------------

        valid_customer_ids = get_valid_reference_ids(
            cursor,
            "raw.customers",
            "customer_id",
        )

        valid_product_ids = get_valid_reference_ids(
            cursor,
            "raw.products",
            "product_id",
        )

        valid_order_ids = get_valid_reference_ids(
            cursor,
            "raw.orders",
            "order_id",
        )

        print(
            f"Customer reference IDs: "
            f"{len(valid_customer_ids):,}"
        )

        print(
            f"Product reference IDs : "
            f"{len(valid_product_ids):,}"
        )

        print(
            f"Order reference IDs   : "
            f"{len(valid_order_ids):,}"
        )

        # --------------------------------------------------------
        # Process in dependency order.
        # --------------------------------------------------------

        for table_name in [
            "customers",
            "products",
            "orders",
            "order_items",
        ]:

            config = TABLE_CONFIG[table_name]

            staged, rejected = process_table(
                cursor,
                run_id,
                table_name,
                config,
                valid_customer_ids,
                valid_order_ids,
                valid_product_ids,
            )

            total_staged += staged
            total_rejected += rejected

        # --------------------------------------------------------
        # Mark pipeline successful.
        # --------------------------------------------------------

        finish_pipeline_run(
            cursor,
            run_id,
            "SUCCESS",
            total_staged,
            total_rejected,
        )

        connection.commit()

        print()
        print("=" * 70)
        print("VALIDATION + STAGING COMPLETED")
        print("=" * 70)

        print(
            f"Pipeline run ID      : {run_id}"
        )

        print(
            f"Records staged       : "
            f"{total_staged:,}"
        )

        print(
            f"Records rejected     : "
            f"{total_rejected:,}"
        )

    except Exception as error:

        connection.rollback()

        print()
        print("=" * 70)
        print("VALIDATION + STAGING FAILED")
        print("=" * 70)

        print(str(error))

        if run_id is not None:

            finish_pipeline_run(
                cursor,
                run_id,
                "FAILED",
                total_staged,
                total_rejected,
                str(error),
            )

            connection.commit()

        raise

    finally:

        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()

