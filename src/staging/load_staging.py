import json
import os
from pathlib import Path

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


# ============================================================
# CONFIGURATION
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CLEAN_DIR = PROJECT_ROOT / "data" / "clean"

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5433"),
    "dbname": os.getenv("POSTGRES_DB", "appdb"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}

BATCH_SIZE = 1000


# ============================================================
# TABLE CONFIGURATION
# ============================================================

TABLE_CONFIG = {
    "customers": {
        "file": "customers.parquet",
        "table": "staging.stg_customers",
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
        "file": "products.parquet",
        "table": "staging.stg_products",
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
        "file": "orders.parquet",
        "table": "staging.stg_orders",
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
        "file": "order_items.parquet",
        "table": "staging.stg_order_items",
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


# ============================================================
# DATABASE
# ============================================================

def get_connection():
    return psycopg2.connect(**DB_CONFIG)


# ============================================================
# PIPELINE RUN
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
        ("staging_load", "RUNNING"),
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
# VALUE CONVERSION
# ============================================================

def clean_value(value):
    """
    Convert pandas-specific missing values to Python None.
    This allows PostgreSQL to store them as SQL NULL.
    """

    if pd.isna(value):
        return None

    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    return value


# ============================================================
# LOAD ONE TABLE
# ============================================================

def load_table(cursor, run_id, table_name, config):
    file_path = CLEAN_DIR / config["file"]

    if not file_path.exists():
        raise FileNotFoundError(
            f"Clean file not found: {file_path}"
        )

    print()
    print("=" * 70)
    print(f"TABLE: {table_name}")
    print(f"FILE : {file_path}")
    print("=" * 70)

    df = pd.read_parquet(file_path)

    total_rows = len(df)

    if total_rows == 0:
        print("No rows found.")
        return 0, 0

    required_columns = set(config["columns"])

    missing_columns = required_columns - set(df.columns)

    if missing_columns:
        raise ValueError(
            f"{table_name} is missing columns: "
            f"{sorted(missing_columns)}"
        )

    valid_count = int(df["is_valid"].fillna(False).sum())
    invalid_count = total_rows - valid_count

    print(f"Total rows   : {total_rows:,}")
    print(f"Valid rows   : {valid_count:,}")
    print(f"Invalid rows : {invalid_count:,}")

    # --------------------------------------------------------
    # Idempotency
    # --------------------------------------------------------
    #
    # raw_record_id uniquely identifies the RAW record that
    # produced this staging record.
    #
    # Before loading, check which raw_record_ids already exist.
    # This prevents rerunning the staging process from doubling
    # the staging data.
    # --------------------------------------------------------

    cursor.execute(
        f"""
        SELECT raw_record_id
        FROM {config["table"]}
        WHERE raw_record_id IS NOT NULL;
        """
    )

    existing_raw_ids = {
        row[0]
        for row in cursor.fetchall()
    }

    if existing_raw_ids:
        df = df[
            ~df["raw_record_id"].isin(existing_raw_ids)
        ].copy()

    new_rows = len(df)

    print(f"New rows     : {new_rows:,}")

    if new_rows == 0:
        print("Nothing new to stage.")
        return 0, invalid_count

    # --------------------------------------------------------
    # Insert ONLY valid rows into staging
    # --------------------------------------------------------

    valid_df = df[
        df["is_valid"].fillna(False)
    ].copy()

    columns = config["columns"]

    column_list = ", ".join(columns)

    insert_query = f"""
        INSERT INTO {config["table"]} (
            {column_list}
        )
        VALUES %s;
    """

    values = []

    for _, row in valid_df.iterrows():
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

    # --------------------------------------------------------
    # Write invalid records to audit.rejected_records
    # --------------------------------------------------------

    invalid_df = df[
        ~df["is_valid"].fillna(False)
    ].copy()

    rejected_values = []

    for _, row in invalid_df.iterrows():

        record_key = None

        for key_column in [
            "customer_id",
            "product_id",
            "order_id",
            "order_item_id",
        ]:
            if key_column in row.index:
                value = clean_value(row[key_column])

                if value is not None:
                    record_key = str(value)
                    break

        record_data = {
            column: clean_value(row[column])
            for column in row.index
        }

        rejected_values.append(
            (
                run_id,
                config["table"],
                record_key,
                str(row.get("validation_reason", "validation failed")),
                json.dumps(
                    record_data,
                    default=str,
                ),
            )
        )

    if rejected_values:

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

    # --------------------------------------------------------
    # Data quality result
    # --------------------------------------------------------

    dq_query = """
        INSERT INTO audit.data_quality_results (
            run_id,
            table_name,
            rule_name,
            records_checked,
            records_passed,
            records_failed
        )
        VALUES %s;
    """

    dq_values = [
        (
            run_id,
            table_name,
            "overall_validation",
            total_rows,
            valid_count,
            invalid_count,
        )
    ]

    execute_values(
        cursor,
        dq_query,
        dq_values,
    )

    staged_count = len(valid_df)

    print(f"Staged       : {staged_count:,}")
    print(f"Rejected     : {invalid_count:,}")

    return staged_count, invalid_count


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("STAGING LOAD")
    print("=" * 70)

    connection = get_connection()
    cursor = connection.cursor()

    run_id = None

    total_processed = 0
    total_failed = 0

    try:

        run_id = create_pipeline_run(cursor)

        print(f"Pipeline run ID: {run_id}")

        # ----------------------------------------------------
        # IMPORTANT:
        # Load in dependency order.
        # ----------------------------------------------------

        for table_name in [
            "customers",
            "products",
            "orders",
            "order_items",
        ]:

            config = TABLE_CONFIG[table_name]

            staged, failed = load_table(
                cursor,
                run_id,
                table_name,
                config,
            )

            total_processed += staged
            total_failed += failed

        finish_pipeline_run(
            cursor,
            run_id,
            "SUCCESS",
            total_processed,
            total_failed,
        )

        connection.commit()

        print()
        print("=" * 70)
        print("STAGING LOAD COMPLETED")
        print("=" * 70)
        print(f"Pipeline run ID : {run_id}")
        print(f"Records staged  : {total_processed:,}")
        print(f"Records rejected: {total_failed:,}")

    except Exception as error:

        connection.rollback()

        print()
        print("=" * 70)
        print("STAGING LOAD FAILED")
        print("=" * 70)
        print(str(error))

        if run_id is not None:
            finish_pipeline_run(
                cursor,
                run_id,
                "FAILED",
                total_processed,
                total_failed,
                str(error),
            )

            connection.commit()

        raise

    finally:

        cursor.close()
        connection.close()


if __name__ == "__main__":
    main()

