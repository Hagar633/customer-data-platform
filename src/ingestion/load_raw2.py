from pathlib import Path
import hashlib
import os
from datetime import datetime

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


# ============================================================
# DATABASE CONFIGURATION
# ============================================================

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5433"),
    "dbname": os.getenv("POSTGRES_DB", "appdb"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}


# ============================================================
# SOURCE → RAW TABLE MAPPING
# ============================================================

TABLE_CONFIG = {

    "customers_with_history.parquet": {
        "table": "raw.customers",

        "columns": [
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
        ],
    },

    "products_with_history.parquet": {
        "table": "raw.products",

        "columns": [
            "product_id",
            "product_name",
            "category",
            "price",
            "currency",
            "stock_quantity",
            "supplier",
            "created_at",
            "updated_at",
        ],
    },

    "orders_with_history.parquet": {
        "table": "raw.orders",

        "columns": [
            "order_id",
            "customer_id",
            "order_date",
            "status",
            "payment_method",
            "updated_at",
        ],
    },

    "orderItems_with_history.parquet": {
        "table": "raw.order_items",

        "columns": [
            "order_item_id",
            "order_id",
            "product_id",
            "quantity",
            "unit_price",
            "discount_pct",
        ],
    },
}


# ============================================================
# DATETIME COLUMNS
# ============================================================

DATETIME_COLUMNS = {
    "birthdate",
    "created_at",
    "updated_at",
    "order_date",
}


# ============================================================
# HELPER: CONVERT PANDAS NULLS
# ============================================================

def convert_nulls(value):
    """
    Convert Pandas NaN/NaT values to Python None.

    PostgreSQL will store None as SQL NULL.
    """

    if pd.isna(value):
        return None

    return value


# ============================================================
# HELPER: PARSE SOURCE DATETIME
# ============================================================

def parse_source_datetime(value):
    """
    Convert the mixed datetime formats used by the source
    Parquet files into Python datetime objects.

    Supported formats:

        YYYY-MM-DD
        YYYY-MM-DDTHH:MM:SS
        YYYY-MM-DDTHH:MM:SS.ssssss
        DD/MM/YYYY
        MM-DD-YYYY
        DD Mon YYYY
        DD Month YYYY

    This is type conversion for RAW ingestion.
    It is NOT data-quality validation.
    """

    if value is None:
        return None

    if pd.isna(value):
        return None

    if isinstance(value, datetime):
        return value

    text = str(value).strip()

    if not text:
        return None

    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m-%d-%Y",
        "%d %b %Y",
        "%d %B %Y",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue

    raise ValueError(
        f"Unable to parse source datetime: {text}"
    )


# ============================================================
# HELPER: CONVERT SOURCE VALUE TO DATABASE VALUE
# ============================================================

def convert_source_value(column, value):
    """
    Convert a source value into a PostgreSQL-compatible value.

    Datetime columns are parsed into Python datetime objects.
    All other columns only have Pandas NULLs converted to None.

    No business validation is performed here.
    """

    if column in DATETIME_COLUMNS:
        return parse_source_datetime(value)

    return convert_nulls(value)


# ============================================================
# HELPER: FILE HASH
# ============================================================

def calculate_file_hash(file_path):
    """
    Calculate a SHA-256 hash for the complete source file.

    The hash identifies the exact version of the file.

    Same file:
        same hash

    Changed file:
        different hash
    """

    sha256 = hashlib.sha256()

    with open(file_path, "rb") as file:

        while True:

            chunk = file.read(1024 * 1024)

            if not chunk:
                break

            sha256.update(chunk)

    return sha256.hexdigest()


# ============================================================
# AUDIT: CREATE PIPELINE RUN
# ============================================================

def create_pipeline_run(connection):
    """
    Create a RUNNING record in audit.pipeline_runs.

    We commit this immediately so that if the ingestion fails,
    the FAILED run can still remain in the audit table.
    """

    cursor = connection.cursor()

    query = """
        INSERT INTO audit.pipeline_runs (
            pipeline_name,
            status
        )
        VALUES (%s, %s)
        RETURNING run_id;
    """

    cursor.execute(
        query,
        (
            "raw_ingestion",
            "RUNNING",
        ),
    )

    run_id = cursor.fetchone()[0]

    connection.commit()

    cursor.close()

    return run_id


# ============================================================
# AUDIT: FINISH PIPELINE RUN
# ============================================================

def finish_pipeline_run(
    connection,
    run_id,
    status,
    records_processed,
    records_failed=0,
    error_message=None,
):
    """
    Update the pipeline run after completion.
    """

    cursor = connection.cursor()

    query = """
        UPDATE audit.pipeline_runs
        SET
            finished_at = CURRENT_TIMESTAMP,
            status = %s,
            records_processed = %s,
            records_failed = %s,
            error_message = %s
        WHERE run_id = %s;
    """

    cursor.execute(
        query,
        (
            status,
            records_processed,
            records_failed,
            error_message,
            run_id,
        ),
    )

    connection.commit()

    cursor.close()


# ============================================================
# FIND ALREADY INGESTED SOURCE ROWS
# ============================================================

def get_existing_source_rows(
    cursor,
    source_file,
    file_hash,
    target_table,
):
    """
    Find source row numbers that have already been loaded.

    IMPORTANT:

    We do NOT use customer_id/order_id/etc.

    We use:

        source_file
        file_hash
        source_row_number
        target_table

    This allows repeated business IDs and historical versions.
    """

    query = """
        SELECT source_row_number
        FROM audit.ingestion_records
        WHERE source_file = %s
          AND file_hash = %s
          AND target_table = %s;
    """

    cursor.execute(
        query,
        (
            source_file,
            file_hash,
            target_table,
        ),
    )

    return {
        row[0]
        for row in cursor.fetchall()
    }


# ============================================================
# LOAD ONE FILE
# ============================================================

def load_file(
    connection,
    dataframe,
    source_file,
    file_hash,
    target_table,
    columns,
    run_id,
):
    """
    Load only source rows that have not already been ingested.

    Returns:
        number of newly inserted rows
    """

    cursor = connection.cursor()

    # --------------------------------------------------------
    # Find rows already loaded
    # --------------------------------------------------------

    existing_rows = get_existing_source_rows(
        cursor=cursor,
        source_file=source_file,
        file_hash=file_hash,
        target_table=target_table,
    )

    # --------------------------------------------------------
    # Prepare new rows
    # --------------------------------------------------------

    new_rows = []

    for source_row_number, row in enumerate(
        dataframe[columns].itertuples(
            index=False,
            name=None,
        ),
        start=1,
    ):

        # Already loaded → skip
        if source_row_number in existing_rows:
            continue

        converted_row = tuple(
            convert_source_value(column, value)
            for column, value in zip(columns, row)
        )

        new_rows.append(
            (
                source_row_number,
                converted_row,
            )
        )

    # --------------------------------------------------------
    # Nothing new
    # --------------------------------------------------------

    if not new_rows:

        cursor.close()

        return 0

    # --------------------------------------------------------
    # Insert RAW records
    # --------------------------------------------------------

    column_list = ", ".join(columns)

    insert_query = f"""
        INSERT INTO {target_table} (
            {column_list}
        )
        VALUES %s
        RETURNING raw_record_id;
    """

    raw_values = [
        row
        for _, row in new_rows
    ]

    inserted_raw_ids = execute_values(
        cursor,
        insert_query,
        raw_values,
        page_size=1000,
        fetch=True,
    )

    inserted_raw_ids = [
        row[0]
        for row in inserted_raw_ids
    ]

    if len(inserted_raw_ids) != len(new_rows):
        raise RuntimeError(
            f"Inserted {len(inserted_raw_ids)} RAW rows "
            f"but expected {len(new_rows)}"
        )

    # --------------------------------------------------------
    # Create ingestion manifest records
    # --------------------------------------------------------

    manifest_values = []

    for (
        (source_row_number, _),
        raw_record_id,
    ) in zip(
        new_rows,
        inserted_raw_ids,
    ):

        manifest_values.append(
            (
                run_id,
                source_file,
                file_hash,
                source_row_number,
                target_table,
                raw_record_id,
            )
        )

    manifest_query = """
        INSERT INTO audit.ingestion_records (
            run_id,
            source_file,
            file_hash,
            source_row_number,
            target_table,
            raw_record_id
        )
        VALUES %s;
    """

    execute_values(
        cursor,
        manifest_query,
        manifest_values,
        page_size=1000,
    )

    cursor.close()

    return len(new_rows)


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 70)
    print("STARTING RAW INGESTION")
    print("=" * 70)

    connection = None
    run_id = None
    total_processed = 0

    try:

        # ----------------------------------------------------
        # CONNECT TO POSTGRES
        # ----------------------------------------------------

        connection = psycopg2.connect(**DB_CONFIG)

        print("Connected to PostgreSQL.")

        # ----------------------------------------------------
        # CREATE AUDIT RUN
        # ----------------------------------------------------

        run_id = create_pipeline_run(
            connection
        )

        print(f"Pipeline run ID: {run_id}")

        # ----------------------------------------------------
        # PROCESS ALL SOURCE FILES
        # ----------------------------------------------------

        for filename, config in TABLE_CONFIG.items():

            print()
            print("-" * 70)
            print(f"Processing: {filename}")
            print("-" * 70)

            file_path = RAW_DATA_DIR / filename

            # ------------------------------------------------
            # Check file exists
            # ------------------------------------------------

            if not file_path.exists():

                raise FileNotFoundError(
                    f"Source file not found: {file_path}"
                )

            # ------------------------------------------------
            # Read Parquet
            # ------------------------------------------------

            dataframe = pd.read_parquet(
                file_path
            )

            print(
                f"Rows read: {len(dataframe):,}"
            )

            # ------------------------------------------------
            # Calculate exact file hash
            # ------------------------------------------------

            file_hash = calculate_file_hash(
                file_path
            )

            print(
                f"File hash: {file_hash[:16]}..."
            )

            # ------------------------------------------------
            # Load only new rows
            # ------------------------------------------------

            loaded = load_file(
                connection=connection,
                dataframe=dataframe,
                source_file=filename,
                file_hash=file_hash,
                target_table=config["table"],
                columns=config["columns"],
                run_id=run_id,
            )

            total_processed += loaded

            print(
                f"New rows loaded: {loaded:,}"
            )

            if loaded == 0:

                print(
                    "No new rows. "
                    "This source version was already ingested."
                )

        # ----------------------------------------------------
        # COMMIT RAW INGESTION
        # ----------------------------------------------------

        connection.commit()

        # ----------------------------------------------------
        # MARK SUCCESS
        # ----------------------------------------------------

        finish_pipeline_run(
            connection=connection,
            run_id=run_id,
            status="SUCCESS",
            records_processed=total_processed,
        )

        print()
        print("=" * 70)
        print("RAW INGESTION COMPLETED SUCCESSFULLY")
        print("=" * 70)

        print(
            f"Total new records loaded: "
            f"{total_processed:,}"
        )

    except Exception as error:

        print()
        print("=" * 70)
        print("RAW INGESTION FAILED")
        print("=" * 70)

        print(f"Error: {error}")

        # ----------------------------------------------------
        # Roll back RAW transaction
        # ----------------------------------------------------

        if connection:

            connection.rollback()

        # ----------------------------------------------------
        # Preserve FAILED audit record
        # ----------------------------------------------------

        if connection and run_id is not None:

            finish_pipeline_run(
                connection=connection,
                run_id=run_id,
                status="FAILED",
                records_processed=0,
                records_failed=1,
                error_message=str(error),
            )

        raise

    finally:

        if connection:

            connection.close()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    main()

