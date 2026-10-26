import json
import os
import getpass
from datetime import date, datetime
from decimal import Decimal

import psycopg2
from kafka import KafkaProducer


# --------------------------------------------------
# 1. Configuration
# --------------------------------------------------

PG_HOST = "localhost"
PG_PORT = 5433
PG_DATABASE = "appdb"
PG_USER = "postgres"

# Kafka's host-accessible listener.
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"

# Staging table -> Kafka topic -> message key
TABLES = {
    "stg_customers": (
        "customer.clean.customers",
        "customer_id",
    ),
    "stg_products": (
        "customer.clean.products",
        "product_id",
    ),
    "stg_orders": (
        "customer.clean.orders",
        "order_id",
    ),
    "stg_order_items": (
        "customer.clean.order_items",
        "order_item_id",
    ),
}

BATCH_SIZE = 500


# --------------------------------------------------
# 2. Convert PostgreSQL values to JSON-compatible data
# --------------------------------------------------

def json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()

    if isinstance(value, Decimal):
        # Preserve decimal precision in the JSON representation.
        return str(value)

    raise TypeError(
        f"Cannot serialize {type(value).__name__} to JSON"
    )


# --------------------------------------------------
# 3. Connect to PostgreSQL and Kafka
# --------------------------------------------------

def main():
    password = os.getenv("PGPASSWORD")

    if not password:
        password = getpass.getpass(
            "PostgreSQL password for user postgres: "
        )

    db = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DATABASE,
        user=PG_USER,
        password=password,
    )

    producer = None

    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            key_serializer=lambda key: key.encode("utf-8"),
            value_serializer=lambda value: json.dumps(
                value,
                default=json_default,
                ensure_ascii=False,
            ).encode("utf-8"),
            acks="all",
            retries=5,
        )

        print("Connected to PostgreSQL and Kafka.")

        # --------------------------------------------------
        # 4. Read valid rows from each staging table
        # --------------------------------------------------

        for table_name, (topic_name, key_column) in TABLES.items():
            print(f"\nReading staging.{table_name}")
            print(f"Publishing to {topic_name}")

            sent = 0
            pending = []

            # Named cursor streams results in batches instead
            # of loading the entire table into memory.
            with db.cursor() as metadata_cursor:
                metadata_cursor.execute(
                     f"""
                            SELECT *
                            FROM staging.{table_name}
                            WHERE is_valid IS TRUE
                            LIMIT 0
                            """
                        )
                columns = [
                        description[0]
                        for description in metadata_cursor.description
                    ]
            with db.cursor(name=f"cursor_{table_name}") as cursor:
                cursor.itersize = BATCH_SIZE
                cursor.execute(
                        f"""
                        SELECT *
                        FROM staging.{table_name}
                        WHERE is_valid IS TRUE
                        ORDER BY staging_record_id
                        """
                    )

# Stream the actual valid records in batches.


                if key_column not in columns:
                    raise RuntimeError(
                        f"{key_column} not found in "
                        f"staging.{table_name}"
                    )

                while True:
                    rows = cursor.fetchmany(BATCH_SIZE)

                    if not rows:
                        break

                    for row in rows:
                        record = dict(zip(columns, row))

                        # These are validation metadata, not
                        # business fields for the clean topic.
                        record.pop("validation_reason", None)
                        record.pop("is_valid", None)

                        key_value = record.get(key_column)

                        if key_value is None:
                            raise RuntimeError(
                                f"Null Kafka key {key_column} "
                                f"in staging.{table_name}"
                            )

                        future = producer.send(
                            topic_name,
                            key=str(key_value),
                            value=record,
                        )
                        pending.append(future)

                    # Wait for each batch to be acknowledged
                    # before moving to the next batch.
                    for future in pending:
                        future.get(timeout=60)

                    sent += len(pending)
                    pending.clear()

                    print(f"  Published {sent} records", end="\r")

            print(f"  Finished: {sent} records published.")

        producer.flush()
        print("\nAll staging tables processed successfully.")

    finally:
        if producer is not None:
            producer.close()

        db.close()


if __name__ == "__main__":
    main()
