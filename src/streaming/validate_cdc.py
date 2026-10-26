"""Continuous customer CDC validator based on src/staging/validate_stage.py.

Customer rules copied from load_customers():
- remove exact full-row duplicates before normalization (lowest raw_record_id wins)
- trim customer_id; blank becomes NULL
- lowercase trimmed segment
- uppercase trimmed source_system
- only validation rule: customer_id must not be NULL
- invalid rows are retained in staging and written to audit.rejected_records
"""
import json
import logging
import os
import time

import psycopg2
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "customer.raw.customers")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "customer-cdc-validator")
DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5433")),
    "dbname": os.getenv("POSTGRES_DB", "appdb"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"),
                    format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("validate_cdc")

CUSTOMER_FIELDS = (
    "customer_id", "first_name", "last_name", "email", "phone", "address",
    "city", "country", "segment", "birthdate", "created_at", "updated_at",
    "source_system",
)


def normalize_text(value):
    if value is None:
        return None
    value = str(value).strip()
    return value or None


def normalize_lower(value):
    value = normalize_text(value)
    return value.lower() if value is not None else None


def normalize_upper(value):
    value = normalize_text(value)
    return value.upper() if value is not None else None


def validate_customer(raw_record):
    """Apply the exact customer validation rules in validate_stage.py."""
    record = dict(raw_record)
    record["customer_id"] = normalize_text(record.get("customer_id"))
    record["segment"] = normalize_lower(record.get("segment"))
    record["source_system"] = normalize_upper(record.get("source_system"))
    reasons = []
    if record["customer_id"] is None:
        reasons.append("missing customer_id")
    record["is_valid"] = not reasons
    record["validation_reason"] = "; ".join(reasons) if reasons else None
    return record


def create_pipeline_run(conn):
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO audit.pipeline_runs (pipeline_name, status) "
            "VALUES (%s, %s) RETURNING run_id",
            ("validate_cdc_customers", "RUNNING"),
        )
        run_id = cur.fetchone()[0]
    conn.commit()
    return run_id


def update_pipeline_run(conn, run_id, processed, failed, status="RUNNING", error=None):
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE audit.pipeline_runs
               SET status=%s, records_processed=%s, records_failed=%s,
                   error_message=%s,
                   finished_at=CASE WHEN %s IN ('SUCCESS','FAILED')
                                    THEN CURRENT_TIMESTAMP ELSE finished_at END
               WHERE run_id=%s""",
            (status, processed, failed, error, status, run_id),
        )
    conn.commit()


def insert_rejection(conn, run_id, record):
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO audit.rejected_records
               (run_id, source_table, record_key, reason, record_data)
               VALUES (%s, %s, %s, %s, %s::jsonb)""",
            (run_id, "raw.customers", record.get("customer_id"),
             record["validation_reason"], json.dumps(record, default=str)),
        )


def decode_json(value):
    if value is None:
        return None
    if isinstance(value, bytes):
        return json.loads(value.decode("utf-8"))
    if isinstance(value, str):
        return json.loads(value)
    return value


def is_full_duplicate(conn, record, raw_record_id):
    """Match batch duplicate key using original (pre-normalized) field values."""
    comparisons = " AND ".join(f"{name} IS NOT DISTINCT FROM %s" for name in CUSTOMER_FIELDS)
    with conn.cursor() as cur:
        cur.execute(
            f"""SELECT EXISTS (
                    SELECT 1 FROM raw.customers
                    WHERE raw_record_id < %s AND {comparisons}
                )""",
            [raw_record_id, *(record.get(name) for name in CUSTOMER_FIELDS)],
        )
        return cur.fetchone()[0]


def upsert_staging(conn, record, raw_record_id):
    fields = (
        "customer_id", "first_name", "last_name", "email", "phone", "address",
        "city", "country", "segment", "birthdate", "created_at", "updated_at",
        "source_system", "is_valid", "validation_reason",
    )
    values = [record.get(name) for name in fields]
    with conn.cursor() as cur:
        cur.execute(
            """UPDATE staging.stg_customers SET
                 customer_id=%s, first_name=%s, last_name=%s, email=%s,
                 phone=%s, address=%s, city=%s, country=%s, segment=%s,
                 birthdate=%s, created_at=%s, updated_at=%s, source_system=%s,
                 is_valid=%s, validation_reason=%s, staged_at=CURRENT_TIMESTAMP
               WHERE raw_record_id=%s""",
            [*values, raw_record_id],
        )
        if cur.rowcount == 0:
            cur.execute(
                """INSERT INTO staging.stg_customers
                   (raw_record_id, customer_id, first_name, last_name, email,
                    phone, address, city, country, segment, birthdate, created_at,
                    updated_at, source_system, is_valid, validation_reason)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                           %s, %s, %s, %s)""",
                [raw_record_id, *values],
            )


def process_message(conn, run_id, message):
    event = decode_json(message.value)
    if not isinstance(event, dict):
        raise ValueError("Kafka message value is not a JSON object")
    payload = event.get("payload", event)
    op = payload.get("op")
    if op not in ("c", "u", "r", "d"):
        return "skipped"

    key = decode_json(message.key)
    before = payload.get("before") or {}
    after = payload.get("after") or {}

    if op == "d":
        raw_id = before.get("raw_record_id")
        if raw_id is None and isinstance(key, dict):
            raw_id = key.get("raw_record_id")
        if raw_id is None:
            raise ValueError("Delete event lacks raw_record_id in before/key")
        with conn.cursor() as cur:
            cur.execute("DELETE FROM staging.stg_customers WHERE raw_record_id=%s", (raw_id,))
        conn.commit()
        log.info("Deleted staging row raw_record_id=%s", raw_id)
        return "processed"

    raw_id = after.get("raw_record_id")
    if raw_id is None and isinstance(key, dict):
        raw_id = key.get("raw_record_id")
    if raw_id is None:
        raise ValueError("Event lacks raw_record_id in after/key; inspect Debezium key")

    # Batch script skips a full duplicate before normalizing or validating it.
    if is_full_duplicate(conn, after, raw_id):
        with conn.cursor() as cur:
            cur.execute("DELETE FROM staging.stg_customers WHERE raw_record_id=%s", (raw_id,))
        conn.commit()
        log.info("Skipped duplicate raw_record_id=%s", raw_id)
        return "processed"

    record = validate_customer(after)
    upsert_staging(conn, record, raw_id)
    if not record["is_valid"]:
        insert_rejection(conn, run_id, record)
    conn.commit()
    log.info("raw_record_id=%s customer_id=%r valid=%s reason=%r",
             raw_id, record["customer_id"], record["is_valid"], record["validation_reason"])
    return "processed"


def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id=KAFKA_GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    conn = None
    run_id = None
    processed = failed = 0
    try:
        while True:
            try:
                if conn is None or conn.closed:
                    conn = psycopg2.connect(**DB_CONFIG)
                    conn.autocommit = False
                    run_id = create_pipeline_run(conn)
                    log.info("Connected to PostgreSQL; run_id=%s", run_id)
                log.info("Listening to %s at %s", KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS)
                for message in consumer:
                    try:
                        result = process_message(conn, run_id, message)
                        if result == "processed":
                            processed += 1
                        update_pipeline_run(conn, run_id, processed, failed)
                        consumer.commit()  # only after PostgreSQL commit
                    except Exception as exc:
                        if conn and not conn.closed:
                            conn.rollback()
                        failed += 1
                        log.exception("Failed topic=%s partition=%s offset=%s",
                                      message.topic, message.partition, message.offset)
                        if conn and not conn.closed:
                            try:
                                update_pipeline_run(conn, run_id, processed, failed,
                                                    status="FAILED", error=str(exc)[:4000])
                            except Exception:
                                conn.rollback()
                        raise  # don't silently skip the failed Kafka event
            except Exception:
                log.exception("Validator failed; reconnecting in 5 seconds")
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
                conn = None
                run_id = None
                time.sleep(5)
    finally:
        consumer.close()
        if conn and not conn.closed:
            try:
                if run_id is not None:
                    update_pipeline_run(conn, run_id, processed, failed, status="SUCCESS")
            except Exception:
                conn.rollback()
            conn.close()


if __name__ == "__main__":
    main()
