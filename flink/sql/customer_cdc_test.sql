SET sql-client.execution.result-mode=TABLEAU;

CREATE TABLE customer_cdc (
    payload ROW<
        before ROW<
            raw_record_id BIGINT,
            customer_id STRING,
            segment STRING
        >,
        after ROW<
            raw_record_id BIGINT,
            customer_id STRING,
            first_name STRING,
            last_name STRING,
            email STRING,
            phone STRING,
            address STRING,
            city STRING,
            country STRING,
            segment STRING,
            birthdate BIGINT,
            created_at BIGINT,
            updated_at BIGINT,
            source_system STRING,
            ingested_at BIGINT
        >,
        source ROW<
            version STRING,
            connector STRING,
            name STRING,
            ts_ms BIGINT,
            snapshot STRING,
            db STRING,
            sequence STRING,
            ts_us BIGINT,
            ts_ns BIGINT,
            schema STRING,
            table_name STRING,
            txId BIGINT,
            lsn BIGINT,
            xmin BIGINT,
            origin STRING,
            origin_lsn BIGINT
        >,
        transaction ROW<
            id STRING,
            total_order BIGINT,
            data_collection_order BIGINT
        >,
        op STRING,
        ts_ms BIGINT,
        ts_us BIGINT,
        ts_ns BIGINT
    >
) WITH (
    'connector' = 'kafka',
    'topic' = 'customer.raw.customers',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-customer-cdc-test-plain-json',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'false'
);

SELECT
    payload.op AS operation,
    payload.after.customer_id AS customer_id,
    payload.after.segment AS current_segment,
    payload.before.segment AS previous_segment,
    payload.ts_ms AS event_timestamp
FROM customer_cdc;


