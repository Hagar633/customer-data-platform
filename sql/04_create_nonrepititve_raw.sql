
CREATE TABLE IF NOT EXISTS audit.ingestion_records (

    ingestion_record_id BIGSERIAL PRIMARY KEY,

    run_id BIGINT,

    source_file TEXT NOT NULL,

    file_hash TEXT NOT NULL,

    source_row_number BIGINT NOT NULL,

    target_table TEXT NOT NULL,

    raw_record_id BIGINT,

    ingested_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_ingestion_run
        FOREIGN KEY (run_id)
        REFERENCES audit.pipeline_runs(run_id),

    CONSTRAINT uq_source_record
        UNIQUE (
            source_file,
            file_hash,
            source_row_number,
            target_table
        )
);


CREATE INDEX IF NOT EXISTS idx_ingestion_source_file
    ON audit.ingestion_records(source_file);


CREATE INDEX IF NOT EXISTS idx_ingestion_file_hash
    ON audit.ingestion_records(file_hash);


CREATE INDEX IF NOT EXISTS idx_ingestion_target_table
    ON audit.ingestion_records(target_table);


CREATE INDEX IF NOT EXISTS idx_ingestion_raw_record
    ON audit.ingestion_records(raw_record_id);
