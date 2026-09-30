
-- ============================================================
-- PIPELINE RUNS
-- Tracks each execution of an ingestion/validation pipeline.
-- ============================================================

CREATE TABLE IF NOT EXISTS audit.pipeline_runs (
    run_id BIGSERIAL PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    status TEXT NOT NULL,
    records_processed INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT
);


-- ============================================================
-- REJECTED RECORDS
-- Stores records that fail validation and are rejected.
-- ============================================================

CREATE TABLE IF NOT EXISTS audit.rejected_records (
    rejection_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT,
    source_table TEXT NOT NULL,
    record_key TEXT,
    reason TEXT NOT NULL,
    record_data JSONB,
    rejected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_rejected_records_run
        FOREIGN KEY (run_id)
        REFERENCES audit.pipeline_runs(run_id)
);


-- ============================================================
-- DATA QUALITY RESULTS
-- Stores pass/fail results for individual validation rules.
-- ============================================================

CREATE TABLE IF NOT EXISTS audit.data_quality_results (
    result_id BIGSERIAL PRIMARY KEY,
    run_id BIGINT,
    table_name TEXT NOT NULL,
    rule_name TEXT NOT NULL,
    records_checked INTEGER DEFAULT 0,
    records_passed INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    executed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_dq_results_run
        FOREIGN KEY (run_id)
        REFERENCES audit.pipeline_runs(run_id)
);


-- ============================================================
-- CHANGE LOG
-- Stores detected changes between record versions.
-- Will later support CDC/SCD requirements.
-- ============================================================

CREATE TABLE IF NOT EXISTS audit.change_log (
    change_id BIGSERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    record_id TEXT NOT NULL,
    operation TEXT NOT NULL,
    changed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    old_value JSONB,
    new_value JSONB,
    source TEXT
);

-- ============================================================
-- INDEXES
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline_name
    ON audit.pipeline_runs(pipeline_name);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at
    ON audit.pipeline_runs(started_at);

CREATE INDEX IF NOT EXISTS idx_rejected_records_run_id
    ON audit.rejected_records(run_id);

CREATE INDEX IF NOT EXISTS idx_rejected_records_source_table
    ON audit.rejected_records(source_table);

CREATE INDEX IF NOT EXISTS idx_dq_results_run_id
    ON audit.data_quality_results(run_id);

CREATE INDEX IF NOT EXISTS idx_dq_results_table_name
    ON audit.data_quality_results(table_name);

CREATE INDEX IF NOT EXISTS idx_change_log_table_record
    ON audit.change_log(table_name, record_id);

CREATE INDEX IF NOT EXISTS idx_change_log_changed_at
    ON audit.change_log(changed_at);
