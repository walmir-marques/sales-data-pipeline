CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.pipeline_runs (
    id          SERIAL PRIMARY KEY,
    run_at      TIMESTAMP DEFAULT NOW(),
    layer       VARCHAR(20),
    status      VARCHAR(20),
    rows_loaded INTEGER,
    message     TEXT
);