CREATE TABLE export_jobs (
    id UUID PRIMARY KEY,
    status VARCHAR(20) NOT NULL,
    total_rows BIGINT DEFAULT 0,
    processed_rows BIGINT DEFAULT 0,
    error TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE
);