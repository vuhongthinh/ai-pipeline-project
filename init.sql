CREATE TABLE IF NOT EXISTS metadata (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR(255) NOT NULL,
    file_url TEXT NOT NULL,
    labels JSONB,
    confidence FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);