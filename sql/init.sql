---script that runs automatically on first Postgres start to init the database with the necessary tables for raw data storage.---

CREATE TABLE IF NOT EXISTS raw_trends (
    id           SERIAL PRIMARY KEY,
    keyword      TEXT        NOT NULL,
    date         DATE        NOT NULL,
    interest     INT,
    extracted_at TIMESTAMP   DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_articles (
    id           SERIAL PRIMARY KEY,
    keyword      TEXT        NOT NULL,
    title        TEXT,
    source_name  TEXT,
    published_at TIMESTAMP,
    url          TEXT,
    description  TEXT,
    extracted_at TIMESTAMP   DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tracked_keywords (
    keyword     TEXT PRIMARY KEY,
    added_by    TEXT DEFAULT 'admin',
    added_at    TIMESTAMP DEFAULT NOW(),
    active      BOOLEAN DEFAULT TRUE
);

-- New feat for UI consistency -- pre populate with current keywords so the table is not empty 
INSERT INTO tracked_keywords (keyword) VALUES
    ('AI marketing'),
    ('content marketing'),
    ('SEO'),
    ('social commerce')
ON CONFLICT DO NOTHING;

-- Indexes for dbt query performance
CREATE INDEX IF NOT EXISTS idx_raw_trends_keyword_date   ON raw_trends (keyword, date);
CREATE INDEX IF NOT EXISTS idx_raw_articles_keyword_date ON raw_articles (keyword, published_at);


-- Create separate databases : fix metabase too many tables 
CREATE DATABASE airflow_db;
CREATE DATABASE metabase_db;