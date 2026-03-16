--  Runs automatically on first Postgres start

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

-- Indexes for dbt query performance
CREATE INDEX IF NOT EXISTS idx_raw_trends_keyword_date   ON raw_trends (keyword, date);
CREATE INDEX IF NOT EXISTS idx_raw_articles_keyword_date ON raw_articles (keyword, published_at);