-- ================================================================
--  DATA WAREHOUSE - TABLES ANALYTIQUES (Star Schema)
--  NEWS PLATFORM
-- ================================================================

-- ----------------------------------------------------------------
--  DIMENSION : Sources
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_sources (
    id          SERIAL PRIMARY KEY,
    source_name VARCHAR(100) UNIQUE NOT NULL,
    country     VARCHAR(10),
    language    VARCHAR(10),
    created_at  TIMESTAMP DEFAULT NOW()
);

-- ----------------------------------------------------------------
--  DIMENSION : Dates
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_dates (
    id           SERIAL PRIMARY KEY,
    full_date    DATE UNIQUE NOT NULL,
    day          INT,
    month        INT,
    year         INT,
    day_of_week  VARCHAR(20),
    week_number  INT
);

-- ----------------------------------------------------------------
--  DIMENSION : Catégories
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_categories (
    id            SERIAL PRIMARY KEY,
    category_name VARCHAR(100) UNIQUE NOT NULL
);

-- ----------------------------------------------------------------
--  TABLE DE FAITS : Articles
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_articles (
    id            SERIAL PRIMARY KEY,
    article_uuid  VARCHAR(36) UNIQUE NOT NULL,
    title         TEXT NOT NULL,
    url           TEXT,
    author        VARCHAR(255),
    content       TEXT,
    word_count    INT,
    language      VARCHAR(10),
    source_id     INT REFERENCES dim_sources(id),
    date_id       INT REFERENCES dim_dates(id),
    category_id   INT REFERENCES dim_categories(id),
    published_at  TIMESTAMP,
    ingested_at   TIMESTAMP DEFAULT NOW()
);

-- ----------------------------------------------------------------
--  TABLE ANALYTIQUE : Articles par jour
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics_articles_per_day (
    id           SERIAL PRIMARY KEY,
    report_date  DATE NOT NULL,
    source_name  VARCHAR(100),
    total        INT DEFAULT 0,
    updated_at   TIMESTAMP DEFAULT NOW(),
    UNIQUE(report_date, source_name)
);

-- ----------------------------------------------------------------
--  TABLE ANALYTIQUE : Top mots-clés
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics_top_keywords (
    id           SERIAL PRIMARY KEY,
    report_date  DATE NOT NULL,
    keyword      VARCHAR(200),
    frequency    INT DEFAULT 0,
    source_name  VARCHAR(100),
    updated_at   TIMESTAMP DEFAULT NOW()
);

-- ----------------------------------------------------------------
--  TABLE ANALYTIQUE : Tendances par thème
-- ----------------------------------------------------------------
CREATE TABLE IF NOT EXISTS analytics_trends (
    id            SERIAL PRIMARY KEY,
    report_date   DATE NOT NULL,
    category      VARCHAR(100),
    article_count INT DEFAULT 0,
    source_name   VARCHAR(100),
    updated_at    TIMESTAMP DEFAULT NOW()
);

-- ----------------------------------------------------------------
--  DONNÉES INITIALES
-- ----------------------------------------------------------------
INSERT INTO dim_sources (source_name, country, language) VALUES
    ('hespress',     'MA', 'ar'),
    ('barlamane',    'MA', 'ar'),
    ('aljazeera_ar', 'QA', 'ar'),
    ('bbc_arabic',   'GB', 'ar'),
    ('reuters',      'US', 'en')
ON CONFLICT (source_name) DO NOTHING;

INSERT INTO dim_categories (category_name) VALUES
    ('politique'), ('économie'), ('sport'), ('culture'),
    ('technologie'), ('santé'), ('international'), ('general')
ON CONFLICT (category_name) DO NOTHING;

-- Remplir la dimension dates pour 2024-2026
INSERT INTO dim_dates (full_date, day, month, year, day_of_week, week_number)
SELECT
    d::date,
    EXTRACT(DAY FROM d)::int,
    EXTRACT(MONTH FROM d)::int,
    EXTRACT(YEAR FROM d)::int,
    TO_CHAR(d, 'Day'),
    EXTRACT(WEEK FROM d)::int
FROM generate_series('2024-01-01'::date, '2026-12-31'::date, '1 day'::interval) AS d
ON CONFLICT (full_date) DO NOTHING;

-- ----------------------------------------------------------------
--  VUE : résumé pour Grafana
-- ----------------------------------------------------------------
CREATE OR REPLACE VIEW v_articles_summary AS
SELECT
    dd.full_date,
    ds.source_name,
    ds.country,
    COUNT(fa.id)        AS total_articles,
    AVG(fa.word_count)  AS avg_word_count
FROM fact_articles fa
JOIN dim_sources ds ON fa.source_id = ds.id
JOIN dim_dates   dd ON fa.date_id   = dd.id
GROUP BY dd.full_date, ds.source_name, ds.country;
