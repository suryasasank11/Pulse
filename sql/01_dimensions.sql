-- ============================================================
-- 01_dimensions.sql
-- The six dimension tables of the star schema (gold / MARTS).
--
-- NOTE on CREATE OR REPLACE: each run drops and recreates the
-- table (and any data in it). That is exactly what we want while
-- we are still shaping the schema and before real data is loaded.
-- Once dbt owns these tables (later phase), dbt manages them.
--
-- NOTE on keys: every dimension has a surrogate primary key.
--   * dim_date uses a "smart" integer key in YYYYMMDD form.
--   * Every other dim uses a 32-char hash of its natural key,
--     generated downstream by dbt. The DDL just declares the
--     column; it does not generate the value.
-- ============================================================


USE ROLE SYSADMIN;
USE WAREHOUSE AI_JOBS_WH;
USE SCHEMA AI_JOBS.MARTS;
USE SCHEMA AI_JOBS.MARTS;

-- ---------- dim_date ----------
-- Static calendar reference data. date_key is YYYYMMDD
-- (e.g. 20260531): human-readable, naturally sortable, and
-- stable forever, so it is the one key we do not hash.
CREATE OR REPLACE TABLE dim_date (
    date_key      NUMBER(8,0)  NOT NULL,   -- YYYYMMDD
    full_date     DATE         NOT NULL,
    year          NUMBER(4,0)  NOT NULL,
    quarter       NUMBER(1,0)  NOT NULL,   -- 1..4
    month         NUMBER(2,0)  NOT NULL,   -- 1..12
    month_name    VARCHAR(9)   NOT NULL,   -- short name, e.g. 'May'
    week_of_year  NUMBER(2,0)  NOT NULL,   -- ISO week 1..53
    day_of_week   NUMBER(1,0)  NOT NULL,   -- ISO: 1=Mon .. 7=Sun
    day_name      VARCHAR(9)   NOT NULL,   -- short name, e.g. 'Sat'
    is_weekend    BOOLEAN      NOT NULL,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
)
COMMENT = 'Calendar dimension';

-- ---------- dim_company ----------
CREATE OR REPLACE TABLE dim_company (
    company_key   VARCHAR(32)  NOT NULL,   -- hashed surrogate (set by dbt)
    company_name  VARCHAR      NOT NULL,   -- natural key
    industry      VARCHAR,
    company_size  VARCHAR,                 -- e.g. '1-50', '51-200'
    is_startup    BOOLEAN,
    CONSTRAINT pk_dim_company PRIMARY KEY (company_key)
)
COMMENT = 'Hiring companies';

-- ---------- dim_location ----------
CREATE OR REPLACE TABLE dim_location (
    location_key  VARCHAR(32)  NOT NULL,
    city          VARCHAR,
    state         VARCHAR,                 -- 2-letter US state where known
    country       VARCHAR,
    region        VARCHAR,                 -- e.g. 'West', 'Northeast'
    is_remote     BOOLEAN      NOT NULL,
    CONSTRAINT pk_dim_location PRIMARY KEY (location_key)
)
COMMENT = 'Normalized job locations';

-- ---------- dim_role ----------
CREATE OR REPLACE TABLE dim_role (
    role_key         VARCHAR(32)  NOT NULL,
    role_family      VARCHAR      NOT NULL,  -- 'ML Engineer', 'Data Engineer', ...
    seniority_level  VARCHAR,                -- 'intern','entry','mid','senior','staff','principal'
    employment_type  VARCHAR,                -- 'full_time','part_time','contract','internship'
    CONSTRAINT pk_dim_role PRIMARY KEY (role_key)
)
COMMENT = 'Standardized role family, seniority, employment type';

-- ---------- dim_source ----------
CREATE OR REPLACE TABLE dim_source (
    source_key   VARCHAR(32)  NOT NULL,
    source_name  VARCHAR      NOT NULL,    -- 'RemoteOK','Adzuna','USAJobs','HackerNews'
    source_type  VARCHAR,                  -- 'api','aggregator','government','community'
    CONSTRAINT pk_dim_source PRIMARY KEY (source_key)
)
COMMENT = 'Origin job board / API';

-- ---------- dim_skill ----------
CREATE OR REPLACE TABLE dim_skill (
    skill_key       VARCHAR(32)  NOT NULL,
    skill_name      VARCHAR      NOT NULL,  -- 'Python','PyTorch','Spark', ...
    skill_category  VARCHAR,                -- 'language','framework','cloud','tool','database'
    CONSTRAINT pk_dim_skill PRIMARY KEY (skill_key)
)
COMMENT = 'Catalog of skills mentioned in postings';