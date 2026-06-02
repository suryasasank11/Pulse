-- ============================================================
-- 02_facts.sql
-- The two fact tables of the star schema (gold / MARTS).
-- Run AFTER 01_dimensions.sql: the foreign keys reference the
-- dimension tables, and fact_job_skill references fact_job_posting.
-- ============================================================

USE SCHEMA AI_JOBS.MARTS;

-- ---------- fact_job_posting ----------
-- GRAIN: one row per (deduplicated) job posting.
--   posting_id  -> the source's own id, kept as a "degenerate
--                  dimension" (an attribute we filter by but that
--                  needs no table of its own).
--   *_key       -> foreign keys into the dimensions.
--   *_usd       -> measures: salary normalized to annual USD.
CREATE OR REPLACE TABLE fact_job_posting (
    job_posting_key  VARCHAR(32)    NOT NULL,   -- hashed surrogate
    posting_id       VARCHAR        NOT NULL,   -- source natural id (degenerate dimension)

    company_key      VARCHAR(32),
    location_key     VARCHAR(32),
    date_key         NUMBER(8,0),
    role_key         VARCHAR(32),
    source_key       VARCHAR(32),

    salary_min_usd   NUMBER(12,2),
    salary_max_usd   NUMBER(12,2),
    salary_avg_usd   NUMBER(12,2),
    is_remote        BOOLEAN,

    job_url          VARCHAR,
    loaded_at        TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT pk_fact_job_posting PRIMARY KEY (job_posting_key),
    CONSTRAINT fk_fjp_company  FOREIGN KEY (company_key)  REFERENCES dim_company  (company_key),
    CONSTRAINT fk_fjp_location FOREIGN KEY (location_key) REFERENCES dim_location (location_key),
    CONSTRAINT fk_fjp_date     FOREIGN KEY (date_key)     REFERENCES dim_date     (date_key),
    CONSTRAINT fk_fjp_role     FOREIGN KEY (role_key)     REFERENCES dim_role     (role_key),
    CONSTRAINT fk_fjp_source   FOREIGN KEY (source_key)   REFERENCES dim_source   (source_key)
)
COMMENT = 'Gold fact: one row per job posting';

-- Clustering: NOT needed at our scale. Snowflake's micro-partitions
-- prune well on their own for tables up to tens of millions of rows,
-- and an auto-clustering key costs extra credits to maintain. If this
-- table ever grew into the hundreds of millions of rows, the most
-- common filter is the posting date, so you would add:
--     ALTER TABLE fact_job_posting CLUSTER BY (date_key);

-- ---------- fact_job_skill (bridge) ----------
-- GRAIN: one row per (job posting, skill) pair.
-- This is how we model the many-to-many between postings and skills.
-- It has NO surrogate key of its own; its primary key is the
-- COMBINATION of the two foreign keys.
CREATE OR REPLACE TABLE fact_job_skill (
    job_posting_key  VARCHAR(32)  NOT NULL,
    skill_key        VARCHAR(32)  NOT NULL,

    CONSTRAINT pk_fact_job_skill PRIMARY KEY (job_posting_key, skill_key),
    CONSTRAINT fk_fjs_posting FOREIGN KEY (job_posting_key) REFERENCES fact_job_posting (job_posting_key),
    CONSTRAINT fk_fjs_skill   FOREIGN KEY (skill_key)       REFERENCES dim_skill        (skill_key)
)
COMMENT = 'Bridge: links postings to their required skills';