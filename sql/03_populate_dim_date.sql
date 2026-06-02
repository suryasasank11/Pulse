-- ============================================================
-- 03_populate_dim_date.sql
-- Fills dim_date with every day from 2020-01-01 to 2030-12-31.
--
-- dim_date is STATIC reference data (the calendar never changes),
-- so we populate it directly here, once. Every OTHER table gets
-- filled by the ingestion pipeline / dbt in later phases.
--
-- Re-runnable: the TRUNCATE clears the table first, so running
-- this twice will not create duplicate days.
-- ============================================================

USE SCHEMA AI_JOBS.MARTS;

TRUNCATE TABLE dim_date;

INSERT INTO dim_date
SELECT
    TO_NUMBER(TO_CHAR(d, 'YYYYMMDD'))  AS date_key,
    d                                  AS full_date,
    YEAR(d)                            AS year,
    QUARTER(d)                         AS quarter,
    MONTH(d)                           AS month,
    MONTHNAME(d)                       AS month_name,
    WEEKISO(d)                         AS week_of_year,   -- ISO week, deterministic
    DAYOFWEEKISO(d)                    AS day_of_week,    -- 1=Mon .. 7=Sun, deterministic
    DAYNAME(d)                         AS day_name,
    DAYOFWEEKISO(d) IN (6, 7)          AS is_weekend
FROM (
    -- GENERATOR(ROWCOUNT => n) emits n rows. ROW_NUMBER()-1 turns
    -- them into a gap-free counter 0,1,2,..., and DATEADD turns that
    -- counter into consecutive calendar dates from the start date.
    SELECT DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, DATE '2020-01-01') AS d
    FROM TABLE(GENERATOR(ROWCOUNT => 4100))
)
WHERE d <= DATE '2030-12-31';

-- Quick sanity check (expect 4018 rows, min 2020-01-01, max 2030-12-31):
-- SELECT COUNT(*), MIN(full_date), MAX(full_date) FROM dim_date;