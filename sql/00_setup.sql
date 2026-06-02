-- ============================================================
-- 00_setup.sql
-- Creates the warehouse, database, and schemas for the
-- AI Job Market Analytics Platform.
--
-- Run ONCE, with a role allowed to create these objects
-- (SYSADMIN is fine on a trial account).
-- Every statement uses IF NOT EXISTS, so it is safe to re-run.
-- Run order for the whole project: 00 -> 01 -> 02 -> 03
-- ============================================================

-- A small, cost-controlled compute warehouse.
-- XSMALL is the cheapest size. AUTO_SUSPEND = 60 parks it after
-- 60 seconds idle and AUTO_RESUME wakes it on the next query, so
-- you only ever pay for seconds you actually compute.
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS AI_JOBS_WH
    WAREHOUSE_SIZE       = 'XSMALL'
    AUTO_SUSPEND         = 60
    AUTO_RESUME          = TRUE
    INITIALLY_SUSPENDED  = TRUE
    COMMENT = 'Compute for the AI jobs analytics platform';

-- One database for the entire project.
CREATE DATABASE IF NOT EXISTS AI_JOBS
    COMMENT = 'AI / ML / DS / DE job market analytics platform';

USE DATABASE AI_JOBS;

-- The medallion layers, as schemas. We build the gold star
-- schema (MARTS) first; RAW and STAGING get used in later phases.
CREATE SCHEMA IF NOT EXISTS RAW
    COMMENT = 'Bronze: raw landed data, as ingested';
CREATE SCHEMA IF NOT EXISTS STAGING
    COMMENT = 'Silver: cleaned and conformed';
CREATE SCHEMA IF NOT EXISTS MARTS
    COMMENT = 'Gold: star schema for analytics';

-- Set the working context for the scripts that follow.
USE SCHEMA   AI_JOBS.MARTS;
USE WAREHOUSE AI_JOBS_WH;