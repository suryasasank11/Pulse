"""PySpark silver transformation for USAJobs.

USAJobs nests the useful fields under MatchedObjectDescriptor and returns salary
as an array (PositionRemuneration). We map into the SAME silver schema as every
other source, keep only annual ('PA') pay so hourly rates don't pollute salary
stats, infer is_remote / telework from the text, and extract skills from the
qualification summary into the posting-skill bridge.
"""
from __future__ import annotations

import argparse
import os
from datetime import date

from pyspark.sql import SparkSession, DataFrame, functions as F

from pulse.config import settings

SKILL_VOCAB = [
    "python", "java", "javascript", "typescript", "go", "scala", "rust", "ruby", "c++", "sql",
    "aws", "gcp", "azure", "kubernetes", "docker", "terraform", "linux",
    "spark", "airflow", "kafka", "hadoop", "snowflake", "dbt", "databricks",
    "pytorch", "tensorflow", "keras", "pandas", "numpy",
    "react", "angular", "vue", "django", "flask", "fastapi",
    "postgres", "postgresql", "mysql", "mongodb", "redis",
    "tableau", "power bi", "looker", "etl", "machine learning", "deep learning", "nlp",
]

D = "MatchedObjectDescriptor"


def build_spark() -> SparkSession:
    builder = SparkSession.builder.appName("pulse-silver-usajobs")
    if settings.data_lake_backend == "s3":
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", ""))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{settings.aws_region}.amazonaws.com")
            .config("spark.hadoop.fs.s3a.endpoint.region", settings.aws_region)
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        )
    return builder.getOrCreate()


def storage_paths(run_date: date):
    d = run_date.isoformat()
    base = (
        f"s3a://{settings.s3_bucket}" if settings.data_lake_backend == "s3"
        else "file://" + os.path.abspath(settings.data_lake_root)
    )
    return (
        f"{base}/bronze/source=USAJobs/ingestion_date={d}/USAJobs.json",
        f"{base}/silver/jobs/source=USAJobs/ingestion_date={d}/",
        f"{base}/silver/job_skills/source=USAJobs/ingestion_date={d}/",
    )


def col_or_null(df: DataFrame, name: str):
    return F.col(name) if name in df.columns else F.lit(None)


def main(run_date: date) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze_path, jobs_path, skills_path = storage_paths(run_date)
    print(f"Reading bronze: {bronze_path}", flush=True)

    raw = spark.read.option("multiLine", "true").json(bronze_path)
    print(f"Columns found: {raw.columns}", flush=True)
    print(f"Rows read: {raw.count()}", flush=True)

    if D not in raw.columns:
        raise SystemExit(f"expected '{D}' in payload; got {raw.columns}")

    pay = F.col(f"{D}.PositionRemuneration").getItem(0)
    is_annual = pay.getField("RateIntervalCode") == F.lit("PA")

    base = raw.select(
        F.concat(F.lit("usajobs-"), col_or_null(raw, "MatchedObjectId").cast("string")).alias("posting_id"),
        F.lit("USAJobs").alias("source"),
        F.trim(F.col(f"{D}.PositionTitle")).alias("title"),
        F.trim(F.col(f"{D}.OrganizationName")).alias("company"),
        F.trim(F.col(f"{D}.PositionLocationDisplay")).alias("location_raw"),
        F.when(is_annual, pay.getField("MinimumRange").cast("double")).alias("salary_min_src"),
        F.when(is_annual, pay.getField("MaximumRange").cast("double")).alias("salary_max_src"),
        F.to_timestamp(F.col(f"{D}.PublicationStartDate")).alias("posted_at"),
        F.col(f"{D}.PositionURI").alias("job_url"),
        F.col(f"{D}.QualificationSummary").alias("summary"),
    ).filter(F.col("title").isNotNull())

    norm = (
        base
        .withColumn("salary_min_usd", F.when(F.col("salary_min_src") > 0, F.col("salary_min_src")))
        .withColumn("salary_max_usd", F.when(F.col("salary_max_src") > 0, F.col("salary_max_src")))
        .withColumn(
            "salary_avg_usd",
            F.when(
                F.col("salary_min_usd").isNotNull() & F.col("salary_max_usd").isNotNull(),
                (F.col("salary_min_usd") + F.col("salary_max_usd")) / 2.0,
            ).otherwise(F.coalesce("salary_min_usd", "salary_max_usd")),
        )
        .withColumn(
            "is_remote",
            F.lower(
                F.concat_ws(
                    " ",
                    F.coalesce(F.col("title"), F.lit("")),
                    F.coalesce(F.col("location_raw"), F.lit("")),
                    F.coalesce(F.col("summary"), F.lit("")),
                )
            ).rlike("remote|telework"),
        )
        .withColumn("ingestion_date", F.lit(run_date.isoformat()))
    )

    jobs_silver = norm.select(
        "posting_id", "source", "title", "company", "location_raw",
        "salary_min_usd", "salary_max_usd", "salary_avg_usd",
        "is_remote", "job_url", "posted_at", "ingestion_date",
    ).dropDuplicates(["posting_id"])

    matched = F.array_distinct(
        F.array_compact(
            F.array(*[
                F.when(F.lower(F.coalesce(F.col("summary"), F.lit(""))).contains(s), F.lit(s))
                for s in SKILL_VOCAB
            ])
        )
    )
    skills_silver = (
        norm.select("posting_id", "ingestion_date", matched.alias("skills"))
        .select("posting_id", "ingestion_date", F.explode_outer("skills").alias("skill"))
        .filter(F.col("skill").isNotNull())
        .dropDuplicates(["posting_id", "skill"])
    )

    print(f"Writing jobs   -> {jobs_path}", flush=True)
    jobs_silver.write.mode("overwrite").parquet(jobs_path)
    print(f"Writing skills -> {skills_path}", flush=True)
    skills_silver.write.mode("overwrite").parquet(skills_path)

    print(f"DONE. USAJobs jobs: {jobs_silver.count()} | skills: {skills_silver.count()}", flush=True)
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="ingestion date (YYYY-MM-DD)")
    main(date.fromisoformat(parser.parse_args().date))