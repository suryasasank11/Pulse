"""PySpark silver transformation for RemoteOK.

Reads raw bronze JSON from S3, cleans + standardizes it, normalizes salary to
annual USD, explodes skills into the posting-skill bridge shape, deduplicates by
the posting's natural id, and writes Parquet to the silver layer.

Hardened for reliability:
  * schema-safe: never assumes a field exists (RemoteOK omits empty fields),
  * explicit S3A credentials read from the env (the same keys boto3 used),
  * loud, flushed progress prints so failures are visible in the logs.

Run via the Docker image in spark/Dockerfile.
"""

from __future__ import annotations

import argparse
import os
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pulse.config import settings


def build_spark() -> SparkSession:
    builder = SparkSession.builder.appName("pulse-silver-remoteok")
    if settings.data_lake_backend == "s3":
        builder = (
            builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            # Pass the credentials explicitly (the exact env keys boto3 succeeded
            # with) so S3A auth can't depend on provider-chain guesswork.
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", ""))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", ""))
            .config(
                "spark.hadoop.fs.s3a.endpoint",
                f"s3.{settings.aws_region}.amazonaws.com",
            )
            .config("spark.hadoop.fs.s3a.endpoint.region", settings.aws_region)
            # Fewer renames when committing to object storage.
            .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
        )
    return builder.getOrCreate()


def storage_paths(run_date: date):
    d = run_date.isoformat()
    base = (
        f"s3a://{settings.s3_bucket}"
        if settings.data_lake_backend == "s3"
        else "file://" + os.path.abspath(settings.data_lake_root)
    )
    return (
        f"{base}/bronze/source=RemoteOK/ingestion_date={d}/RemoteOK.json",
        f"{base}/silver/jobs/ingestion_date={d}/",
        f"{base}/silver/job_skills/ingestion_date={d}/",
    )


def col_or_null(df: DataFrame, name: str):
    """The column if it exists in the inferred schema, else a null literal."""
    return F.col(name) if name in df.columns else F.lit(None)


def main(run_date: date) -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    bronze_path, jobs_path, skills_path = storage_paths(run_date)
    print(f"Reading bronze: {bronze_path}", flush=True)

    # RemoteOK returns ONE JSON array; multiLine=true reads one row per element.
    raw = spark.read.option("multiLine", "true").json(bronze_path)
    print(f"Columns found: {raw.columns}", flush=True)
    print(f"Rows read (incl. metadata element): {raw.count()}", flush=True)

    # Drop RemoteOK's leading {"legal": ...} element (it has no job title).
    jobs = raw.filter(col_or_null(raw, "position").isNotNull())

    cleaned = jobs.select(
        col_or_null(jobs, "id").cast("string").alias("posting_id"),
        F.lit("RemoteOK").alias("source"),
        F.trim(col_or_null(jobs, "position")).alias("title"),
        F.trim(col_or_null(jobs, "company")).alias("company"),
        F.coalesce(F.trim(col_or_null(jobs, "location")), F.lit("Remote")).alias("location_raw"),
        col_or_null(jobs, "tags").alias("tags"),
        col_or_null(jobs, "salary_min").cast("double").alias("salary_min_src"),
        col_or_null(jobs, "salary_max").cast("double").alias("salary_max_src"),
        F.to_timestamp(col_or_null(jobs, "date")).alias("posted_at"),
        col_or_null(jobs, "url").alias("job_url"),
    )

    # Normalize salary to annual USD; 0/null means unknown.
    norm = (
        cleaned.withColumn(
            "salary_min_usd",
            F.when(F.col("salary_min_src") > 0, F.col("salary_min_src")),
        )
        .withColumn(
            "salary_max_usd",
            F.when(F.col("salary_max_src") > 0, F.col("salary_max_src")),
        )
        .withColumn(
            "salary_avg_usd",
            F.when(
                F.col("salary_min_usd").isNotNull() & F.col("salary_max_usd").isNotNull(),
                (F.col("salary_min_usd") + F.col("salary_max_usd")) / 2.0,
            ).otherwise(F.coalesce("salary_min_usd", "salary_max_usd")),
        )
        .withColumn("is_remote", F.lit(True))
        .withColumn("ingestion_date", F.lit(run_date.isoformat()))
    )

    jobs_silver = norm.drop("salary_min_src", "salary_max_src", "tags").dropDuplicates(
        ["posting_id"]
    )

    if "tags" in norm.columns:
        skills_silver = (
            norm.select(
                "posting_id",
                "ingestion_date",
                F.explode_outer("tags").alias("skill_raw"),
            )
            .withColumn("skill", F.lower(F.trim(F.col("skill_raw"))))
            .filter(F.col("skill").isNotNull() & (F.length("skill") > 0))
            .dropDuplicates(["posting_id", "skill"])
            .drop("skill_raw")
        )
    else:
        skills_silver = (
            norm.select("posting_id", "ingestion_date")
            .limit(0)
            .withColumn("skill", F.lit(None).cast("string"))
        )

    print(f"Writing jobs   -> {jobs_path}", flush=True)
    jobs_silver.write.mode("overwrite").parquet(jobs_path)
    print(f"Writing skills -> {skills_path}", flush=True)
    skills_silver.write.mode("overwrite").parquet(skills_path)

    print(
        f"DONE. Silver jobs: {jobs_silver.count()} rows | skills: {skills_silver.count()} rows",
        flush=True,
    )
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="ingestion date (YYYY-MM-DD)")
    main(date.fromisoformat(parser.parse_args().date))
