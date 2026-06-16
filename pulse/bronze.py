"""Writes raw source payloads to the bronze layer (local disk or S3).

Bronze = exactly what we received, byte-for-byte, untouched, partitioned by
source and ingestion date. Parsing and cleaning happen later in the silver
layer, so the raw record is always available to reprocess.
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

from .config import settings

logger = logging.getLogger(__name__)


def _bronze_key(source: str, logical_date: date) -> str:
    """Deterministic, partitioned object key for one day's pull.

    Same source + same date -> same key, so a re-run OVERWRITES the existing
    file instead of creating a duplicate. That is what makes landing idempotent.
    Partitioning by ingestion_date also lets downstream queries read just one
    day's slice instead of scanning everything.
    """
    return f"bronze/source={source}/ingestion_date={logical_date.isoformat()}/{source}.json"


def write_bronze(payload: bytes, source: str, logical_date: date) -> str:
    """Write raw bytes to bronze. Returns the full destination path or URI."""
    key = _bronze_key(source, logical_date)
    if settings.data_lake_backend == "s3":
        return _write_s3(payload, key)
    return _write_local(payload, key)


def _write_local(payload: bytes, key: str) -> str:
    path = Path(settings.data_lake_root) / key
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)
    logger.info("Wrote %d bytes to %s", len(payload), path)
    return str(path)


def _write_s3(payload: bytes, key: str) -> str:
    import boto3  # imported lazily so a local run needs no AWS setup at all

    if not settings.s3_bucket:
        raise RuntimeError("DATA_LAKE_BACKEND=s3 but S3_BUCKET is not set")
    s3 = boto3.client("s3", region_name=settings.aws_region)
    s3.put_object(Bucket=settings.s3_bucket, Key=key, Body=payload)
    uri = f"s3://{settings.s3_bucket}/{key}"
    logger.info("Wrote %d bytes to %s", len(payload), uri)
    return uri
