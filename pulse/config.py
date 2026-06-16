"""Central, environment-driven configuration.

Everything that changes between your laptop, CI, and production lives here
and is read from environment variables (with sensible local defaults), so no
secret or environment-specific value is ever hard-coded in the source.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()  # read a local .env file if one exists


@dataclass(frozen=True)
class Settings:
    data_lake_root: str = os.getenv("DATA_LAKE_ROOT", "./data")
    data_lake_backend: str = os.getenv("DATA_LAKE_BACKEND", "local")  # local | s3
    aws_region: str = os.getenv("AWS_REGION", "us-east-2")
    s3_bucket: str | None = os.getenv("S3_BUCKET") or None
    http_user_agent: str = os.getenv(
        "HTTP_USER_AGENT", "ai-jobs-platform/0.1 (contact: you@example.com)"
    )
    http_timeout_seconds: int = int(os.getenv("HTTP_TIMEOUT_SECONDS", "30"))


settings = Settings()
