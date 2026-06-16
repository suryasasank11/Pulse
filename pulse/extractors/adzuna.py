"""Adzuna extractor.

Adzuna exposes a free REST API (developer.adzuna.com) over a large aggregated
job database. Unlike RemoteOK it covers on-site roles and provides real,
structured salary and location data -- exactly the gap RemoteOK leaves.

Auth is two free query-string credentials, app_id + app_key, read from the
environment (ADZUNA_APP_ID / ADZUNA_APP_KEY). The search endpoint is paginated
and returns an OBJECT with a "results" array; we page through it and land the
combined results array to bronze, so bronze stays a clean JSON array of records
consistent with every other source.
"""

from __future__ import annotations

import json
import logging
import os

from ..http_client import build_session
from .base import BaseExtractor, ExtractionError

logger = logging.getLogger(__name__)

ADZUNA_BASE = "https://api.adzuna.com/v1/api/jobs"


class AdzunaExtractor(BaseExtractor):
    source_name = "Adzuna"

    def __init__(self) -> None:
        self.session = build_session()
        self.app_id = os.getenv("ADZUNA_APP_ID", "")
        self.app_key = os.getenv("ADZUNA_APP_KEY", "")
        self.country = os.getenv("ADZUNA_COUNTRY", "us")
        self.what = os.getenv("ADZUNA_WHAT", "data engineer")
        self.pages = int(os.getenv("ADZUNA_PAGES", "5"))
        self.per_page = int(os.getenv("ADZUNA_RESULTS_PER_PAGE", "50"))

    def fetch(self) -> bytes:
        if not self.app_id or not self.app_key:
            raise ExtractionError("Adzuna: ADZUNA_APP_ID / ADZUNA_APP_KEY not set in environment")

        all_results: list = []
        for page in range(1, self.pages + 1):
            url = f"{ADZUNA_BASE}/{self.country}/search/{page}"
            params = {
                "app_id": self.app_id,
                "app_key": self.app_key,
                "results_per_page": self.per_page,
                "what": self.what,
                "content-type": "application/json",
            }
            logger.info("[%s] GET %s (page %d)", self.source_name, url, page)
            try:
                resp = self.session.get(url, params=params)
            except Exception as exc:
                raise ExtractionError(f"{self.source_name}: request failed: {exc}") from exc

            if resp.status_code != 200:
                raise ExtractionError(
                    f"{self.source_name}: unexpected status {resp.status_code} on page {page}"
                )

            page_results = resp.json().get("results", [])
            if not page_results:
                logger.info("[%s] page %d empty; stopping", self.source_name, page)
                break
            all_results.extend(page_results)

        logger.info("[%s] collected %d total results", self.source_name, len(all_results))
        return json.dumps(all_results).encode("utf-8")
