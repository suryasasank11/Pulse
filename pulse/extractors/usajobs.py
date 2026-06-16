"""USAJobs extractor.

Federal job postings from the US Office of Personnel Management. Auth is three
required headers: Host, User-Agent (your registered email), and Authorization-Key.
The search endpoint returns an object; we page through
SearchResult.SearchResultItems and land the combined items array to bronze, so
bronze stays a clean JSON array consistent with the other sources.

Defaults to occupational series 2210 (Information Technology Management).
"""

from __future__ import annotations

import json
import logging
import os

from ..http_client import build_session
from .base import BaseExtractor, ExtractionError

logger = logging.getLogger(__name__)

USAJOBS_URL = "https://data.usajobs.gov/api/search"


class USAJobsExtractor(BaseExtractor):
    source_name = "USAJobs"

    def __init__(self) -> None:
        self.session = build_session()
        self.api_key = os.getenv("USAJOBS_API_KEY", "")
        self.email = os.getenv("USAJOBS_EMAIL", "")
        self.keyword = os.getenv("USAJOBS_KEYWORD", "")
        self.category = os.getenv("USAJOBS_CATEGORY", "2210")
        self.pages = int(os.getenv("USAJOBS_PAGES", "5"))
        self.per_page = int(os.getenv("USAJOBS_RESULTS_PER_PAGE", "50"))

    def fetch(self) -> bytes:
        if not self.api_key or not self.email:
            raise ExtractionError("USAJobs: USAJOBS_API_KEY / USAJOBS_EMAIL not set in environment")

        headers = {
            "Host": "data.usajobs.gov",
            "User-Agent": self.email,
            "Authorization-Key": self.api_key,
        }

        items: list = []
        for page in range(1, self.pages + 1):
            params = {
                "ResultsPerPage": self.per_page,
                "Page": page,
                "JobCategoryCode": self.category,
            }
            if self.keyword:
                params["Keyword"] = self.keyword

            logger.info("[%s] GET %s (page %d)", self.source_name, USAJOBS_URL, page)
            try:
                resp = self.session.get(USAJOBS_URL, params=params, headers=headers)
            except Exception as exc:
                raise ExtractionError(f"{self.source_name}: request failed: {exc}") from exc

            if resp.status_code != 200:
                raise ExtractionError(
                    f"{self.source_name}: unexpected status {resp.status_code} on page {page}"
                )

            page_items = resp.json().get("SearchResult", {}).get("SearchResultItems", [])
            if not page_items:
                logger.info("[%s] page %d empty; stopping", self.source_name, page)
                break
            items.extend(page_items)

        logger.info("[%s] collected %d items", self.source_name, len(items))
        return json.dumps(items).encode("utf-8")
