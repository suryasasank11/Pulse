"""Base class for all source extractors.

Each source only implements fetch(); this base class owns the shared flow —
fetch -> validate -> land to bronze — with consistent logging and error
semantics. Adding a new source later means writing one small fetch() method.
"""
from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from datetime import date, datetime, timezone

from ..bronze import write_bronze

logger = logging.getLogger(__name__)


class ExtractionError(Exception):
    """Raised on a failure that should fail the task (so the orchestrator alerts)."""


class BaseExtractor(ABC):
    # Subclasses set this to the dim_source name, e.g. "RemoteOK".
    source_name: str = "base"

    @abstractmethod
    def fetch(self) -> bytes:
        """Call the source and return the raw response body as bytes.

        Must raise ExtractionError on a permanent failure.
        """

    def validate(self, payload: bytes) -> int:
        """Cheap sanity check BEFORE we land anything; returns the record count.

        This guarantees we never write an empty or malformed file to bronze —
        a corrupt landing is worse than a clean failure, because it silently
        poisons everything downstream.
        """
        if not payload:
            raise ExtractionError(f"{self.source_name}: empty response body")
        try:
            data = json.loads(payload)
        except json.JSONDecodeError as exc:
            raise ExtractionError(f"{self.source_name}: response is not valid JSON") from exc
        if not isinstance(data, list) or len(data) == 0:
            raise ExtractionError(f"{self.source_name}: expected a non-empty JSON array")
        return len(data)

    def run(self, logical_date: date | None = None) -> str:
        """Fetch -> validate -> land. Returns the bronze path/URI written."""
        logical_date = logical_date or datetime.now(timezone.utc).date()
        logger.info("[%s] starting extraction for %s", self.source_name, logical_date)

        payload = self.fetch()
        count = self.validate(payload)
        path = write_bronze(payload, source=self.source_name, logical_date=logical_date)

        logger.info("[%s] landed %d records to %s", self.source_name, count, path)
        return path