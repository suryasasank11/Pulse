"""RemoteOK extractor.

RemoteOK exposes a free public JSON API at https://remoteok.com/api returning
the current list of recent remote jobs. Two real-world quirks to know:
  1. It REQUIRES a User-Agent header; default/empty agents get a 403.
     (We set one centrally in http_client, sourced from config.)
  2. The FIRST element of the returned array is a metadata/legal notice,
     not a job. We land the raw payload exactly as received and strip that
     element later, in the silver layer — bronze stays byte-for-byte raw.
"""
from __future__ import annotations

import logging

from ..http_client import build_session
from .base import BaseExtractor, ExtractionError

logger = logging.getLogger(__name__)

REMOTEOK_API_URL = "https://remoteok.com/api"


class RemoteOKExtractor(BaseExtractor):
    source_name = "RemoteOK"

    def __init__(self) -> None:
        self.session = build_session()

    def fetch(self) -> bytes:
        logger.info("[%s] GET %s", self.source_name, REMOTEOK_API_URL)
        try:
            resp = self.session.get(REMOTEOK_API_URL)
        except Exception as exc:  # raised only after retries are exhausted
            raise ExtractionError(f"{self.source_name}: request failed: {exc}") from exc

        if resp.status_code != 200:
            # Transient 429/5xx were already retried by the session; if we are
            # still not at 200, the failure is permanent — fail fast and loud.
            raise ExtractionError(
                f"{self.source_name}: unexpected status {resp.status_code}"
            )
        return resp.content