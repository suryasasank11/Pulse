"""A requests session hardened with retries, exponential backoff, and a
default timeout — the transport layer every extractor shares.
"""
from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import settings

# Statuses worth retrying: rate-limit + transient server errors.
# We deliberately do NOT retry ordinary 4xx (400/403/404) — those are
# permanent and retrying just wastes time and hammers the source.
_RETRY_STATUSES = (429, 500, 502, 503, 504)


class _TimeoutSession(requests.Session):
    """A Session that applies a default timeout to every request, so a
    hung connection can never block the pipeline forever."""

    def __init__(self, timeout: int) -> None:
        super().__init__()
        self._timeout = timeout

    def request(self, *args, **kwargs):  # type: ignore[override]
        kwargs.setdefault("timeout", self._timeout)
        return super().request(*args, **kwargs)


def build_session(total_retries: int = 5, backoff_factor: float = 1.0) -> requests.Session:
    """Build a session that retries transient failures with exponential backoff.

    With backoff_factor=1.0 the waits between attempts grow ~ {1, 2, 4, 8, 16}s,
    and the server's Retry-After header is honored when present.
    """
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=_RETRY_STATUSES,
        allowed_methods=frozenset(["GET"]),
        respect_retry_after_header=True,
        raise_on_status=False,  # return the final response; we inspect status ourselves
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = _TimeoutSession(timeout=settings.http_timeout_seconds)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": settings.http_user_agent})
    return session