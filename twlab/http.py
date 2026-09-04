"""Polite HTTP client for Official Sources.

Identifiable UA, bounded retries with backoff, and a minimum interval between
requests so scrapers never hammer TWSE/TPEx/MOPS. The pipeline receives a
session object, so tests replace it with a fake at the HTTP boundary; clock
and sleep are injected so politeness itself is testable without real waiting.
"""
from __future__ import annotations

import time
from typing import Any, Callable

import requests

USER_AGENT = "twlab/0.1 (self-hosted research data pipeline)"
DEFAULT_TIMEOUT = 30
DEFAULT_MIN_INTERVAL = 3.0  # seconds between requests to official sources
DEFAULT_RETRIES = 3


class PoliteSession:
    """requests.Session wrapper: rate-limited, retrying, identifiable."""

    def __init__(
        self,
        min_interval: float = DEFAULT_MIN_INTERVAL,
        retries: int = DEFAULT_RETRIES,
        timeout: float = DEFAULT_TIMEOUT,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
        transport: requests.Session | None = None,
    ):
        self._session = transport or requests.Session()
        self._session.headers["User-Agent"] = USER_AGENT
        self._min_interval = min_interval
        self._retries = retries
        self._timeout = timeout
        self._sleep = sleep
        self._clock = clock
        self._last_request_at: float | None = None

    def _fetch(self, url: str, params: dict[str, Any] | None,
               decode: Callable[[requests.Response], Any]) -> Any:
        last_error: Exception | None = None
        for attempt in range(self._retries):
            if self._last_request_at is not None:
                wait = self._min_interval - (self._clock() - self._last_request_at)
                if wait > 0:
                    self._sleep(wait)
            self._last_request_at = self._clock()
            try:
                resp = self._session.get(url, params=params, timeout=self._timeout)
                resp.raise_for_status()
                return decode(resp)
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                self._sleep(2 ** attempt)
        assert last_error is not None
        raise last_error

    def get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        return self._fetch(url, params, lambda resp: resp.json())

    def get_text(self, url: str, params: dict[str, Any] | None = None,
                 encoding: str | None = None) -> str:
        """Fetch an HTML/text page. MOPS and the ISIN site serve legacy
        encodings without declaring them; `encoding` overrides, otherwise the
        declared or apparent encoding is used."""

        def decode(resp: requests.Response) -> str:
            if encoding:
                resp.encoding = encoding
            elif resp.encoding is None or resp.encoding.lower() == "iso-8859-1":
                resp.encoding = resp.apparent_encoding
            return resp.text

        return self._fetch(url, params, decode)
