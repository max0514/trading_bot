"""PoliteSession behavior: identifiable, rate-limited, retrying.

No real network and no real waiting — transport, clock, and sleep are injected.
"""
import pytest
import requests

from twlab.http import USER_AGENT, PoliteSession


class FakeResponse:
    def __init__(self, payload=None, error=None):
        self._payload = payload
        self._error = error

    def raise_for_status(self):
        if self._error:
            raise self._error

    def json(self):
        return self._payload


class FakeTransport:
    """Stands in for requests.Session; serves scripted responses."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.headers: dict[str, str] = {}
        self.calls: list[str] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append(url)
        return self._responses.pop(0)


class FakeClock:
    def __init__(self):
        self.now = 0.0
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


def make_session(responses, clock, min_interval=3.0, retries=3):
    transport = FakeTransport(responses)
    session = PoliteSession(
        min_interval=min_interval,
        retries=retries,
        sleep=clock.sleep,
        clock=clock,
        transport=transport,
    )
    return session, transport


def test_identifies_itself():
    clock = FakeClock()
    session, transport = make_session([FakeResponse({"stat": "OK"})], clock)
    session.get_json("https://example.test")
    assert transport.headers["User-Agent"] == USER_AGENT
    assert "twlab" in USER_AGENT


def test_back_to_back_requests_respect_min_interval():
    clock = FakeClock()
    session, _ = make_session(
        [FakeResponse({}), FakeResponse({})], clock, min_interval=3.0
    )
    session.get_json("https://example.test/a")
    session.get_json("https://example.test/b")
    assert any(s >= 3.0 for s in clock.slept)  # second call waited out the interval


def test_retries_transient_failures_then_succeeds():
    clock = FakeClock()
    session, transport = make_session(
        [
            FakeResponse(error=requests.ConnectionError("boom")),
            FakeResponse({"stat": "OK"}),
        ],
        clock,
    )
    assert session.get_json("https://example.test") == {"stat": "OK"}
    assert len(transport.calls) == 2


def test_gives_up_after_bounded_retries():
    clock = FakeClock()
    session, transport = make_session(
        [FakeResponse(error=requests.ConnectionError("boom"))] * 3,
        clock,
        retries=3,
    )
    with pytest.raises(requests.ConnectionError):
        session.get_json("https://example.test")
    assert len(transport.calls) == 3
