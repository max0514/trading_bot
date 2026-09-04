"""`RunStatus`: the one run-outcome vocabulary.

The pipeline, Orchestrator, Witness and dashboard each used to extend a
`Literal` with statuses of their own; these pin the single type they now share
and the `published` question every one of them was really asking.
"""
import datetime as dt

import pytest

from twlab.pipeline import RunResult
from twlab.status import PUBLISHED, RunStatus

DAY = dt.date(2026, 8, 7)


@pytest.mark.parametrize("status,published", [
    (RunStatus.OK, True),
    (RunStatus.NO_DATA, True),        # a holiday needs no further work either
    (RunStatus.QUARANTINED, False),
    (RunStatus.FAILED, False),
    (RunStatus.WITNESS_OK, False),    # the Witness never publishes a batch day
    (RunStatus.WITNESS_ALERT, False),
    (RunStatus.NEVER, False),
    (RunStatus.UNAVAILABLE, False),
    (RunStatus.RUNNING, False),
])
def test_published_is_answered_for_every_status(status, published):
    assert status.published is published


def test_published_matches_the_orchestrators_retry_set():
    assert PUBLISHED == (RunStatus.OK, RunStatus.NO_DATA)
    assert set(PUBLISHED) == {s for s in RunStatus if s.published}


def test_a_status_reads_and_compares_as_its_string():
    """Run log rows, CLI output and badge lookups all still use plain strings."""
    assert RunStatus.QUARANTINED == "quarantined"
    assert str(RunStatus.QUARANTINED) == "quarantined"
    assert f"{RunStatus.NO_DATA}" == "no_data"
    assert {RunStatus.OK: 1}["ok"] == 1
    assert RunStatus("witness_alert") is RunStatus.WITNESS_ALERT


def test_a_run_result_holds_the_enum_however_it_was_built():
    assert RunResult("price", DAY, "ok").status is RunStatus.OK
    assert RunResult("price", DAY, RunStatus.FAILED).status.published is False
    with pytest.raises(ValueError):
        RunResult("price", DAY, "half_ok")


def test_the_run_log_can_only_hold_a_known_status(mongo):
    from twlab import registry
    spec = registry.get_spec("price")
    with pytest.raises(ValueError):
        mongo.record_run(spec, DAY, "sort_of_ok", dt.datetime(2026, 8, 7, 21, 32))


def test_every_status_has_a_dashboard_badge():
    from dashboard.twlab_panel import _BADGES
    assert set(_BADGES) == set(RunStatus)
