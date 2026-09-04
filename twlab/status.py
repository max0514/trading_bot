"""`RunStatus`: the one vocabulary for how a Dataset's run turned out.

The pipeline records an outcome per batch day, the Orchestrator adds the one
it records when a run raises, the Witness logs its own against the Dataset,
and the dashboard synthesizes three more for rows that have no run log entry
at all. They were four informal extensions of one `Literal`; this is the
single type, and `published` is the question every caller was really asking.

It lives in its own module because both the pipeline and the Mongo store need
it, and the pipeline imports the store.
"""
from __future__ import annotations

from enum import Enum


class RunStatus(str, Enum):
    """Every outcome a Dataset can be in — one vocabulary for the pipeline,
    the Orchestrator, the Witness and the dashboard, which each used to
    extend a `Literal` informally with statuses of their own.

    Members compare and hash as their string value, so a run log row, a
    `--status` line and a badge lookup all still work with a plain string.
    """

    # Recorded by the pipeline against one batch day.
    OK = "ok"
    NO_DATA = "no_data"
    QUARANTINED = "quarantined"
    # Recorded by the Orchestrator when a run raised out of the pipeline.
    FAILED = "failed"
    # Recorded by the Witness against the Dataset, not a batch day.
    WITNESS_OK = "witness_ok"
    WITNESS_ALERT = "witness_alert"
    # Synthesized for display; never written to the run log.
    NEVER = "never"
    UNAVAILABLE = "unavailable"
    RUNNING = "running"

    def __str__(self) -> str:
        return self.value

    @property
    def published(self) -> bool:
        """Is this batch day done, so the Orchestrator should not retry it?

        A holiday is as published as a full trading day: both mean the day
        needs no further work. Quarantined and failed days do.
        """
        return self in (RunStatus.OK, RunStatus.NO_DATA)


# Outcomes that count as "this batch day is done" for the Orchestrator.
PUBLISHED = tuple(s for s in RunStatus if s.published)
