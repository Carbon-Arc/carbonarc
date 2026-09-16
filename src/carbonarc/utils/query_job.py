"""Client-side driver for power-api's poll=true job-queue contract
(PRO-3420, parent PRO-3279).

Mirrors power-ui's own ``pollableJob.ts`` driver: an endpoint called with
``poll=true`` either answers inline (the operation finished within its own
short server-side wait, or the ``queue_mediator_enabled_*`` feature flag is
off and it never entered the queue at all -- server behavior is the exact
"original always-blocking behavior" default the endpoint's own ``poll``
param documents) or returns a ``QueryJobResponse`` envelope
(``job_id``/``state``/``position``/``eta_seconds``/``queue_wait_ms``/
``poll_after_ms``) to poll against ``GET /v2/query-jobs/{job_id}``.

Both shapes are handled uniformly here via :func:`is_job_envelope` -- the
same defensive check ``pollableJob.ts`` does -- so callers never need to know
whether the flag was on, or whether their particular call happened to be
fast enough to skip the queue entirely.
"""

import time
from typing import Any, Callable, Dict, Optional

from carbonarc.utils.exceptions import (
    QueryJobCancelledError,
    QueryJobFailedError,
    QueryJobTimeoutError,
)

_TERMINAL_STATES = {"done", "failed", "cancelled"}
_DEFAULT_POLL_AFTER_MS = 1000


def is_job_envelope(response: Any) -> bool:
    """True if *response* is a ``QueryJobResponse`` envelope rather than a
    finished operation's own final result.

    Checked on shape (``job_id`` + ``state`` keys), not on whether
    ``poll=true`` was passed -- a poll=true call still gets the final result
    directly, not an envelope, whenever the operation finished inside the
    server's own short synchronous wait, or the ``queue_mediator_enabled_*``
    flag is off.
    """
    return isinstance(response, dict) and "job_id" in response and "state" in response


def poll_job_to_completion(
    *,
    initial: Dict[str, Any],
    poll_status: Callable[[str], Dict[str, Any]],
    finalize: Callable[[str], Dict[str, Any]],
    on_progress: Optional[Callable[[Dict[str, Any]], None]] = None,
    max_wait_seconds: Optional[float] = None,
) -> Dict[str, Any]:
    """Drive *initial* (a submit call's own response) to a final result.

    Args:
        initial: The response from the initial ``poll=true`` submit call.
        poll_status: ``GET /v2/query-jobs/{job_id}`` -- takes a job_id,
            returns the live envelope. Cheap: reads status only, never
            re-runs the operation.
        finalize: Re-calls the *original* endpoint with
            ``job_id=<id>&poll=true`` (same body/params as the initial
            submit) once ``poll_status`` reports a terminal state, to fetch
            the operation's real, correctly-shaped result -- a terminal
            envelope from ``poll_status`` alone never carries it (PRO-3420:
            the payload lives behind a one-time finalize, not on every
            status read).
        on_progress: Called with the live envelope on every non-terminal
            tick (both the initial response, if it's already an envelope,
            and every subsequent poll) -- the only way to observe
            ``position``/``eta_seconds``/``queue_wait_ms``/``is_heavy_query``
            live. Never called for a call that never queues at all.
        max_wait_seconds: Optional client-side ceiling on total time spent
            polling. ``None`` (default) polls indefinitely, matching prior
            behavior -- deliberately not defaulted to a finite value, since
            legitimate heavy queries are expected (see the heavy-query hint
            surfaced via ``is_heavy_query``/``on_progress``) and this
            package has no production-scale data to pick a ceiling that
            will not false-positive on some caller's real workload.
            Defense in depth only, not the primary safeguard: the server
            already self-evicts a job stuck queued or running to
            state="failed" well within an hour (see
            ``QueryStatusTracker``'s ``max_queue_age_seconds`` /
            ``max_running_age_seconds``), and this loop already terminates
            promptly on that. This guards the case the server bound doesn't
            cover -- a job that never reaches a terminal state at all (a
            regression in that eviction logic, or a still-unanticipated way
            for a job to get stuck) -- so a caller isn't left polling forever
            no matter what the server does. If you do want a ceiling,
            ``5400`` (90 minutes) is a reasonable starting point: safely
            above the server's own worst-case combined bound (30 min queued
            + 60 min running), so it will not fire against anything the
            server already guarantees to resolve on its own.

    Returns:
        The operation's final result, in the exact shape a synchronous
        (non-polling) call to the same endpoint would have returned.

    Raises:
        QueryJobFailedError: the job reached ``state="failed"``.
        QueryJobCancelledError: the job reached ``state="cancelled"`` (this
            caller's own :meth:`cancel_query_job`, or another caller/thread
            sharing the same job_id).
        QueryJobTimeoutError: ``max_wait_seconds`` elapsed before the job
            reached a terminal state.
    """
    response = initial
    # monotonic, not time.time(): immune to wall-clock adjustments (NTP
    # sync, DST, manual changes) during a wait that can span many minutes.
    deadline = (
        time.monotonic() + max_wait_seconds if max_wait_seconds is not None else None
    )
    while is_job_envelope(response) and response["state"] not in _TERMINAL_STATES:
        if deadline is not None and time.monotonic() >= deadline:
            job_id = response["job_id"]
            raise QueryJobTimeoutError(
                f"query job {job_id} did not reach a terminal state within "
                f"{max_wait_seconds}s",
                response=response,
            )
        if on_progress is not None:
            on_progress(response)
        time.sleep((response.get("poll_after_ms") or _DEFAULT_POLL_AFTER_MS) / 1000)
        response = poll_status(response["job_id"])

    if not is_job_envelope(response):
        # Never queued at all (flag off), or the initial submit already
        # finished inside its own short wait -- the final result, already.
        return response

    job_id = response["job_id"]
    state = response["state"]
    if state == "failed":
        raise QueryJobFailedError(
            response.get("error") or f"query job {job_id} failed",
            response=response,
        )
    if state == "cancelled":
        raise QueryJobCancelledError(
            f"query job {job_id} was cancelled", response=response
        )
    # state == "done": the terminal envelope itself never carries the real
    # payload (see `finalize`'s own docstring above) -- one more call fetches it.
    return finalize(job_id)
