"""Train (preview): train a forecasting model on frameworks you bought.

**Preview.** ``client.train`` works only for accounts with Train turned on;
for any other account every call raises :class:`TrainNotEnabled` (or, for a
call that names an id, :class:`TrainNotFound`). Methods and return shapes can
still change.

Every method returns the API's JSON as plain dicts and lists, with its keys
exactly as power-api sends them (camelCase: ``trainable``, ``trainingNote``,
``windowStart``). The ``*_to_dataframe`` helpers give snake_case columns.
"""

import warnings
from http import HTTPStatus
from typing import Any, Dict, List, Optional, Type

import pandas as pd
import requests

from carbonarc.utils.client import BaseAPIClient
from carbonarc.utils.exceptions import (
    NoTrainingGrant,
    NotEnoughData,
    TrainBusy,
    TrainError,
    TrainNotEnabled,
    TrainNotFound,
    TrainPlatformError,
    TrainRunExists,
)

# PLACEHOLDER (PRO-6520): the customer-facing wording for an account without
# Train is still to be agreed with the team, as is when client.train ships.
TRAIN_NOT_ENABLED_MESSAGE = "Train isn't enabled for this account."

# power-api's refusal codes (the contract on PRO-5784) -> the exception each
# raises. "FW-404" is absent: a 404 depends on the call (see ``_refusal``).
_REFUSALS: Dict[str, Type[TrainError]] = {
    "TRAIN_RIGHTS": NoTrainingGrant,
    "TRAIN_DATA": NotEnoughData,
    "TRAIN_DUPLICATE": TrainRunExists,
    "TRAIN_BUSY": TrainBusy,
    "TRAIN_PLATFORM": TrainPlatformError,
}

# The same codes by HTTP status, for a refusal that carries no code.
_REFUSALS_BY_STATUS: Dict[int, Type[TrainError]] = {
    HTTPStatus.FORBIDDEN: NoTrainingGrant,
    HTTPStatus.CONFLICT: TrainRunExists,
    HTTPStatus.SERVICE_UNAVAILABLE: TrainBusy,
}


class TrainAPIClient(BaseAPIClient):
    """Client for the Carbon Arc Train API (preview).

    Find what you can train on: the frameworks your account bought, whether
    each can be trained and why not, a sample of each one's data, and which
    measures could help predict a target.
    """

    def __init__(self, token: str, host: str, version: str):
        super().__init__(token=token, host=host, version=version)
        self._base_url = f"{host.rstrip('/')}/{version}/train"

    # -- What you can train on ----------------------------------------------

    def list_frameworks(
        self,
        search: Optional[str] = None,
        framework_ids: Optional[List[str]] = None,
        include_untrainable: bool = False,
    ) -> List[dict]:
        """List the frameworks your account bought that Train can train on,
        as the New model page offers them.

        A framework can be trained when its data's training rights allow it
        (``trainable``) and Train can build training data from it
        (``trainingNote`` is null). The rest are left out, and a warning
        says how many and why; pass ``include_untrainable=True`` to list
        them too. :func:`frameworks_to_dataframe` then combines the two into
        one ``trainable`` column with the reason, in the New model page's
        words.

        At most 50 come back, the larger purchases first, then the newest. A
        warning says when there are more; narrow the list with ``search``.

        Args:
            search: Keep only frameworks whose name, order name, insight or
                companies contain this text.
            framework_ids: List these frameworks instead, by id, trainable
                or not. Not with ``search``.
            include_untrainable: Also list the frameworks Train can't train
                on.

        Returns:
            One dict per framework: ``id``, ``name``, ``trainable``,
            ``trainability`` (``yes``, ``no`` or ``not_negotiated``),
            ``trainingNote``, ``grant``, ``series``, ``insights``,
            ``metrics`` (the measures a run can predict), ``windowStart``,
            ``windowEnd`` and more.

        Raises:
            TrainNotEnabled: Train isn't turned on for this account.
        """
        if search is not None and framework_ids is not None:
            raise ValueError("pass search or framework_ids, not both")
        params: Dict[str, Any] = {}
        if search is not None:
            params["q"] = search
        if framework_ids is not None:
            params["ids"] = list(framework_ids)
        elif not include_untrainable:
            params["trainable"] = "true"
        catalog = self._train_request("GET", "/frameworks", params=params)
        frameworks = catalog.get("frameworks", [])
        total = catalog.get("total", len(frameworks))
        left_out = catalog.get("leftOutTotal") or 0
        if left_out:
            reasons = "".join(
                f"\n- {len(group.get('frameworks') or [])}: {group.get('reason')}"
                for group in catalog.get("leftOut") or []
            )
            warnings.warn(
                f"Left out {left_out} framework{'' if left_out == 1 else 's'} "
                "Train can't train on. Pass include_untrainable=True to list "
                f"them.{reasons}",
                stacklevel=2,
            )
        if total > len(frameworks):
            warnings.warn(
                f"Listing {len(frameworks)} of {total} frameworks. "
                "Pass search= to narrow the list.",
                stacklevel=2,
            )
        return frameworks

    def get_framework_sample(
        self, framework_id: str, series_by: Optional[str] = None
    ) -> dict:
        """See what a framework's data looks like before you train on it.

        Args:
            framework_id: Id of a framework your account bought.
            series_by: For a purchase split by a filter (a state, a channel):
                ``"company"`` for one series per company, or ``"split"`` for
                one per company in each split. None: the shape a run trains
                by default.

        Returns:
            Dict with the newest ``rows`` and their ``columns``, each
            company's monthly values (``series``), and the ways a run could
            train it (``shapes``, each with a ``note`` when it can't).

        Raises:
            TrainNotFound: Your account didn't buy that framework, or Train
                isn't turned on for it.
        """
        params = {"seriesBy": series_by} if series_by is not None else None
        return self._train_request(
            "GET", f"/frameworks/{framework_id}/sample", params=params, has_id=True
        )

    def get_features(
        self,
        framework_ids: List[str],
        target: str,
        window_start: str,
        cutoff: str,
        horizon: Optional[int] = None,
        series_by: Optional[str] = None,
    ) -> dict:
        """Score the measures in your frameworks as features for a target.

        Args:
            framework_ids: Ids of frameworks your account bought.
            target: The measure to predict, as ``"<framework id>:<metric>"``
                (metric keys are in each framework's ``metrics``).
            window_start: First month the scores read, ``YYYY-MM``.
            cutoff: Last month the scores read, ``YYYY-MM``.
            horizon: Months ahead the run forecasts. Each feature is scored
                read this many months back. None: power-api's default.
            series_by: As in :meth:`get_framework_sample`.

        Returns:
            Dict with ``scores`` keyed by metric id (each with ``coverage``,
            ``score`` and ``suggested``), ``lagMonths``, and ``scored``:
            false when the data couldn't be read, with ``note`` saying why.

        Raises:
            TrainNotEnabled: Train isn't turned on for this account.
        """
        return self._train_request(
            "POST",
            "/features",
            json=_features_body(
                framework_ids, target, window_start, cutoff, horizon, series_by
            ),
        )

    def get_suggestions(
        self,
        framework_ids: List[str],
        target: str,
        window_start: str,
        cutoff: str,
        horizon: Optional[int] = None,
        series_by: Optional[str] = None,
    ) -> dict:
        """Find data in Builder you don't own yet that could help predict a
        target.

        Takes the same arguments as :meth:`get_features`.

        Returns:
            Dict with ``suggestions``, best first (each with ``name``,
            ``insightId``, ``coverage``, ``score``, ``reason``, Builder's
            ``price`` in tokens and the ``framework`` it priced), and
            ``scored``: false when the lookup couldn't be made, with
            ``note`` saying why. Buy a suggestion with
            ``client.explorer.buy_frameworks(suggestion["framework"])``: it
            buys exactly what ``price`` quoted.

        Raises:
            TrainNotEnabled: Train isn't turned on for this account.
        """
        return self._train_request(
            "POST",
            "/suggestions",
            json=_features_body(
                framework_ids, target, window_start, cutoff, horizon, series_by
            ),
        )

    @staticmethod
    def frameworks_to_dataframe(frameworks: List[dict]) -> pd.DataFrame:
        """Same as the module's :func:`frameworks_to_dataframe`."""
        return frameworks_to_dataframe(frameworks)

    # -- HTTP -----------------------------------------------------------------

    def _train_request(
        self, method: str, path: str, has_id: bool = False, **kwargs
    ) -> Any:
        """Call the Train API and return its JSON, raising a ``TrainError``
        for a refusal power-api names.

        ``has_id`` says the path names a framework, run or model: its 404 is
        then :class:`TrainNotFound` rather than :class:`TrainNotEnabled`,
        since power-api answers both with the same 404.

        This bypasses the shared ``HttpRequestManager`` status handling,
        which turns every 409 into a bare "Conflict error" and drops the
        body Train's refusals carry. Anything Train doesn't name still goes
        through it, so 401s and other errors behave as elsewhere in the SDK.
        """
        manager = self.request_manager
        response = manager.request_session.request(
            method, self._base_url + path, auth=manager.auth_token, **kwargs
        )
        if response.ok:
            return response.json() if response.content else {}
        refusal = _refusal(response, has_id)
        if refusal is not None:
            raise refusal
        manager._raise_for_status(response)
        # _raise_for_status raises for every status that isn't ok.
        raise AssertionError("unreachable")  # pragma: no cover


def _features_body(
    framework_ids: List[str],
    target: str,
    window_start: str,
    cutoff: str,
    horizon: Optional[int],
    series_by: Optional[str],
) -> dict:
    body: Dict[str, Any] = {
        "selectedIds": list(framework_ids),
        "target": target,
        "windowStart": window_start,
        "cutoff": cutoff,
    }
    if horizon is not None:
        body["horizon"] = horizon
    if series_by is not None:
        body["seriesBy"] = series_by
    return body


def _refusal(response: requests.Response, has_id: bool) -> Optional[TrainError]:
    """The ``TrainError`` for a refused response, or None when it isn't a
    refusal Train names (it then raises as anywhere else in the SDK)."""
    try:
        body = response.json()
    except ValueError:
        body = None
    if not isinstance(body, dict):
        body = {}
    code = body.get("error_code")
    detail = body.get("detail")
    # A 409's detail is an object: {message, existingRunId, ...}.
    extra = detail if isinstance(detail, dict) else {}
    message = extra.get("message") if extra else detail
    if not isinstance(message, str) or not message:
        message = None

    if response.status_code == HTTPStatus.NOT_FOUND:
        if has_id:
            return TrainNotFound(
                (message or "Not found").rstrip(".")
                + ". It doesn't exist, isn't yours, or Train isn't enabled"
                " for this account.",
                status_code=response.status_code,
                response=response,
                error_code=code,
            )
        return TrainNotEnabled(
            TRAIN_NOT_ENABLED_MESSAGE,
            status_code=response.status_code,
            response=response,
            error_code=code,
        )

    cls = _REFUSALS.get(code) if code else None
    if cls is None:
        cls = _REFUSALS_BY_STATUS.get(response.status_code)
    if cls is None and response.status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
        cls = TrainPlatformError
    if cls is None:
        return None

    kwargs: Dict[str, Any] = dict(
        status_code=response.status_code, response=response, error_code=code
    )
    message = message or f"The Train request failed (HTTP {response.status_code})"
    if cls is TrainRunExists:
        kwargs["existing_run_id"] = body.get("existingRunId") or extra.get(
            "existingRunId"
        )
    if cls is TrainBusy:
        kwargs["retry_after"] = _retry_after(response)
    return cls(message, **kwargs)


def _retry_after(response: requests.Response) -> Optional[int]:
    value = response.headers.get("Retry-After")
    try:
        return int(value) if value is not None else None
    except ValueError:
        return None


# -- DataFrames ------------------------------------------------------------------


def _not_trainable_reason(framework: dict) -> Optional[str]:
    """Why a framework can't be trained, in the New model page's words
    (``lockedReason`` in power-ui's ``NewModelPage.tsx``); None when it can."""
    if not framework.get("trainable"):
        supplier = framework.get("supplierName") or "Unattributed supplier"
        if framework.get("trainability") == "not_negotiated":
            return (
                "This data has no training grant yet. You can still query and chart it."
            )
        return (
            f"{supplier}'s terms don't allow training a model on it. "
            "You can still query and chart it."
        )
    # Blank counts as no note, as on the page (``canSelect`` reads it falsy).
    return framework.get("trainingNote") or None


_FRAMEWORK_COLUMNS = [
    "id",
    "name",
    "trainable",
    "reason",
    "series",
    "insights",
    "metrics",
    "window_start",
    "window_end",
    "dataset_name",
    "panel",
    "supplier_name",
]


def frameworks_to_dataframe(frameworks: List[dict]) -> pd.DataFrame:
    """One row per framework from :meth:`TrainAPIClient.list_frameworks`.

    ``trainable`` is what the New model page lets you pick: the training
    rights allow it and Train can build training data from it. ``reason``
    says why not, in the page's words, and is None when it can be trained.
    ``metrics`` lists the measures (metric keys) a run can predict.
    """
    rows = []
    for f in frameworks:
        reason = _not_trainable_reason(f)
        rows.append(
            {
                "id": f.get("id"),
                "name": f.get("name"),
                "trainable": reason is None,
                "reason": reason,
                "series": f.get("series"),
                "insights": f.get("insights"),
                "metrics": [m.get("key") for m in f.get("metrics") or []],
                "window_start": f.get("windowStart"),
                "window_end": f.get("windowEnd"),
                "dataset_name": f.get("datasetName"),
                "panel": f.get("panel"),
                "supplier_name": f.get("supplierName"),
            }
        )
    return pd.DataFrame(rows, columns=_FRAMEWORK_COLUMNS)
