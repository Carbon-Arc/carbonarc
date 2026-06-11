import re
from typing import Iterable, Optional
from uuid import UUID

from carbonarc.utils.client import BaseAPIClient


# Block lag literals are ``<n><unit>`` with unit in {d, m, y} where m=30d
# and y=365d — same convention as ``parse_lag_to_days`` in
# ``cams/api/.../block_lag_resolution.py`` and ``lagToDays`` in
# ``power-ui/.../BlockDataModal.tsx``. Free-form tearsheet labels like
# ``"T + 7 Days"`` are also normalized so the SDK speaks one unit
# throughout — integer days — matching the request intake form.
_LAG_LITERAL_RE = re.compile(r"^\s*(\d+)\s*([dmy])\s*$", re.IGNORECASE)
_LAG_FREEFORM_RES: tuple[tuple[re.Pattern, int], ...] = (
    (re.compile(r"(\d+)\s*day", re.IGNORECASE), 1),
    (re.compile(r"(\d+)\s*week", re.IGNORECASE), 7),
    (re.compile(r"(\d+)\s*month", re.IGNORECASE), 30),
    (re.compile(r"(\d+)\s*year", re.IGNORECASE), 365),
)


def _lag_to_days(lag) -> Optional[int]:
    """Normalize any lag representation to an integer number of days.

      - ``None`` / ``""`` / ``"None"`` → ``0`` (most-current data)
      - SKU literal (``"7d"``, ``"6m"``, ``"1y"``) → ``n * {d:1, m:30, y:365}``
      - Free-form tearsheet labels (``"T + 7 Days"``, ``"1 week"``,
        ``"3 months"``, ``"2 years"``) → parsed via the first matched
        unit
      - Anything else → ``None`` (surface unparseable rather than throw,
        since the dataset-level catalog ``lag`` is curator-controlled)
      - ``int`` passthrough (idempotent)
    """
    if lag is None:
        return 0
    if isinstance(lag, int):
        return lag
    s = str(lag).strip()
    if not s or s.lower() == "none":
        return 0
    m = _LAG_LITERAL_RE.match(s)
    if m:
        n, unit = int(m.group(1)), m.group(2).lower()
        return n * {"d": 1, "m": 30, "y": 365}[unit]
    for pattern, multiplier in _LAG_FREEFORM_RES:
        match = pattern.search(s)
        if match:
            return int(match.group(1)) * multiplier
    return None


_ACCESS_STATUSES = {"APPROVED", "TRIAL_ACTIVE", "CONTRACTED"}
_PENDING_STATUSES = {"PENDING"}
_DENIED_STATUSES = {"DENIED"}

_INTERNAL_PENDING_STEPS = {"accounts", "legal", "engineering"}
_INTERNAL_REJECTION_SOURCES = {
    "carbonarc_accounts",
    "carbonarc_legal",
    "carbonarc_engineering",
}


def _simplify_internal_queue_step(request: dict) -> Optional[str]:
    """Collapse the server's ``internal_queue_step`` + ``rejection_source``
    into ``"pending"`` / ``"approved"`` / ``"rejected"`` / ``None``.

      - ``"rejected"`` — the request was rejected during one of Carbon Arc's
        internal-review steps.
      - ``"approved"`` — internal review fully approved
        (server step == ``complete``).
      - ``"pending"`` — currently sitting in one of the internal-review
        queues.
      - ``None`` — the request never entered internal review (rejected at
        a client-side step, or still upstream at
        ``pending_block_admin`` / ``pending_compliance``).
    """
    if request.get("rejection_source") in _INTERNAL_REJECTION_SOURCES:
        return "rejected"
    step = request.get("internal_queue_step")
    if step == "complete":
        return "approved"
    if step in _INTERNAL_PENDING_STEPS:
        return "pending"
    return None


def _absolutize_tear_sheet(row: dict, cams_host: str) -> dict:
    """Rewrite ``compliance_tear_sheet.download_url`` to an absolute URL.

    CAMS emits the download path relative to its own root
    (``/api/v1/block/datasets/{dataset_id}/compliance-tear-sheet``) so the
    catalog payload doesn't bake in a host. The SDK already knows the
    ``cams_host`` it's talking to, so resolve the path here — callers get a
    link they can hand to ``requests.get`` / a browser without first having
    to know which CAMS environment produced the row.
    """
    if not isinstance(row, dict):
        return row
    sheet = row.get("compliance_tear_sheet")
    if isinstance(sheet, dict):
        url = sheet.get("download_url")
        if isinstance(url, str) and url.startswith("/"):
            sheet["download_url"] = f"{cams_host.rstrip('/')}{url}"
    return row


def _strip_sku_fields(arn: dict) -> dict:
    """Drop SKU dims from a registered-ARN row. Position inside a
    per-SKU ``arns`` array already conveys ``dataset_id`` / ``cut`` /
    ``lag``, so repeating them on every row would be noise."""
    return {
        k: v for k, v in arn.items()
        if k not in {"scope", "catalog", "dataset_id", "cut", "lag"}
    }


def _reshape_cuts_with_arns(
    cuts: list[dict],
    dataset_id: str,
    all_arns: list[dict],
) -> list[dict]:
    """Replace each cut's ``lags`` / ``request_statuses`` pair with a
    SKU-centric ``skus`` array. Each SKU carries its lag, current request
    status, and the ARNs registered against ``(dataset_id, cut, lag)``.

    A SKU is ``(dataset_id, cut, lag)``; for a given dataset_id the SKU
    is uniquely identified by ``(cut, lag)``. The no-lag variant is
    represented with ``lag == None`` (server keys it as ``""`` in
    ``request_statuses`` and stores ``lag = NULL`` on ARN rows).
    """
    def _arn_matches(arn: dict, cut_value, lag_value) -> bool:
        if arn.get("scope") != "block":
            return False
        if str(arn.get("dataset_id")) != dataset_id:
            return False
        if (arn.get("cut") or None) != (cut_value or None):
            return False
        if (arn.get("lag") or None) != (lag_value or None):
            return False
        return True

    new_cuts: list[dict] = []
    for cut in cuts or []:
        cut_value = cut.get("cut")
        lags: list[str] = cut.get("lags") or []
        statuses: dict[str, str] = cut.get("request_statuses") or {}

        skus: list[dict] = []
        seen_lag_keys: set[str] = set()
        for lag in lags:
            lag_key = lag or ""
            seen_lag_keys.add(lag_key)
            skus.append({
                "lag_days": _lag_to_days(lag),
                "request_status": statuses.get(lag_key),
                "arns": [
                    _strip_sku_fields(a)
                    for a in all_arns
                    if _arn_matches(a, cut_value, lag)
                ],
            })
        # Surface the no-lag variant when the server only reports a status
        # under the empty-string key (lags list will be empty in that case).
        for lag_key, status in statuses.items():
            if lag_key in seen_lag_keys:
                continue
            lag_value = lag_key or None
            skus.append({
                "lag_days": _lag_to_days(lag_value),
                "request_status": status,
                "arns": [
                    _strip_sku_fields(a)
                    for a in all_arns
                    if _arn_matches(a, cut_value, lag_value)
                ],
            })

        new_cut = {
            k: v for k, v in cut.items()
            if k not in {"lags", "request_statuses"}
        }
        new_cut["skus"] = skus
        new_cuts.append(new_cut)
    return new_cuts


def _dataset_has_status(dataset: dict, statuses: Iterable[str]) -> bool:
    wanted = set(statuses)
    for cut in dataset.get("cuts", []) or []:
        for status in (cut.get("request_statuses") or {}).values():
            if status in wanted:
                return True
    return False


class BlockAPIClient(BaseAPIClient):
    """
    A client for Carbon Arc Block functionality:
    dataset discovery, trial-access request lifecycle, dataset pre-approvals,
    and S3 ARN management.
    """

    def __init__(
        self,
        token: str,
        host: str = "http://localhost:8080",
        cams_host: str = "http://localhost:8000",
        version: str = "v2",
    ):
        """All Block functionality is served by the CAMS API
        under ``/api/v1/block/*`` — dataset discovery, request lifecycle,
        pre-approvals, and ARN management. ``host`` is kept on the
        signature for parity with the other sub-clients but is unused;
        the data API hosts none of the Block endpoints today.
        """
        super().__init__(token=token, host=host, version=version)
        self._cams_host = cams_host.rstrip('/')
        self._v1_url = f"{self._cams_host}/api/v1/block"

    # ---- Phase 1: discovery -------------------------------------------------

    def list_datasets(self) -> dict:
        """
        List all Block datasets visible to the caller's client, with per-cut
        pricing, available lags, and per-(cut, lag) request statuses inline.

        Returns:
            Dict with key ``datasets`` (list of dataset entries). Each entry
            carries a ``cuts`` list whose items have ``request_statuses``
            mapping lag → one of NONE / PENDING / APPROVED / TRIAL_ACTIVE /
            CONTRACTED / DENIED, plus a ``compliance_tear_sheet`` block
            whose ``download_url`` is an absolute URL the caller can fetch
            directly.
        """
        response = self._get(f"{self._v1_url}/datasets")
        for d in response.get("datasets", []):
            _absolutize_tear_sheet(d, self._cams_host)
        return response

    def my_access(self) -> list[dict]:
        """Datasets the caller has active access to (approved, trial active,
        or contracted) on at least one cut/lag."""
        return [
            d for d in self.list_datasets().get("datasets", [])
            if _dataset_has_status(d, _ACCESS_STATUSES)
        ]

    def pending(self) -> list[dict]:
        """Datasets with an in-flight (pending) request on at least one
        cut/lag."""
        return [
            d for d in self.list_datasets().get("datasets", [])
            if _dataset_has_status(d, _PENDING_STATUSES)
        ]

    def available(self) -> list[dict]:
        """Datasets that are publicly visible (``status='ready'``) and have
        no active request status for the caller on any cut/lag — i.e.
        eligible to request."""
        out = []
        for d in self.list_datasets().get("datasets", []):
            if d.get("status") != "ready":
                continue
            if _dataset_has_status(
                d, _ACCESS_STATUSES | _PENDING_STATUSES | _DENIED_STATUSES
            ):
                continue
            out.append(d)
        return out

    def coming_soon(self) -> list[dict]:
        """Datasets announced but not yet released (``status='coming_soon'``).
        Preview-only — visible in the catalog with metadata and pricing, but
        not yet requestable. Excludes any the caller already has an
        access / pending / denied request against, mirroring
        :meth:`available`."""
        out = []
        for d in self.list_datasets().get("datasets", []):
            if d.get("status") != "coming_soon":
                continue
            if _dataset_has_status(
                d, _ACCESS_STATUSES | _PENDING_STATUSES | _DENIED_STATUSES
            ):
                continue
            out.append(d)
        return out

    def rejected(self) -> list[dict]:
        """Datasets with a rejected request on at least one cut/lag."""
        return [
            d for d in self.list_datasets().get("datasets", [])
            if _dataset_has_status(d, _DENIED_STATUSES)
        ]

    def request_history(self, dataset_id: str) -> list[dict]:
        """Per-request timeline of every Block request the caller's client
        has filed against ``dataset_id`` — whether the result is approved,
        in-flight, denied, contracted, or anything else. Works for any
        ``dataset_id`` the client has touched, regardless of whether the
        dataset is currently in ``my_access`` / ``pending`` / ``rejected``.

        Each request emits an ordered ``events`` list with one entry per
        observable step. Step keys and what they carry:

          - ``submitted``                — ``actor`` (requestor_email),
                                            ``at`` (created_at), ``use_case``
          - ``block_admin_approved``     — ``actor`` (approved_by_block_admin)
                                            [timestamp not stored separately]
          - ``compliance_approved``      — ``actor`` (approved_by_compliance)
                                            [timestamp not stored separately]
          - ``fully_approved``           — ``at`` (approved_at)
          - ``rejected``                 — ``actor`` (rejected_by),
                                            ``source`` (rejection_source —
                                            which step the rejection came
                                            from), ``reason``
                                            (rejection_message),
                                            ``at`` (updated_at; CAMS stamps
                                            updated_at on the rejection
                                            transition)
          - ``cancellation_requested``   — ``actor``, ``at``, ``reason``
          - ``trial_started``            — ``at`` (trial_start_date),
                                            ``trigger`` (ingestion vs. admin
                                            force-start)
          - ``first_ingestion``          — ``at`` (first_ingestion_at)
          - ``trial_ends``               — ``at`` (trial_end_date)

        A step is only emitted if the underlying field is populated.
        Returns an empty list if the client has never filed a request
        against this dataset.

        Args:
            dataset_id: CA-prefixed dataset identifier (e.g. ``"CA0031"``).
                Matched exactly — case-sensitive.
        """
        out: list[dict] = []
        for r in self.list_requests().get("requests", []):
            if str(r.get("dataset_id")) != dataset_id:
                continue
            events: list[dict] = []

            if r.get("requestor_email") or r.get("created_at"):
                events.append({
                    "step": "submitted",
                    "actor": r.get("requestor_email"),
                    "at": r.get("created_at"),
                    "use_case": r.get("use_case"),
                })

            if r.get("approved_by_block_admin"):
                events.append({
                    "step": "block_admin_approved",
                    "actor": r.get("approved_by_block_admin"),
                    "at": None,
                })

            if r.get("approved_by_compliance"):
                events.append({
                    "step": "compliance_approved",
                    "actor": r.get("approved_by_compliance"),
                    "at": None,
                })

            if r.get("approved_at"):
                events.append({
                    "step": "fully_approved",
                    "at": r.get("approved_at"),
                })

            if r.get("rejected_by") or r.get("rejection_message"):
                events.append({
                    "step": "rejected",
                    "actor": r.get("rejected_by"),
                    "source": r.get("rejection_source"),
                    "reason": r.get("rejection_message"),
                    "at": r.get("updated_at"),
                })

            if r.get("cancellation_requested_at"):
                events.append({
                    "step": "cancellation_requested",
                    "actor": r.get("cancellation_requested_by"),
                    "at": r.get("cancellation_requested_at"),
                    "reason": r.get("cancellation_request_reason"),
                })

            if r.get("trial_start_date"):
                events.append({
                    "step": "trial_started",
                    "at": r.get("trial_start_date"),
                    "trigger": r.get("trial_start_trigger"),
                })

            if r.get("first_ingestion_at"):
                events.append({
                    "step": "first_ingestion",
                    "at": r.get("first_ingestion_at"),
                })

            if r.get("trial_end_date"):
                events.append({
                    "step": "trial_ends",
                    "at": r.get("trial_end_date"),
                })

            out.append({
                "request_id": r.get("id"),
                "dataset_id": r.get("dataset_id"),
                "lag_days": _lag_to_days(r.get("lag")),
                "cut": r.get("cut"),
                "current_status": r.get("status"),
                "internal_queue_step": _simplify_internal_queue_step(r),
                "trial_duration_months": r.get("trial_duration_months"),
                "annual_price": r.get("annual_price"),
                "events": events,
            })
        return out

    def dataset_status(self, dataset_id: str) -> dict:
        """One-call summary of the caller's Block access for ``dataset_id``.

        Joins :meth:`list_datasets` (catalog metadata + current per-cut /
        per-lag statuses) with :meth:`list_requests` (full historical
        request timeline against this dataset) into a single dict. Returns
        ``None`` for ``catalog`` if the dataset is not visible to the
        caller's client, and an empty list for ``requests`` if no requests
        exist.

        Args:
            dataset_id: CA-prefixed dataset identifier (e.g. ``"CA0031"``).
                Matched exactly — case-sensitive.

        Returns:
            ::

                {
                    "dataset_id": "CA0031",
                    "catalog": {                       # from list_datasets
                        "label": "...",
                        "vendor": "...",
                        "description": "...",
                        "status": "ready" | "coming_soon" | ...,
                        "cuts": [
                            {
                                "cut": "...",
                                "lags": ["6m", ...],
                                "annual_price": "...",
                                "request_statuses": {"6m": "APPROVED", ...},
                                ...
                            },
                            ...
                        ],
                        ...
                    } or None,
                    "requests": [                      # from list_requests
                        {
                            "id": "...",
                            "status": "pending_block_admin" | ...,
                            "lag": "...", "cut": "...",
                            "requestor_email": "...",
                            "approved_by_block_admin": "...",
                            "approved_by_compliance": "...",
                            "created_at": "...", "updated_at": "...",
                            ...
                        },
                        ...
                    ],
                }
        """
        catalog_entry = next(
            (
                d for d in self.list_datasets().get("datasets", [])
                if str(d.get("dataset_id")) == dataset_id
            ),
            None,
        )
        # Strip the redundant ``dataset_id`` from the catalog dict — it's
        # already echoed at the top level — and reshape each cut's lags +
        # request_statuses into a per-SKU ``skus`` array with the
        # registered ARNs attached. ARNs are per-(dataset_id, cut, lag),
        # so the SKU axis is the natural place to surface them.
        if catalog_entry is not None:
            all_arns = self.list_arns().get("items", []) or []
            cuts = catalog_entry.get("cuts") or []
            reshaped_cuts = _reshape_cuts_with_arns(cuts, dataset_id, all_arns)
            # Strip the redundant ``dataset_id`` and the freeform ``lag``
            # string ("T + 7 Days") — both are echoed elsewhere in
            # normalized form (``lag_days`` below).
            lag_raw = catalog_entry.get("lag")
            catalog_entry = {
                k: v for k, v in catalog_entry.items()
                if k not in {"dataset_id", "cuts", "lag"}
            }
            # Normalize the dataset-level lag label to integer days — same
            # convention as the SKUs and request intake form.
            catalog_entry["lag_days"] = _lag_to_days(lag_raw)
            catalog_entry["cuts"] = reshaped_cuts
        all_requests = self.list_requests().get("requests", [])
        requests_for_dataset = []
        for r in all_requests:
            if str(r.get("dataset_id")) != dataset_id:
                continue
            simplified = {k: v for k, v in r.items() if k != "lag"}
            simplified["lag_days"] = _lag_to_days(r.get("lag"))
            simplified["internal_queue_step"] = _simplify_internal_queue_step(r)
            requests_for_dataset.append(simplified)
        return {
            "dataset_id": dataset_id,
            "catalog": catalog_entry,
            "requests": requests_for_dataset,
        }

    # ---- Phase 2: trial-access lifecycle ------------------------------------

    def request_trial(
        self,
        dataset_id: str,
        lag: Optional[str] = None,
        cut: Optional[str] = None,
        use_case: Optional[str] = None,
        additional_email_recipients: Optional[list[str]] = None,
        accepted_block_tou_version_id: Optional[str] = None,
    ) -> dict:
        """
        Submit a Block trial-access request for ``dataset_id``.

        Args:
            dataset_id: CA-prefixed dataset identifier (e.g. ``"CA0027"``).
            lag: Optional lag variant (e.g. ``"6m"``).
            cut: Optional cut variant.
            use_case: Optional rationale shown to reviewers.
            additional_email_recipients: Optional list of extra email
                addresses to copy on workflow notifications.
            accepted_block_tou_version_id: UUID (as string) of the per-dataset
                Block TOU version the requestor accepted. Required when a
                dataset-specific TOU exists.

        Returns:
            Dict describing the created request (id, status, dataset_id, ...).
        """
        body = {"dataset_id": dataset_id}
        if lag is not None:
            body["lag"] = lag
        if cut is not None:
            body["cut"] = cut
        if use_case is not None:
            body["use_case"] = use_case
        if additional_email_recipients is not None:
            body["additional_email_recipients"] = additional_email_recipients
        if accepted_block_tou_version_id is not None:
            body["accepted_block_tou_version_id"] = accepted_block_tou_version_id
        return _absolutize_tear_sheet(
            self._post(f"{self._v1_url}/requests", json=body),
            self._cams_host,
        )

    def list_requests(self) -> dict:
        """List all Block requests in the caller's client.

        Returns:
            Dict with ``requests`` (list of request rows), ``total``, and
            ``users_by_email`` (lookup table of requestor / approver details).
        """
        response = self._get(f"{self._v1_url}/requests")
        # The server returns the rows under ``items``; rename to
        # ``requests`` for the public SDK surface.
        response["requests"] = [
            _absolutize_tear_sheet(r, self._cams_host)
            for r in (response.pop("items", None) or [])
        ]
        return response

    def get_request(self, request_id: str) -> dict:
        """Fetch a single Block request by UUID."""
        return _absolutize_tear_sheet(
            self._get(f"{self._v1_url}/requests/{request_id}"),
            self._cams_host,
        )

    # ---- Phase 3: pre-approvals ---------------------------------------------
    # Approving / rejecting requests is deliberately not exposed here —
    # those decisions go through the platform UI, where the Block Admin /
    # Compliance Admin workflow lives.

    def list_preapprovals(self) -> dict:
        """List dataset IDs preapproved for the caller's client.

        Returns:
            Dict with ``items`` (list of dataset_id strings).
        """
        return self._get(f"{self._v1_url}/dataset-preapprovals")

    # Pre-approving / un-pre-approving a dataset is deliberately not exposed
    # here — that Compliance Admin decision goes through the platform UI.

    # ---- Phase 4a: S3 ARN management ----------------------------------------

    def register_sample_arns(self, arns: list[str]) -> dict:
        """Register sample-scoped IAM ARN(s). Grants access to all sample
        datasets the client is entitled to (present and future)."""
        return self._post(
            f"{self._v1_url}/arns/sample",
            json={"arns": arns},
        )

    def register_block_arns(
        self,
        arns: list[str],
        dataset_id: str,
        lag: Optional[str] = None,
        cut: Optional[str] = None,
    ) -> dict:
        """Register block-scoped IAM ARN(s) for exactly one resolved
        ``(dataset_id, lag, cut)`` SKU bucket."""
        body = {"arns": arns, "dataset_id": dataset_id}
        if lag is not None:
            body["lag"] = lag
        if cut is not None:
            body["cut"] = cut
        return self._post(f"{self._v1_url}/arns/block", json=body)

    def list_arns(self) -> dict:
        """List the caller's active registered ARNs (both sample and block
        scopes)."""
        return self._get(f"{self._v1_url}/arns")

    def bucket_path(
        self,
        dataset_id: str,
        lag: Optional[str] = None,
        cut: Optional[str] = None,
        catalog: str = "bulk",
    ) -> dict:
        """Get the S3 bucket path for a given SKU. ``catalog`` is ``"bulk"``
        (the default — exact SKU match) or ``"sample"`` (lag-agnostic)."""
        params = {"dataset_id": dataset_id, "catalog": catalog}
        if lag is not None:
            params["lag"] = lag
        if cut is not None:
            params["cut"] = cut
        return self._get(f"{self._v1_url}/arns/bucket-path", params=params)

    def deregister_arn(self, arn_id: str) -> dict:
        """Deregister a previously-registered ARN."""
        return self._delete(f"{self._v1_url}/arns/{arn_id}")
