from carbonarc.utils.client import BaseAPIClient


class PrismAPIClient(BaseAPIClient):
    """Client for the Carbon Arc Prisms API.

    A prism is one standing framework computation — an insight plus a set of
    entities — recomputed as its upstream data advances. The public prisms
    page renders the newest published compute; this client reads the whole
    recorded history behind it.

    Any valid API token may read prisms. There is no entitlement to enable and
    no cost per call: prism computes are already paid for.
    """

    def __init__(self, token: str, host: str, version: str):
        super().__init__(token=token, host=host, version=version)
        self._base_url = f"{host.rstrip('/')}/{version}/prisms"

    def get_prism_snapshots(
        self,
        prism_id: str,
        page: int = 1,
        size: int = 25,
    ) -> dict:
        """Get one page of a prism's distinct states, newest first.

        A prism's history is append-only: each successful compute adds a
        snapshot that is never rewritten. What you get back is the sequence of
        *distinct states* — a run of computes that all rendered the same
        series is collapsed to the one that first produced it, because the
        recompute pipeline can fire without its inputs having moved and about
        a third of stored computes are that kind of redundant. Snapshots are
        ordered by ``computed_at`` descending.

        Paged: the history is append-only and unbounded, and a prism can
        record several computes in one day, so a long-lived prism accumulates
        thousands of them. ``total`` counts the same collapsed set the pages
        walk, so you can size the walk before making it.

        Two properties of the result are worth reading together before you
        chart it:

        - **``compute_date`` is not unique.** It is the calendar day (ET) a
          compute represents, while ``computed_at`` is when the compute ran,
          and a single day can carry several computes that each changed the
          series — a scheduled tick that found new data, then a manual repair
          that restated it. For one row per day, keep the greatest
          ``computed_at`` per ``compute_date``. ``published`` marks the
          computes *intended* for the public page; ``platform_published_at``
          is what says one reached readers.
        - **``data_through`` can trail ``compute_date``.** It is the last day
          of real data inside the snapshot, and it lags when upstream sources
          are behind. Use it, not ``compute_date``, to judge how current a
          snapshot's series is.

        Only live prisms are readable; an unknown, draft or retired prism id
        raises a 404. Prism ids come from the public prisms page
        (``GET /v2/public-prisms``), whose entries carry ``prism_id``.

        Values are relative — year-over-year index levels and share-of-group
        percentages — never absolute spend, visits or downloads.

        Args:
            prism_id: UUID of the prism.
            page: Page number, 1-indexed (default ``1``).
            size: Computes per page, 1-100 (default ``25``).

        Returns:
            Dict with ``prism_id``, ``total`` (int, across all pages),
            ``page`` (int), ``size`` (int), and ``snapshots`` (list).

            Each snapshot carries ``id``, ``compute_date``, ``computed_at``,
            ``platform_published_at``, ``published``, ``data_through``,
            ``window_start``, ``window_end``, and ``snapshot`` — the series
            that compute produced.

            ``computed_at`` and ``platform_published_at`` are different
            moments and both matter: the first is when the framework purchase
            finished, the second is when the public prism page was first
            rewarmed with these numbers and a reader could see them. The gap
            between them is the publish lag. Because identical computes are
            collapsed, the stamp can come from a later compute of the same
            state than the ``computed_at`` reported.

            A ``platform_published_at`` timestamp proves the state reached
            readers. ``None`` does **not** prove the opposite — the stamp is
            newer than the history, so older computes simply do not carry one,
            and a stamp write can fail after a successful rewarm. Treat it as
            positive evidence only.

            Inside ``snapshot``, ``window`` is ``{"start": ..., "end": ...}``
            (the charted span, not a duration), and each entity in
            ``entities`` carries ``mtd_yoy`` / ``mtd_share`` by day of the
            latest month with data, plus ``hist_share`` / ``hist_yoy`` by
            month over the charted year.

            Several fields are nullable and worth guarding: ``snapshot`` is
            ``None`` on rows recorded before the series was captured,
            ``data_through`` on a compute that produced no dated data,
            ``platform_published_at`` on one with no visibility stamp, and
            ``window_start`` / ``window_end`` on rows recorded before the
            window was tracked. Only ``id``, ``compute_date``, ``computed_at``
            and ``published`` are always present.
        """
        return self._get(
            f"{self._base_url}/{prism_id}/snapshots",
            params={"page": page, "size": size},
        )
