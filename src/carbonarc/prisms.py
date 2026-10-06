from typing import Optional

import pandas as pd

from carbonarc.utils.client import BaseAPIClient


class PrismAPIClient(BaseAPIClient):
    """Client for the Carbon Arc Prisms API.

    A prism is one standing framework computation (an insight plus a set of
    entities) recomputed as its upstream data advances. These methods read
    prisms at their current published state, which is what the public prisms
    page is showing.

    Any valid API token may read prisms. There is no entitlement to enable and
    no cost per call.

    Prism values are relative: year-over-year index levels and share-of-group
    percentages, never absolute spend, visits or downloads.
    """

    def __init__(self, token: str, host: str, version: str):
        super().__init__(token=token, host=host, version=version)
        self._base_url = f"{host.rstrip('/')}/{version}/prisms"
        self._public_url = f"{host.rstrip('/')}/{version}/public-prisms"

    def get_prism(self, prism_id: str) -> dict:
        """Get one prism at its current published snapshot.

        Args:
            prism_id: UUID of the prism.

        Returns:
            Dict with the prism's display copy (``title``, ``category``,
            ``question``, ``question_share``, ``note``, ``insight_id``,
            ``insight_label``, ``published_at``), its current series
            (``window``, ``data_through``, ``entities``), and ``framework``.

            Each entry in ``entities`` carries ``entity_id``,
            ``entity_representation``, ``entity_name`` and four series:
            ``mtd_yoy`` / ``mtd_share`` by day of the latest month with data,
            and ``hist_yoy`` / ``hist_share`` by month over the charted year.

            ``data_through``, not the end of ``window``, is what says how
            current the numbers are: it is the last day of real data, and it
            lags when upstream sources are behind.

            ``framework`` is the framework that produced all of it, ready
            to purchase. See :meth:`list_prisms` for its shape and the one
            field you may have to add first.

        Raises:
            A 404 if no published prism matches that id.
        """
        return self._get(f"{self._base_url}/{prism_id}")

    def list_prisms(
        self,
        insight_id: Optional[int] = None,
        entity_id: Optional[int] = None,
        entity_representation: Optional[str] = None,
    ) -> dict:
        """Find prisms by the insight they compute or the entity they cover.

        Pass ``insight_id``, or ``entity_id`` (optionally narrowed by
        ``entity_representation``), or both to intersect them. With no
        arguments this returns the full catalog (:meth:`get_prism_catalog`),
        where prisms carry no ``framework``; the two shapes differ because
        that is what the API serves on each path.

        **Expect a list of any length, including zero.** Several matches is the
        normal case: one insight is usually cut across more than one entity
        group, and one entity appears in prisms for several insights. Nothing
        guarantees exactly one result, so do not write code that assumes it.

        A prism is matched on the entities and insight it is configured to
        compute. That can include an entity missing from its current chart,
        since an entity with no data for the charted period is left out of the
        series.

        Args:
            insight_id: Return prisms computing this metric insight.
            entity_id: Return prisms covering this entity (carc_id).
            entity_representation: Qualifies ``entity_id``. An entity is
                (carc_id, representation) and a carc_id alone is ambiguous
                across representations, so omit this only when you mean the id
                in any of them.

        Returns:
            Dict with ``prisms``, a list of the same objects
            :meth:`get_prism` returns, each at its current published snapshot.

            Every prism carries a ``framework``: the framework that produced
            its numbers, in the shape a purchase takes.

            .. code-block:: python

                {
                    "entities": [
                        {
                            "carc_id": 36003,
                            "representation": "service",
                            "entity_name": "Delta Air Lines",
                        }
                    ],
                    "insight": {
                        "insight_id": 626,
                        "insight_name": "Credit Card Spend",
                    },
                    "filters": {
                        "date_resolution": "day",
                        "location_resolution": "us",
                        "date_range": {
                            "start_date": "2024-09-01",
                            "end_date": "2026-09-16",
                        },
                    },
                    "aggregate": "sum",
                }

            Pass it through rather than rebuilding it field by field::

                framework = prism["framework"]

                # Absent only when the prism's window was not recorded.
                framework["filters"].setdefault(
                    "date_range",
                    {"start_date": "2024-09-01", "end_date": "2026-09-16"},
                )

                order = client.explorer.build_framework(**framework)
                purchase = client.explorer.buy_frameworks(order)

            Note the key names. A framework entity is ``carc_id`` /
            ``representation``, while the prism's own ``entities`` above are
            ``entity_id`` / ``entity_representation``. They are the same
            numbers under different names, and both are required on a
            purchase, so substituting the prism's spelling is rejected.

            ``filters.date_range`` is the window the data was bought with,
            which is wider than the charted ``window``: a year-over-year point
            needs the prior-year month in the same pull.
        """
        params = {
            key: value
            for key, value in (
                ("insight_id", insight_id),
                ("entity_id", entity_id),
                ("entity_representation", entity_representation),
            )
            if value is not None
        }
        if not params:
            return self.get_prism_catalog()
        return self._get(self._base_url, params=params)

    get_prisms = list_prisms

    def get_prism_catalog(self) -> dict:
        """Get the full prism catalog with your API token.

        The same payload :meth:`get_public_prisms` returns, read through the
        authenticated API instead of the public endpoint. It takes no
        parameters and returns every live prism in one response, so prefer
        :meth:`get_prism` or :meth:`list_prisms` when you want a single prism
        or a filtered set.

        Returns:
            Dict with ``prisms``, ``arrays`` and ``tou``, exactly as described
            on :meth:`get_public_prisms`. As there, the prisms in it do not
            carry ``framework``; read a prism with :meth:`get_prism` or
            :meth:`list_prisms` for that.
        """
        return self._get(f"{self._base_url}/catalog")

    def list_arrays(self) -> dict:
        """Every live Array. Same objects as ``arrays`` in
        :meth:`get_prism_catalog`; see :meth:`get_public_prisms` for the shape."""
        return self._get(f"{self._base_url}/arrays")

    def get_array(self, array_id: str) -> dict:
        """One live Array by ``array_id``. Raises on 404 like :meth:`get_prism`."""
        return self._get(f"{self._base_url}/arrays/{array_id}")

    def get_public_prisms(self) -> dict:
        """Get the public prism catalog.

        This endpoint needs no authentication and takes no parameters. It
        returns every live prism in one response, so prefer :meth:`get_prism`
        or :meth:`list_prisms` when you want a single prism or a filtered set.
        :meth:`get_prism_catalog` returns the same payload through the
        authenticated API.

        Returns:
            Dict with:

            - ``prisms``: every live prism, sorted by category then title, in
              the same shape :meth:`get_prism` returns but without
              ``framework``. Use the authenticated reads above for that.
            - ``arrays``: the live Arrays (see below). Read it as
              ``payload.get("arrays", [])``.
            - ``tou``: the Terms of Use governing use of this data.

            An Array is an equal-weight aggregation of one prism's entities
            into a single monthly growth series. It is not a prism and never
            appears in ``prisms``. Each carries ``array_id``,
            ``source_prism_id``, ``title``, ``category``, ``entities``
            (objects with ``entity_key`` and ``title``), ``entity_count``,
            ``level`` (the series, as ``{"month", "value"}``), ``coverage``,
            ``data_through`` and ``last_derived_at``.

            Two rules govern ``level``. A month carries a value only when every
            entity in the Array reported one; where an entity is missing the
            month stays in the series with a ``null`` value, and ``coverage``
            says which were missing. A month that is not yet complete is left
            out of the series entirely. So an absent month and a ``null`` month
            mean different things, and the last point is not necessarily the
            current month.
        """
        return self._get(self._public_url)


def prism_to_dataframe(prism: dict) -> pd.DataFrame:
    """Tidy rows, one per (entity, series, period).

    Columns: entity_id, entity_representation, entity_name, series, period,
    value. ``period`` is the point's ``date`` (daily ``mtd_*`` series) or
    ``month`` (monthly ``hist_*`` series)."""
    rows = []
    for entity in prism.get("entities", []):
        for series in ("mtd_yoy", "mtd_share", "hist_yoy", "hist_share"):
            for point in entity.get(series) or []:
                rows.append(
                    {
                        "entity_id": entity.get("entity_id"),
                        "entity_representation": entity.get("entity_representation"),
                        "entity_name": entity.get("entity_name"),
                        "series": series,
                        "period": point.get("date") or point.get("month"),
                        "value": point.get("value"),
                    }
                )
    return pd.DataFrame(rows)


def array_to_dataframe(array: dict) -> pd.DataFrame:
    """Tidy rows from ``level``: month, value, plus array_id and title."""
    return pd.DataFrame(
        {"array_id": array.get("array_id"), "title": array.get("title"), **point}
        for point in array.get("level", [])
    )
