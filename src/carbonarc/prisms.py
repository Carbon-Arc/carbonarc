from typing import Optional

from carbonarc.utils.client import BaseAPIClient


class PrismAPIClient(BaseAPIClient):
    """Client for the Carbon Arc Prisms API.

    A prism is one standing framework computation — an insight plus a set of
    entities — recomputed as its upstream data advances. These methods read
    prisms at their **current published state**: the same numbers the public
    prisms page is showing right now, from the same composition, so the two
    cannot disagree.

    Any valid API token may read prisms. There is no entitlement to enable and
    no cost per call: prism computes are already paid for.

    Values are relative — year-over-year index levels and share-of-group
    percentages — never absolute spend, visits or downloads.
    """

    def __init__(self, token: str, host: str, version: str):
        super().__init__(token=token, host=host, version=version)
        self._base_url = f"{host.rstrip('/')}/{version}/prisms"

    def get_prism(self, prism_id: str) -> dict:
        """Get one prism at its current published snapshot.

        Args:
            prism_id: UUID of the prism.

        Returns:
            Dict with the prism's display copy (``title``, ``category``,
            ``question``, ``question_share``, ``note``, ``insight_id``,
            ``insight_label``, ``published_at``) and its current series
            (``window``, ``data_through``, ``entities``).

            Each entry in ``entities`` carries ``entity_id``,
            ``entity_representation``, ``entity_name`` and four series:
            ``mtd_yoy`` / ``mtd_share`` by day of the latest month with data,
            and ``hist_yoy`` / ``hist_share`` by month over the charted year.

            ``data_through``, not the end of ``window``, is what says how
            current the numbers are: it is the last day of real data and it
            lags when upstream sources are behind.

        Raises:
            A 404 for a prism that is unknown, not public, or has never
            published. Those are deliberately one answer.
        """
        return self._get(f"{self._base_url}/{prism_id}")

    def get_prisms(
        self,
        insight_id: Optional[int] = None,
        entity_id: Optional[int] = None,
        entity_representation: Optional[str] = None,
    ) -> dict:
        """Find prisms by the insight they compute or the entity they cover.

        Pass ``insight_id``, or ``entity_id`` (optionally narrowed by
        ``entity_representation``), or both to intersect them. At least one is
        required.

        **Expect an array of any length, including zero.** Several matches is
        the normal case, not an edge: one insight is usually cut across more
        than one entity group, and one entity appears in prisms for several
        insights. Nothing guarantees exactly one result, so do not write code
        that assumes it.

        Matching runs against what each prism is *configured* to compute rather
        than the series it last rendered. The two differ when the warehouse
        returned no rows for an entity: that entity is dropped from the chart
        while the prism is still, in every meaningful sense, about it.

        Args:
            insight_id: Return prisms computing this metric insight.
            entity_id: Return prisms covering this entity (carc_id).
            entity_representation: Qualifies ``entity_id``. An entity is
                (carc_id, representation) and a carc_id alone is ambiguous
                across representations, so omit this only when you mean the id
                in any of them.

        Returns:
            Dict with ``prisms`` — a list of the same objects
            :meth:`get_prism` returns, each at its current published snapshot.
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
        return self._get(self._base_url, params=params)
