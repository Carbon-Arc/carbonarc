from typing import Literal, Optional, Union

from carbonarc.utils.client import BaseAPIClient


class CatalogAPIClient(BaseAPIClient):
    """
    A client for interacting with the Carbon Arc IDO Catalog API.
    """

    def __init__(
        self,
        token: str,
        host: str = "https://api.carbonarc.co",
        version: str = "v2",
    ):
        super().__init__(token=token, host=host, version=version)
        self.base_catalog_url = self._build_base_url("catalog")

    def list_assets(
        self,
        tier: Optional[Union[int, list]] = None,
        provider_id: Optional[str] = None,
        category: Optional[str] = None,
        visibility: Optional[str] = None,
        search: Optional[str] = None,
        data_type: Optional[str] = None,
        geography: Optional[str] = None,
        frequency: Optional[str] = None,
        sort: Optional[Literal["popularity", "newest"]] = None,
    ) -> dict:
        """
        List approved IDO assets. Results are gated by the caller's plan_type.

        Args:
            tier: Filter by asset tier (1, 2, or 3).
            provider_id: Filter by provider UUID.
            category: Filter by data category.
            visibility: Filter by visibility ('public', 'gated', 'locked').
            search: Case-insensitive title search.
            data_type: Filter by data type.
            geography: Filter by geography (exact match within array field).
            frequency: Filter by data frequency.
            sort: Sort order — 'popularity' (vote count desc) or 'newest' (published_at desc).

        Returns:
            Dict with 'assets' list and 'total' count.
        """
        params = {k: v for k, v in {
            "tier": tier,
            "provider_id": provider_id,
            "category": category,
            "visibility": visibility,
            "search": search,
            "data_type": data_type,
            "geography": geography,
            "frequency": frequency,
            "sort": sort,
        }.items() if v is not None}
        return self._get(f"{self.base_catalog_url}/assets", params=params)

    def get_asset(self, asset_id: str) -> dict:
        """
        Get full metadata, samples, and data dictionary for a single asset.

        Args:
            asset_id: UUID of the asset to retrieve.

        Returns:
            Dict with asset detail including metadata, samples, and dictionary.
        """
        return self._get(f"{self.base_catalog_url}/assets/{asset_id}")

    def get_trending(self, limit: int = 20) -> dict:
        """
        Get assets sorted by vote count descending.

        Args:
            limit: Max assets to return (1–100, default 20).

        Returns:
            List of assets with vote_count field.
        """
        return self._get(
            f"{self.base_catalog_url}/assets/trending",
            params={"limit": limit},
        )

    def toggle_vote(self, asset_id: str) -> dict:
        """
        Toggle an upvote on an asset (YouTube-style).
        Adds a vote if the caller hasn't voted; removes it if they have.

        Args:
            asset_id: UUID of the asset to vote on.

        Returns:
            Dict with 'asset_id', 'voted' (bool), and 'vote_count'.
        """
        return self._post(f"{self.base_catalog_url}/assets/{asset_id}/vote")

    def get_vote_status(self, asset_id: str) -> dict:
        """
        Get the caller's current vote status for an asset.

        Args:
            asset_id: UUID of the asset to check.

        Returns:
            Dict with 'asset_id', 'voted' (bool), and 'vote_count'.
        """
        return self._get(f"{self.base_catalog_url}/assets/{asset_id}/vote")

    def request_access(
        self,
        asset_id: str,
        request_type: Literal["access", "diligence", "compliance"] = "access",
    ) -> dict:
        """
        Submit an access request for a gated or locked asset.
        Only valid for non-public assets. Raises 409 if an active request already exists.

        Args:
            asset_id: UUID of the asset to request access to.
            request_type: Type of request — 'access', 'diligence', or 'compliance'.

        Returns:
            Dict with request id, status ('pending'), and routed_to.
        """
        return self._post(
            f"{self.base_catalog_url}/assets/{asset_id}/request-access",
            json={"request_type": request_type},
        )
