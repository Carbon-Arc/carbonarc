from typing import Optional, Union

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
        """
        Initialize CatalogAPIClient with an authentication token.

        Args:
            token: The authentication token to be used for requests.
            host: The base URL of the Carbon Arc API.
            version: The API version to use.
        """
        super().__init__(token=token, host=host, version=version)
        self.base_catalog_url = self._build_base_url("catalog")

    def list_assets(
        self,
        tier: Optional[Union[int, list]] = None,
        provider_id: Optional[str] = None,
        category: Optional[str] = None,
        visibility: Optional[str] = None,
    ) -> dict:
        """
        List approved IDO assets. Results are gated by the caller's plan_type.

        Args:
            tier: Filter by asset tier level (int or list of ints).
            provider_id: Filter by provider ID.
            category: Filter by asset category.
            visibility: Filter by visibility (e.g. 'public', 'private').

        Returns:
            Dictionary containing assets list and total count.
        """
        params = {k: v for k, v in {
            "tier": tier,
            "provider_id": provider_id,
            "category": category,
            "visibility": visibility,
        }.items() if v is not None}
        return self._get(f"{self.base_catalog_url}/assets", params=params)

    def get_asset(self, asset_id: str) -> dict:
        """
        Get full metadata, samples, and dictionary for a single asset.

        Args:
            asset_id: The ID of the asset to retrieve.

        Returns:
            Dictionary containing asset details including metadata, samples, and dictionary.
        """
        return self._get(f"{self.base_catalog_url}/assets/{asset_id}")

    def get_trending(self, limit: int = 20) -> dict:
        """
        Get assets sorted by vote count descending.

        Args:
            limit: Maximum number of trending assets to return (default 20).

        Returns:
            Dictionary containing trending assets with vote counts.
        """
        return self._get(
            f"{self.base_catalog_url}/assets/trending",
            params={"limit": limit},
        )
