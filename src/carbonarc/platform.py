from carbonarc.base.client import BaseAPIClient

class PlatformAPIClient(BaseAPIClient):
    """
    A client for interacting with the Carbon Arc Platform API.
    """

    def __init__(
        self, 
        token: str,
        host: str = "https://platform.carbonarc.co",
        version: str = "v2"
        ):
        """
        Initialize PlatformAPIClient.

        Args:
            token: Authentication token for requests.
            host: Base URL of the Carbon Arc API.
            version: API version to use.
        """
        super().__init__(token=token, host=host, version=version)
        
        self.base_platform_url = self._build_base_url("clients")

    def get_balance(self) -> dict:
        """
        Retrieve balance for the current user.
        """
        url = f"{self.base_platform_url}/me/balance"
        return self._get(url)
    
    def get_usage(self) -> dict:
        """
        Retrieve usage for the current user.
        """
        url = f"{self.base_platform_url}/me/usage"
        return self._get(url)
    
    def get_order_history(self) -> dict:
        """
        Retrieve order history.

        Returns:
            Dictionary of order history.
        """
        url = f"{self.base_platform_url}/me/orders"
        return self._get(url)
    
    def get_order_details(self, order_id: str) -> dict:
        """
        Retrieve details for a specific order.
        
        Args:
            order_id: ID of the order to retrieve details for.

        Returns:
            Dictionary of order details.
        """
        url = f"{self.base_platform_url}/orders/{order_id}"
        return self._get(url)
    
