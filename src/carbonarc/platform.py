from carbonarc.base import BaseAPIClient

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
        Initialize PlatformAPIClient with an authentication token and user agent.
        :param auth_token: The authentication token to be used for requests.
        :param host: The base URL of the Carbon Arc API.
        :param version: The API version to use.
        """
        super().__init__(token=token, host=host, version=version)
        
        self.base_platform_url = self._build_base_url("platform")

    def get_wallet_balance(self) -> dict:
        raise NotImplementedError("get_balance is not implemented yet.")
    
    def get_order_history(self) -> dict:
        raise NotImplementedError("get_order_history is not implemented yet.")
    
    def get_workbooks(self) -> dict:
        raise NotImplementedError("get_subscriptions is not implemented yet.")