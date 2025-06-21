from carbonarc.base.client import BaseAPIClient

class HubAPIClient(BaseAPIClient):
    """
    A client for interacting with the Carbon Arc Hub API.
    """

    def __init__(
        self, 
        token: str,
        host: str = "https://platform.carbonarc.co",
        version: str = "v2"
        ):
        """
        Initialize HubAPIClient with an authentication token and user agent.
        :param auth_token: The authentication token to be used for requests.
        :param host: The base URL of the Carbon Arc API.
        :param version: The API version to use.
        """
        super().__init__(token=token, host=host, version=version)
        
        self.base_hub_url = self._build_base_url("hub")

    def get_subscriptions(self) -> dict:
        """return dashboard subscriptions"""
        raise NotImplementedError("get_subscriptions is not implemented yet.")
    
    def get_webcontent_data(self) -> dict:
        raise NotImplementedError("get_webcontent_data is not implemented yet.")