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
        
        Args:
            token: The authentication token to be used for requests.
            host: The base URL of the Carbon Arc API.
            version: The API version to use.
        """
        super().__init__(token=token, host=host, version=version)
        
        self.base_hub_url = self._build_base_url("hub")
    
    def get_webcontent_feeds(self) -> dict:
        """
        Retrieve all webcontent feeds.
        """
        url = f"{self.base_hub_url}/webcontent"
        return self._get(url)
    
    def get_subscribed_feeds(self) -> dict:
        """
        Retrieve all subscribed webcontent feeds.
        """
        url = f"{self.base_hub_url}/webcontent/subscribed"
        return self._get(url)
    
    def get_webcontent_data(self, webcontent_name: str, page: int = 1, size: int = 100) -> dict:
        """
        Retrieve a webcontent feed by name.
        """
        url = f"{self.base_hub_url}/webcontent/{webcontent_name}?page={page}&size={size}"
        return self._get(url)