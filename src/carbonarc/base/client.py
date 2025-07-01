import logging
from typing import Literal

from carbonarc.base.auth import TokenAuth
from carbonarc.base.manager import HttpRequestManager


class BaseAPIClient:
    """
    A client for interacting with the Carbon Arc API.
    """

    def __init__(
        self, 
        token: str,
        host: str = "https://api.carbonarc.co",
        version: str = "v2"
        ):
        """
        Initialize APIClient with an authentication token and user agent.
        :param auth_token: The authentication token to be used for requests.
        :param host: The base URL of the Carbon Arc API.
        :param version: The API version to use.
        """
        
        self.host = host
        self.version = version
        
        self._logger = logging.getLogger(__name__)
        
        self.auth_token = TokenAuth(token)
        self.request_manager = HttpRequestManager(auth_token=self.auth_token)

    def _build_base_url(
        self,
        product: Literal["clients", "framework", "library", "ontology", "hub"],
    ) -> str:
        
        url = f"{self.host.rstrip('/')}/{self.version}/{product}"
        
        return url
    
    def _get(self, url: str, **kwargs) -> dict:
        return self.request_manager.get(url, **kwargs).json()

    def _post(self, url: str, **kwargs) -> dict:
        return self.request_manager.post(url, **kwargs).json()