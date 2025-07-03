from carbonarc.data import DataAPIClient
from carbonarc.explorer import ExplorerAPIClient
from carbonarc.hub import HubAPIClient
from carbonarc.client import PlatformAPIClient
from carbonarc.ontology import OntologyAPIClient


class CarbonArcClient:
    """
    A client for interacting with the Carbon Arc API.
    """

    def __init__(
        self,
        token: str,
        host: str = "https://api.carbonarc.co",
        version: str = "v2",
    ):
        """
        Initialize CarbonArcClient with an authentication token and user agent.
        
        Args:
            token (str): The authentication token to be used for requests.
            host (str): The base URL of the Carbon Arc API.
            version (str): The API version to use.
        """
        self.data = DataAPIClient(token=token, host=host, version=version)
        self.explorer = ExplorerAPIClient(token=token, host=host, version=version)
        self.hub = HubAPIClient(token=token, host=host, version=version)
        self.client = PlatformAPIClient(token=token, host=host, version=version)
        self.ontology = OntologyAPIClient(token=token, host=host, version=version)
