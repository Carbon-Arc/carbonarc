from carbonarc.data import DataAPIClient
from carbonarc.framework import FrameworkAPIClient
from carbonarc.hub import HubAPIClient
from carbonarc.platform import PlatformAPIClient
from carbonarc.ontology import OntologyAPIClient


class CarbonArcClient:
    """
    A client for interacting with the Carbon Arc API.
    """

    def __init__(
        self,
        token: str,
        host: str = "https://platform.carbonarc.co",
        version: str = "v2",
    ):
        """
        Initialize APIClient with an authentication token and user agent.
        :param auth_token: The authentication token to be used for requests.
        :param host: The base URL of the Carbon Arc API.
        :param version: The API version to use.
        """
        self.data = DataAPIClient(token=token, host=host, version=version)
        self.builder = FrameworkAPIClient(token=token, host=host, version=version)
        self.hub = HubAPIClient(token=token, host=host, version=version)
        self.platform = PlatformAPIClient(token=token, host=host, version=version)
        self.ontology = OntologyAPIClient(token=token, host=host, version=version)
