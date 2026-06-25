from carbonarc.block import BlockAPIClient
from carbonarc.catalog import CatalogAPIClient
from carbonarc.data import DataAPIClient
from carbonarc.explorer import ExplorerAPIClient
from carbonarc.hub import HubAPIClient
from carbonarc.client import PlatformAPIClient
from carbonarc.ontology import OntologyAPIClient
from carbonarc.transcripts import TranscriptAPIClient


class CarbonArcClient:
    """
    A client for interacting with the Carbon Arc API.
    """

    def __init__(
        self,
        token: str,
        host: str = "https://api.carbonarc.co",
        cams_host: str = "https://app.carbonarc.co",
        version: str = "v2",
    ):
        """
        Initialize CarbonArcClient with an authentication token and user agent.

        Args:
            token (str): The authentication token to be used for requests.
            host (str): Base URL of the data API (power-api). Hosts
                ``/v2/*`` routes — data library, catalog, ontology, hub,
                explorer, platform/billing.
            cams_host (str): Base URL of the admin/auth API (CAMS). Hosts
                ``/api/v1/*`` routes — all of Block (dataset discovery,
                request lifecycle, pre-approvals, S3 ARN management,
                Polaris credential rotation). Used only by
                :class:`BlockAPIClient`. The two backends live on separate
                hostnames in every environment (prod, stage, dev, local).
            version (str): The data-API version to use.
        """
        self.block = BlockAPIClient(
            token=token, host=host, cams_host=cams_host, version=version
        )
        self.catalog = CatalogAPIClient(token=token, host=host, version=version)
        self.data = DataAPIClient(token=token, host=host, version=version)
        self.explorer = ExplorerAPIClient(token=token, host=host, version=version)
        self.hub = HubAPIClient(token=token, host=host, version=version)
        self.client = PlatformAPIClient(token=token, host=host, version=version)
        self.ontology = OntologyAPIClient(token=token, host=host, version=version)
        self.transcripts = TranscriptAPIClient(token=token, host=host, version=version)
