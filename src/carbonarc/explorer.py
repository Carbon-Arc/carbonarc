import pandas as pd
from typing import Optional, Literal, Union, List, Dict, Any

from carbonarc.base.utils import timeseries_response_to_pandas
from carbonarc.base.client import BaseAPIClient
from carbonarc.base.exceptions import InvalidConfigurationError


class ExplorerAPIClient(BaseAPIClient):
    """Client for interacting with the Carbon Arc Builder API."""

    def __init__(
        self,
        token: str,
        host: str = "https://platform.carbonarc.co",
        version: str = "v2"
    ):
        """
        Initialize BuilderAPIClient.

        Args:
            token: Authentication token for requests.
            host: Base URL of the Carbon Arc API.
            version: API version to use.
        """
        super().__init__(token=token, host=host, version=version)
        self.base_framework_url = self._build_base_url("framework")

    @staticmethod
    def build_framework(
        entities: List[Dict],
        insight: int,
        filters: Dict[str, Any],
        aggregate: Optional[Literal["sum", "mean"]] = None
    ) -> dict:
        """
        Build a framework payload for the API.

        Args:
            entities: List of entity dicts (with "carc_id" and "representation").
            insight: Insight ID.
            filters: Filters to apply.
            aggregate: Aggregation method ("sum" or "mean").

        Returns:
            Framework dictionary.
        """
        return {
            "entities": entities,
            "insight": {"insight_id": insight},
            "filters": filters,
            "aggregate": aggregate
        }

    @staticmethod
    def _validate_framework(framework: dict):
        """
        Validate a framework dictionary for required structure.

        Args:
            framework: Framework dictionary.

        Raises:
            InvalidConfigurationError: If the framework is invalid.
        """
        if not isinstance(framework, dict):
            raise InvalidConfigurationError("Framework must be a dictionary. Use build_framework().")
        if "entities" not in framework:
            raise InvalidConfigurationError("Framework must have an 'entities' key.")
        if not isinstance(framework["entities"], list):
            raise InvalidConfigurationError("Entities must be a list.")
        if not all(isinstance(entity, dict) for entity in framework["entities"]):
            raise InvalidConfigurationError("Each entity must be a dictionary.")
        for entity in framework["entities"]:
            if "carc_id" not in entity:
                raise InvalidConfigurationError("Each entity must have a 'carc_id' key.")
            if "representation" not in entity:
                raise InvalidConfigurationError("Each entity must have a 'representation' key.")
        if not isinstance(framework["insight"], dict):
            raise InvalidConfigurationError("Insight must be a dictionary.")
        if "insight_id" not in framework["insight"]:
            raise InvalidConfigurationError("Insight must have an 'insight_id' key.")

    def collect_framework_filters(self, framework: dict) -> dict:
        """
        Retrieve available filters for a framework.

        Args:
            framework: Framework dictionary.

        Returns:
            Dictionary of available filters.
        """
        self._validate_framework(framework)
        url = f"{self.base_framework_url}/filters"
        return self._post(url, json={"framework": framework})

    def collect_framework_filter_options(self, framework: dict, filter_key: str) -> dict:
        """
        Retrieve options for a specific filter in a framework.

        Args:
            framework: Framework dictionary.
            filter_key: Filter key to retrieve options for.

        Returns:
            Dictionary of filter options.
        """
        self._validate_framework(framework)
        url = f"{self.base_framework_url}/filters/{filter_key}/options"
        return self._post(url, json={"framework": framework})

    
    def collect_framework_information(self, framework: dict) -> dict:
        """
        Retrieve metadata for a framework.

        Args:
            framework: Framework dictionary.

        Returns:
            Dictionary of framework metadata.
        """
        self._validate_framework(framework)
        url = f"{self.base_framework_url}/information"
        return self._post(url, json={"framework": framework})

    def buy_frameworks(self, order: List[dict]) -> dict:
        """
        Purchase one or more frameworks.

        Args:
            order: List of framework dictionaries to purchase.

        Returns:
            Dictionary with purchase information.
        """
        for framework in order:
            self._validate_framework(framework)
        url = f"{self.base_framework_url}/buy"
        return self._post(url, json={"order": {"frameworks": order}})

    def get_framework_data(
        self,
        framework_id: str,
        page: int = 1,
        page_size: int = 100,
        data_type: Optional[Literal["dataframe", "timeseries"]] = None,
    ) -> Union[pd.DataFrame, dict]:
        """
        Retrieve data for a specific framework.

        Args:
            framework_id: Framework ID.
            page: Page number (default 1).
            page_size: Number of items per page (default 100).
            data_type: Data type to retrieve ("dataframe" or "timeseries").

        Returns:
            Data as a DataFrame, dictionary, or timeseries, depending on data_type.
        """
        endpoint = f"{framework_id}/data"
        url = f"{self.base_framework_url}/{endpoint}?page={page}&size={page_size}"
        if data_type:
            url += f"&type={data_type}"
        if data_type == "dataframe":
            return pd.DataFrame(self._get(url).get("data", {}))
        elif data_type == "timeseries":
            return timeseries_response_to_pandas(response=self._get(url))
        else:
            return self._get(url)

    def stream_framework_data(
        self,
        framework_id: str,
        page_size: int = 100,
        data_type: Optional[Literal["dataframe", "timeseries"]] = None,
    ):
        """
        Iterate over all data for a framework, yielding each page.

        Args:
            framework_id: Framework ID.
            page_size: Number of items per page (default 100).
            data_type: Data type to yield ("dataframe" or "timeseries").

        Yields:
            Data for each page as a DataFrame, timeseries, or dictionary.
        """
        page = 1
        while True:
            response = self.get_framework_data(
                framework_id=framework_id,
                page=page,
                page_size=page_size,
            )
            if not response:
                break
            total_pages = response.get("pages", 0)
            if page > total_pages:
                break
            if data_type == "dataframe":
                yield pd.DataFrame(response.get("data", {}))
            elif data_type == "timeseries":
                yield timeseries_response_to_pandas(response=response)
            else:
                yield response
            page += 1
    
    def get_framework_metadata(self, framework_id: str) -> dict:
        """
        Retrieve metadata for a specific framework.

        Args:
            framework_id: Framework ID.

        Returns:
            Dictionary of framework metadata.
        """
        endpoint = f"{framework_id}/metadata"
        url = f"{self.base_framework_url}/{endpoint}"
        return self._get(url)
    
