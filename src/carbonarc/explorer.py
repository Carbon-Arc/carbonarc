import pandas as pd
from typing import Optional, Literal, Union, List, Dict, Any
import logging

from carbonarc.utils.timeseries import timeseries_response_to_pandas
from carbonarc.utils.client import BaseAPIClient
from carbonarc.utils.exceptions import InvalidConfigurationError

logger = logging.getLogger(__name__)


class ExplorerAPIClient(BaseAPIClient):
    """Client for interacting with the Carbon Arc Builder API."""

    def __init__(
        self,
        token: str,
        host: str = "https://api.carbonarc.co",
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

    def build_framework(
        self,
        entities: Union[List[Dict], Dict, str],
        insight: int,
        filters: Dict[str, Any],
        aggregate: Optional[Literal["sum", "mean"]] = None
    ) -> dict:
        """
        Build a framework payload for the API.

        Args:
            entities: List of entity dicts (with "carc_id" and "representation") or a representation string.
            insight: Insight ID.
            filters: Filters to apply.
            aggregate: Aggregation method ("sum" or "mean").

        Returns:
            Framework dictionary.
        """
        return {
            "entities": self._clean_entities(entities),
            "insight": self._clean_insight(insight),
            "filters": filters,
            "aggregate": aggregate
        }

    def _validate_framework(self, framework: dict):
        """
        Validate a framework dictionary for required structure.

        Args:
            framework: Framework dictionary.

        Raises:
            InvalidConfigurationError: If the framework is invalid.
        """
        
        framework["entities"] = self._clean_entities(framework["entities"])
        framework["insight"] = self._clean_insight(framework["insight"])
        
        if not isinstance(framework, dict):
            raise InvalidConfigurationError("Framework must be a dictionary. Use build_framework().")
        if "entities" not in framework:
            raise InvalidConfigurationError("Framework must have an 'entities' key.")
        entities = framework["entities"]
        if isinstance(entities, list):
            if not all(isinstance(entity, dict) for entity in entities):
                raise InvalidConfigurationError("Each entity in the list must be a dictionary.")
        elif isinstance(entities, dict):
            if entities.get("carc_name") != "*" or "representation" not in entities:
                raise InvalidConfigurationError(
                    "If entities is a dictionary, it must be of the form {'carc_name': '*', 'representation': ...}."
                )
        else:
            raise InvalidConfigurationError("Entities must be a list of dicts or a wildcard dictionary.")
        if not isinstance(framework["insight"], dict):
            raise InvalidConfigurationError("Insight must be a dictionary.")
        if "insight_id" not in framework["insight"]:
            raise InvalidConfigurationError("Insight must have an 'insight_id' key.")
        
        return framework
        
    @staticmethod
    def _clean_entities(entities: Union[List[Dict], Dict, str]) -> Union[List[Dict], Dict]:
        """
        Clean the entities list.
        """
        if isinstance(entities, str):
            return {"carc_name": "*", "representation": entities}
        
        elif isinstance(entities, dict) and "carc_name" in entities and "representation" in entities:
            return entities
        
        elif isinstance(entities, dict) and "representation" in entities and len(entities) == 1:
            return {"carc_name": "*", "representation": list(entities.values())[0]}
        
        elif isinstance(entities, dict):
            entities = [entities]

        for entity in entities:
            if "id" in entity:
                entity["carc_id"] = entity["id"]
            elif "entity_id" in entity:
                entity["carc_id"] = entity["entity_id"]
                
        # clean up extra keys
        for entity in entities:
            if "id" in entity:
                del entity["id"]
            elif "entity_id" in entity:
                del entity["entity_id"]
        
        return entities
    
    @staticmethod
    def _clean_insight(insight: Union[int, str, dict]) -> dict:
        """
        Clean the insight.
        """
        if isinstance(insight, int):
            return {"insight_id": insight}
        elif isinstance(insight, str):
            return {"insight_id": int(insight)}
        elif isinstance(insight, dict):
            if "id" in insight:
                insight["insight_id"] = insight["id"]
                del insight["id"]
            elif "carc_id" in insight:
                insight["insight_id"] = insight["carc_id"]
                del insight["carc_id"]
            return insight

    def collect_framework_filters(self, framework: dict) -> dict:
        """
        Retrieve available filters for a framework.

        Args:
            framework: Framework dictionary.

        Returns:
            Dictionary of available filters.
        """
        framework = self._validate_framework(framework)
        url = f"{self.base_framework_url}/filters"
        return self._post(url, json={"framework": framework})
    
    def check_framework_price(self, framework: dict) -> dict:
        """
        Check the price of a framework.

        Args:
            framework: Framework dictionary.

        Returns:
            Dictionary of available filters.
        """
        framework = self._validate_framework(framework)
        url = f"{self.base_framework_url}/order"
        price = self._post(url, json={"framework": framework}).get("price", None)
        
        return price

    def collect_framework_filter_options(self, framework: dict, filter_key: str) -> dict:
        """
        Retrieve options for a specific filter in a framework.

        Args:
            framework: Framework dictionary.
            filter_key: Filter key to retrieve options for.

        Returns:
            Dictionary of filter options.
        """
        framework = self._validate_framework(framework)
        url = f"{self.base_framework_url}/filters/{filter_key}/options"
        return self._post(url, json={"framework": framework})

    def buy_frameworks(self, order: Union[List[dict], dict]) -> dict:
        """
        Purchase one or more frameworks.

        Args:
            order: List of framework dictionaries to purchase.

        Returns:
            Dictionary with purchase information.
        """
        if isinstance(order, dict):
            order = [order]
        
        validated_order = []
        for framework in order:
            validated_order.append(self._validate_framework(framework))
        url = f"{self.base_framework_url}/buy"
        return self._post(url, json={"order": {"frameworks": validated_order}})

    def get_framework_data(
        self,
        framework_id: str,
        data_type: Optional[Literal["dataframe", "timeseries"]] = None,
        page: Optional[int] = None,
        size: Optional[int] = None,
        fetch_all: bool = True,
    ) -> Union[pd.DataFrame, dict]:
        """
        Retrieve data for a specific framework.

        Args:
            framework_id: Framework ID.
            page: Page number (default 1).
            size: Number of items per page (default 100).
            data_type: Data type to retrieve ("dataframe" or "timeseries").

        Returns:
            Data as a DataFrame, dictionary, or timeseries, depending on data_type.
        """
        endpoint = f"{framework_id}/data"
        if fetch_all:
            if page or size:
                logger.warning("Page and size are ignored when fetch_all is True")
            url = f"{self.base_framework_url}/{endpoint}?fetch_all=true"
        else:
            url = f"{self.base_framework_url}/{endpoint}?page={page}&size={size}"
        if data_type:
            url += f"&data_type={data_type}"
        if data_type == "dataframe":
            df = pd.DataFrame(self._get(url).get("data", {}))
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"]).dt.date
            return df
        elif data_type == "timeseries":
            return timeseries_response_to_pandas(response=self._get(url))
        else:
            return self._get(url)

    def stream_framework_data(
        self,
        framework_id: str,
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
                fetch_all=True,
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
    
