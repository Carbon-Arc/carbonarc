from typing import Optional, Literal, Union, List
import pandas as pd

import carbonarc.utils as Utils
from carbonarc.base import BaseAPIClient

class ExplorerAPIClient(BaseAPIClient):
    """
    A client for interacting with the Carbon Arc Explorer API.
    """

    def __init__(
        self, 
        token: str,
        host: str = "https://platform.carbonarc.co",
        version: str = "v2"
        ):
        """
        Initialize ExplorerAPIClient with an authentication token and user agent.
        :param auth_token: The authentication token to be used for requests.
        :param host: The base URL of the Carbon Arc API.
        :param version: The API version to use.
        """
        super().__init__(token=token, host=host, version=version)
        
        self.base_explorer_url = self._build_base_url("explorer")
    
    # TODO: implement  
    def get_insights_from_subject_or_topic(
        self,
        subject_id: Optional[int] = None,
        topic_id: Optional[int]= None,
    ):
        """
        Get insights from the Carbon Arc API based on subjects or topics.
        :param subjects: A list of subjects to filter insights by (optional).
        :param topics: A list of topics to filter insights by (optional).
        :return: A dictionary containing the insights.
        """
        endpoint = "/insights"
        url = f"{self.base_explorer_url}/{endpoint}"
        
        if not subject_id and not topic_id:
            raise ValueError("At least one of subject_id or topic_id must be provided.")
        
        params = {}
        if subject_id:
            params["subjects"] = subject_id
        if topic_id:
            params["topics"] = topic_id
        
        return self._get(url, params=params)
    
    # TODO: implement 
    def get_entities_from_subject_or_topic(
        self,
        subject_id: Optional[int] = None,
        topic_id: Optional[int] = None,
    ):
        """
        Get entities from the Carbon Arc API based on subjects or topics.
        :param subject_id: The ID of the subject to filter entities by (optional).
        :param topic_id: The ID of the topic to filter entities by (optional).
        :return: A dictionary containing the entities.
        """
        endpoint = "/entities"
        url = f"{self.base_explorer_url}/{endpoint}"
        
        if not subject_id and not topic_id:
            raise ValueError("At least one of subject_id or topic_id must be provided.")
        
        params = {}
        if subject_id:
            params["subjects"] = subject_id
        if topic_id:
            params["topics"] = topic_id
        
        return self._get(url, params=params)
    
    # TODO: implement 
    def get_insights_from_entity(
        self,
        entity_id: int,
        ):
        """
        Get insights from the Carbon Arc API based on a specific entity.
        :param entity: The entity to filter insights by.
        :return: A dictionary containing the insights for the specified entity.
        """
        endpoint = f"/insights/entity/{entity_id}"
        url = f"{self.base_explorer_url}/{endpoint}"
        return self._get(url)
    
    # TODO: implement 
    def get_entities_from_insight(
        self,
        insight_id: int,
    ):
        """
        Get entities from a specific insight in the Carbon Arc API.
        :param insight_id: The ID of the insight to retrieve entities for.
        :return: A dictionary containing the entities for the specified insight.
        """
        endpoint = f"/insights/{insight_id}/entities"
        url = f"{self.base_explorer_url}/{endpoint}"
        return self._get(url)
    
    # TODO: implement
    def get_insight_information(self, insight_id: int,) -> dict:
        """
        Get the information for a specific insight from the Carbon Arc API.
        :param insight_id: The ID of the insight to retrieve information for.
        :return: A dictionary containing the information for the specified insight.
        """
        endpoint = f"/insights/{insight_id}"
        url = f"{self.base_explorer_url}/{endpoint}"
        
        return self._get(url)


    # TODO: implement
    def get_insight_filters(
        self,
        insight_id: int,
    ):
        """
        Get the filters for a specific insight from the Carbon Arc API.
        :param insight_id: The ID of the insight to retrieve filters for.
        :return: A dictionary containing the filters for the specified insight.
        """
        endpoint = f"/insights/{insight_id}/filters"
        url = f"{self.base_explorer_url}/{endpoint}"
        
        return self._get(url)
    
    # TODO: test
    @staticmethod
    def _build_framework_payload(framework: Union[dict, List[dict]]) -> dict:
        if isinstance(framework, dict):
            framework = [framework]
            
        for f in framework:
            if not isinstance(f, dict):
                raise ValueError("Each framework must be a dictionary with a 'hash' key.")
            if "insight" not in f:
                raise ValueError("Each framework must contain an 'insight' key.")
            if "entities" not in f:
                raise ValueError("Each framework must contain an 'entities' key.")
        
        return {"frameworks": framework}
    
    # TODO: test
    def collect_framework_information(
        self,
        framework: Union[dict, List[dict]]
    ) -> dict:
        """
        Get the data for a specific framework from the Carbon Arc API.
        :param framework: The framework or list of frameworks to retrieve data for.
        :return: A dictionary containing the information for the specified framework.
        """
        endpoint = "/framework/metadata"
        url = f"{self.base_explorer_url}/{endpoint}"
        
        return self._post(url, json=self._build_framework_payload(framework=framework))
    
    # TODO: test
    def buy_framework(
        self,
        framework: Union[dict, List[dict]]
    ):
        """
        Buy a framework from the Carbon Arc API.
        :param framework_hash: The hash of the framework to buy.
        :return: A dictionary containing the response from the API.
        """


        endpoint = "/framework/buy"
        url = f"{self.base_explorer_url}/{endpoint}"
        
        return self._post(url, json=self._build_framework_payload(framework=framework))
    
    # TODO: test
    def get_framework_data(
        self,
        framework_hash: str,
        page: int = 1,
        page_size: int = 100,
    ) -> dict:
        """
        Get the data for a specific framework from the Carbon Arc API.
        :param framework_hash: The hash of the framework to retrieve data for.
        :param page: The page number to retrieve.
        :param page_size: The number of items per page.
        :return: A dictionary containing the data for the specified framework.
        """
        endpoint = f"/frameworks/{framework_hash}/data"
        url = f"{self.base_explorer_url}/{endpoint}?page={page}&size={page_size}"
        
        return self._get(url)

    # TODO: test
    def get_framework_dataframe(
        self,
        framework_hash: str,
        page: int = 1,
        page_size: int = 100,
    ) -> pd.DataFrame:
        """
        Get the data for a specific framework from the Carbon Arc API.
        :param framework_hash: The hash of the framework to retrieve data for.
        :param page: The page number to retrieve.
        :param page_size: The number of items per page.
        :return: A dictionary containing the data for the specified framework.
        """
        endpoint = f"/frameworks/{framework_hash}/data"
        url = f"{self.base_explorer_url}/{endpoint}?page={page}&size={page_size}"
        
        return pd.DataFrame(self._get(url).get("data", {}))
    
    # TODO: test
    def get_framework_metadata(
        self,
        framework_hash: str,
    ) -> dict:
        """
        Get the data for a specific framework from the Carbon Arc API.
        :param framework_hash: The hash of the framework to retrieve data for.
        :return: A dictionary containing the data for the specified framework.
        """
        endpoint = f"/frameworks/{framework_hash}/metadata"
        url = f"{self.base_explorer_url}/{endpoint}"
        
        return self._get(url)
    

    
    
    
    
    
    
    
    
    
    
    def _build_insight_filter_values_url(
        self, data_identifier: str, filter_key: str
    ) -> str:
        return (
            self.base_explorer_url
            + f"/insights/{data_identifier}/filters/{filter_key}"
        )

    def _build_insight_data_identifiers_url(self, page: int = 1, page_size: int = 100) -> str:
        return (
            self.base_explorer_url
            + f"/data/data-identifiers?page={page}&size={page_size}"
        )

    def _build_insight_data_url(
        self,
        data_identifier: str,
        page: int = 1,
        page_size: int = 100,
        data_type: Literal["dataframe", "timeseries"] = "dataframe",
        aggregation: Literal["sum", "mean", "avg"] = "sum",
    ) -> str:
        return (
            self.base_explorer_url
            + f"/insights/{data_identifier}?page={page}&size={page_size}&type={data_type}&aggregation={aggregation}"
        )
    
    def get_insights_data_identifiers(self, page: int = 1, page_size: int = 100) -> dict:
        """
        Get the list of data identifiers from the Carbon Arc API.
        :param page: The page number to retrieve.
        :param page_size: The number of items per page.
        :return: A dictionary containing the list of data identifiers.
        """
        # Build the URL for the data identifiers endpoint
        url = self._build_insight_data_identifiers_url(page=page, page_size=page_size)
        return self._get(url)

    def get_insight_metadata(self, data_identifier: str) -> dict:
        """
        Get the metadata for a specific data identifier from the Carbon Arc API.
        :param data_identifier: The identifier of the data to retrieve metadata for.
        :return: A dictionary containing the metadata for the specified data identifier.
        """
        endpoint = f"/insights/{data_identifier}/metadata"
        url = f"{self.base_explorer_url}/{endpoint}"
        
        return self._get(url)

    def _get_insight_filters(self, data_identifier: str) -> dict:
        """
        Get the filters for a specific data identifier from the Carbon Arc API.
        :param data_identifier: The identifier of the data to retrieve filters for.
        :return: A dictionary containing the filters for the specified data identifier.
        """
        endpoint = f"/insights/{data_identifier}/filters"
        url = f"{self.base_explorer_url}/{endpoint}"
        
        return self._get(url)
    
    def get_insight_filter_values(self, data_identifier: str, filter_key: str) -> dict:
        """
        Get the filter values for a specific data identifier from the Carbon Arc API.
        :param data_identifier: The identifier of the data to retrieve filter values for.
        :param filter_key: The key of the filter to retrieve values for.
        :return: A dictionary containing the filter values for the specified data identifier.
        """
        url = self._build_insight_filter_values_url(
            data_identifier=data_identifier, filter_key=filter_key
        )
        return self._get(url)

    def get_insight_data(
        self,
        data_identifier: str,
        payload: dict,
        page: int = 1,
        page_size: int = 100,
        data_type: Literal["dataframe", "timeseries"] = "dataframe",
        aggregation: Literal["sum", "mean", "avg"] = "sum",
    ) -> dict:
        """
        Get the data for a specific data identifier from the Carbon Arc API.
        :param data_identifier: The identifier of the data to retrieve.
        :param payload: The payload to send with the request.
        :param page: The page number to retrieve.
        :param page_size: The number of items per page.
        :param
        data_type: The type of data to retrieve.
        :param aggregation: The aggregation method to use.
        :return: A dictionary containing the data for the specified data identifier.
        """
        url = self._build_insight_data_url(
            data_identifier=data_identifier,
            page=page,
            page_size=page_size,
            data_type=data_type,
            aggregation=aggregation,
        )
        return self._post(url, json=payload)

    def iter_insight_data(
        self,
        data_identifier: str,
        payload: dict,
        page_size: int = 100,
        data_type: Literal["dataframe", "timeseries"] = "dataframe",
        aggregation: Literal["sum", "mean", "avg"] = "sum",
    ):
        """
        Iterate over the data for a specific data identifier from the Carbon Arc API.
        :param data_identifier: The identifier of the data to retrieve.
        :param payload: The payload to send with the request.
        :param page_size: The number of items per page.
        :param data_type: The type of data to retrieve.
        :param aggregation: The aggregation method to use.
        :return: A generator that yields the data for the specified data identifier.
        """
        page = 1
        while True:
            response = self.get_insight_data(
                data_identifier=data_identifier,
                payload=payload,
                page=page,
                page_size=page_size,
                data_type=data_type,
                aggregation=aggregation,
            )
            if not response:
                break  # Exit if the response is None or invalid
            total_pages = response.get("pages", 0)
            if page > total_pages:
                break
            yield response
            page += 1

    def get_insight_data_pandas(
        self,
        data_identifier: str,
        payload: dict,
        page: int = 1,
        page_size: int = 100,
        data_type: Literal["dataframe", "timeseries"] = "dataframe",
        aggregation: Literal["sum", "mean", "avg"] = "sum",
    ) -> pd.DataFrame:
        """
        Get the data for a specific data identifier from the Carbon Arc API.
        :param data_identifier: The identifier of the data to retrieve.
        :param payload: The payload to send with the request.
        :param page: The page number to retrieve.
        :param page_size: The number of items per page.
        :param data_type: The type of data to retrieve.
        :param aggregation: The aggregation method to use.
        :return: A pandas DataFrame containing the data for the specified data identifier.
        """
        response = self.get_insight_data(
            data_identifier=data_identifier,
            payload=payload,
            page=page,
            page_size=page_size,
            data_type=data_type,
            aggregation=aggregation,
        )
        if data_type == "dataframe":
            return pd.DataFrame(response["data"])
        elif data_type == "timeseries":
            return Utils.timeseries_response_to_pandas(response=response)

    def iter_insight_data_pandas(
        self,
        data_identifier: str,
        payload: dict,
        page_size: int = 100,
        data_type: Literal["dataframe", "timeseries"] = "dataframe",
        aggregation: Literal["sum", "mean", "avg"] = "sum",
    ):
        """
        Iterate over the data for a specific data identifier from the Carbon Arc API.
        :param data_identifier: The identifier of the data to retrieve.
        :param payload: The payload to send with the request.
        :param page_size: The number of items per page.
        :param data_type: The type of data to retrieve.
        :param
        aggregation: The aggregation method to use.
        :return: A generator that yields pandas DataFrames containing the data for the specified data identifier.
        """
        for response in self.iter_insight_data(
            data_identifier=data_identifier,
            payload=payload,
            page_size=page_size,
            data_type=data_type,
            aggregation=aggregation,
        ):
            if data_type == "dataframe":
                yield pd.DataFrame(response["data"])
            elif data_type == "timeseries":
                yield Utils.timeseries_response_to_pandas(response=response)


