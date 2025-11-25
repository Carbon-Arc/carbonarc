from optparse import Option
import os
import logging
from io import BytesIO
from typing import Optional, Literal, Tuple, Union, Dict, Any
from datetime import datetime
import base64

from carbonarc.utils.client import BaseAPIClient

log = logging.getLogger(__name__)


class DataAPIClient(BaseAPIClient):
    """
    A client for interacting with the Carbon Arc Data API.
    """

    def __init__(
        self,
        token: str,
        host: str = "https://api.carbonarc.co",
        version: str = "v2",
    ):
        """
        Initialize DataAPIClient with an authentication token and user agent.
        
        Args:
            token: The authentication token to be used for requests.
            host: The base URL of the Carbon Arc API.
            version: The API version to use.
        """
        super().__init__(token=token, host=host, version=version)

        self.base_data_url = self._build_base_url("library")

    def get_datasets(
        self,
    ) -> dict:
        url = f"{self.base_data_url}/data"

        return self._get(url)

    def get_dataset_information(self, dataset_id: str) -> dict:
        """
        Get the information for a specific dataset from the Carbon Arc API.
        
        Args:
            data_identifier (str): The identifier of the data to retrieve information for.
            
        Returns:
            dict: A dictionary containing the information for the specified dataset.
        """
        endpoint = f"data/{dataset_id}"
        url = f"{self.base_data_url}/{endpoint}"

        return self._get(url)
    
    def get_data_dictionary(self, 
                            dataset_id: str,
                            entity_topic_id: Optional[int] = None) -> dict:
        """
        Get the data dictionary for a specific dataset from the Carbon Arc API.
        
        Args:
            dataset_id (str): The identifier of the data to retrieve the data dictionary for.
            entity_topic_id (Optional[int]): The identifier of the entity topic to retrieve the data dictionary for. If not provided, all data dictionaries for the dataset will be returned.
            
        Returns:
            list: A list of dictionaries containing the data dictionary or data dictionaries for the specified dataset.    
        """
        endpoint = f"data/{dataset_id}/data-dictionary"
        url = f"{self.base_data_url}/{endpoint}"
        params = {}
        if entity_topic_id:
            params["entity_topic_id"] = entity_topic_id

        return self._get(url, params=params)

    def get_graphs(
        self,
    ) -> dict:
        url = f"{self.base_data_url}/graph"

        return self._get(url)

    def get_graph_information(self, graph_id: str) -> dict:
        """
        Get the information for a specific dataset from the Carbon Arc API.
        
        Args:
            data_identifier (str): The identifier of the data to retrieve information for.
            
        Returns:
            dict: A dictionary containing the information for the specified dataset.
        """
        endpoint = f"graph/{graph_id}"
        url = f"{self.base_data_url}/{endpoint}"

        return self._get(url)
    
    def get_graph_data(self, graph_id: str, download_type: Literal["csv", "json", "graphml"] = "csv") -> dict:
        """
        Get the information for a specific dataset from the Carbon Arc API.
        
        Args:
            data_identifier (str): The identifier of the data to retrieve information for.
            
        Returns:
            dict: A dictionary containing the information for the specified dataset.
        """
        endpoint = f"graph/{graph_id}/data"
        url = f"{self.base_data_url}/{endpoint}?download_type={download_type}"

        return self._get(url)

    def get_library_version_changes(
        self, 
        version: str = "latest",
        dataset_id: Optional[str] = None,
        topic_id: Optional[int] = None,
        entity_representation: Optional[str] = None,
        page: Optional[int] = None,
        size: Optional[int] = None,
        order: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Check if the data library version has changed for a specific dataset.

        Args:
            version: The version to check for changes against.
            dataset_id: The dataset id to check for changes against.
            topic_id: The topic id to check for changes against.
            entity_representation: The entity representation to check for changes against.
            page: The page number to check for changes against.
            size: The size of the page to check for changes against.
            order: The order of the query.

        Returns:
            A dictionary containing the changes in the data library version.
        """
        if page or size or order:
            size = size or 100
            page = page or 1
            order = order or "asc"

        params = {
            "version": version.replace("v", ""),
            "page": page,
            "size": size,
            "order": order
        }
        if dataset_id:
            params["dataset_id"] = dataset_id
        if topic_id:
            params["topic_id"] = topic_id
        if entity_representation:
            params["entity_representation"] = entity_representation

        url = f"{self.base_data_url}/data-library/version-changes"
        return self._get(url, params=params)