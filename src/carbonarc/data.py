import os
from typing import Optional, Literal

from carbonarc.base import BaseAPIClient
from carbonarc.utils import is_valid_date

class DataAPIClient(BaseAPIClient):
    """
    A client for interacting with the Carbon Arc Data API.
    """

    def __init__(
        self, 
        token: str,
        host: str = "https://platform.carbonarc.co",
        version: str = "v2"
        ):
        """
        Initialize DataAPIClient with an authentication token and user agent.
        :param auth_token: The authentication token to be used for requests.
        :param host: The base URL of the Carbon Arc API.
        :param version: The API version to use.
        """
        super().__init__(token=token, host=host, version=version)
        
        self.base_data_url = self._build_base_url("data")

    # TODO: implement
    def get_datasources(
        self,
        delivered: Literal["table", "graph", "all"] = "all",
        ecosystem: Literal["demand", "logistics", "supply", "all"] = "all",
        allocation: Literal["wallet", "attention", "balancesheet", "all"] = "all",
        subject: Optional[str] = None,
        ) -> dict:
        """
        Get the list of datasources from the Carbon Arc API.
        :param delivered: The type of data delivered (table, graph, or all).
        :param ecosystem: The ecosystem to filter datasources by (demand, logistics, supply, or all).
        :param allocation: The type of allocation to filter datasources by (wallet, attention, balancesheet, or all).
        :param subject: The subject to filter datasources by (optional).
        :return: A dictionary containing the list of datasources.
        """
        endpoint = "datasources"
        url = f"{self.base_data_url}/{endpoint}"
        params = {
            "delivered": delivered,
            "ecosystem": ecosystem,
            "allocation": allocation
        }
        if subject:
            params["subject"] = subject
        return self._get(url, params=params)
    
    # TODO: implement
    def get_subjects(self) -> dict:
        """
        Get the subjects from the Carbon Arc API.
        :return: A dictionary containing the subjects.
        """
        endpoint = "subjects"
        url = f"{self.base_data_url}/{endpoint}"
        
        return self._get(url)
    
    # TODO: implement
    def get_topics(self, subject: Optional[str] = None) -> dict:
        """
        Get the topics from the Carbon Arc API.
        :param subject: The subject to filter topics by (optional).
        :return: A dictionary containing the topics.
        """
        endpoint = "topics"
        url = f"{self.base_data_url}/{endpoint}"
        params = {}
        if subject:
            params["subject"] = subject
        
        return self._get(url, params=params)
    
    def get_datasource_confidence(self):
        raise NotImplementedError("get_datasource_confidence is not implemented yet.")
    
    
    
    
    
    
    
    
    
    def get_alldata_data_identifiers(self) -> dict:
        """
        Get the list of data identifiers from the Carbon Arc API.
        :return: A dictionary containing the list of data identifiers.
        """
        endpoint = "data-identifiers"
        url = f"{self.base_data_url}/{endpoint}"
        
        return self._get(url)
    
    # GRAPH DATA
    # TODO:  Deprecate
    def get_graph_data_identifiers(self) -> dict:
        """
        Get the list of graph data identifiers from the Carbon Arc API.
        :return: A dictionary containing the list of graph data identifiers.
        """
        endpoint = "graphdata/data-identifiers"
        url = f"{self.base_data_url}/{endpoint}"
        return self._get(url)

    def get_alldata_metadata(self, data_identifier: str) -> dict:
        """
        Get the metadata for a specific data identifier from the Carbon Arc API.
        :param data_identifier: The identifier of the data to retrieve metadata for.
        :return: A dictionary containing the metadata for the specified data identifier.
        """
        endpoint = f"{data_identifier}/metadata"
        url = f"{self.base_data_url}/{endpoint}"
        
        return self._get(url)

    def get_alldata_sample(self, data_identifier: str) -> dict:
        """
        Get the sample data for a specific data identifier from the Carbon Arc API.
        :param data_identifier: The identifier of the data to retrieve sample data for.
        :return: A dictionary containing the sample data for the specified data identifier.
        """
        endpoint = f"{data_identifier}"
        url = f"{self.base_data_url}/{endpoint}"
        
        return self._get(url)

    def get_alldata_manifest(
        self, data_identifier: str,
        created_since: Optional[str] = None,
        updated_since: Optional[str] = None
    ) -> dict:
        """
        Get the manifest for a specific data identifier from the Carbon Arc API.
        :param data_identifier: The identifier of the data to retrieve manifest for.
        :param created_since: The filter for created timestamp. Format is YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS.
        :param updated_since: The filter by updated timestamp, modification_time field. Format is YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS.
        :return: A dictionary containing the manifest for the specified data identifier.
        """
        endpoint = f"{data_identifier}/manifest"
        url = f"{self.base_data_url}/{endpoint}"
        params = {}
        if created_since:
            # validate created_since format
            if not is_valid_date(created_since):
                raise ValueError("created_since must be in YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS format.")
            params["created_since"] = created_since
        if updated_since:
            # validate updated_since format
            if not is_valid_date(updated_since):
                raise ValueError("updated_since must be in YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS format.")
            params["updated_since"] = updated_since
        return self._get(url, params=params)

    def stream_alldata(
        self,
        url: str,
        chunk_size: int = 1024 * 1024 * 250,  # 250MB
    ):
        """
        Download a file stream from the Carbon Arc API.
        :param url: The URL of the file to download.
        :param chunk_size: The size of each chunk to download.
        :return: A generator yielding the raw stream of the file.
        """
        response = self.request_manager.get_stream(url)
        for chunk in response.iter_content(chunk_size=chunk_size):
            yield chunk

    def download_alldata_to_file(
        self, url: str, output_file: str, chunk_size: int = 1024 * 1024 * 250
    ):
        """
        Download data for a specific data identifier and save it to a file.
        :param url: The URL of the file to download.
        :param output_file: The path to the file where the data should be saved.
        :param chunk_size: The size of each chunk to download.
        """
        # check if output_file directory exists
        output_dir = os.path.dirname(output_file)
        if not os.path.exists(output_dir):
            raise FileNotFoundError(f"Output directory {output_dir} does not exist.")

        with open(output_file, "wb") as f:
            for chunk in self.stream_alldata(url, chunk_size):
                f.write(chunk)

