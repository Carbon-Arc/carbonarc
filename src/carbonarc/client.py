import os
import logging
from typing import Literal, Optional, List
import pandas as pd

from carbonarc.auth import TokenAuth
from carbonarc.exceptions import AuthenticationError
from carbonarc.manager import HttpRequestManager
from carbonarc.routes import Routes


class APIClient:
    """
    A client for interacting with the Carbon Arc Power API.
    """

    def __init__(
        self, auth_token: TokenAuth, user_agent: str = "Python-APIClient/0.1.0"
    ):
        """
        Initialize the PowerAPIClient with an authentication token and user agent.
        :param auth_token: The authentication token to be used for requests.
        :param user_agent: The user agent string to be used for requests.
        """

        self._logger = logging.getLogger(__name__)
        self.auth_token = auth_token
        self._routes = Routes()
        self.request_manager = HttpRequestManager(auth_token=self.auth_token)
        self.user_agent = user_agent

    def _get(self, url: str, **kwargs):
        return self.request_manager.get(url, **kwargs).json()

    def _post(self, url: str, **kwargs):
        return self.request_manager.post(url, **kwargs).json()

    def get_insights_data_idetifiers(self, page: int = 1, page_size: int = 100) -> dict:
        """
        Get the list of data identifiers from the Carbon Arc Power API.
        :param page: The page number to retrieve.
        :param page_size: The number of items per page.
        :return: A dictionary containing the list of data identifiers.
        """
        # Build the URL for the data identifiers endpoint
        url = self._routes._build_data_identifiers_url(page=page, page_size=page_size)
        return self._get(url)

    def get_insight_metadata(self, data_identifier: str) -> dict:
        """
        Get the metadata for a specific data identifier from the Carbon Arc Power API.
        :param data_identifier: The identifier of the data to retrieve metadata for.
        :return: A dictionary containing the metadata for the specified data identifier.
        """
        url = self._routes._build_data_identifier_metadata_url(
            data_identifier=data_identifier
        )
        return self._get(url)

    def get_insight_filters(self, data_identifier: str) -> dict:
        """
        Get the filters for a specific data identifier from the Carbon Arc Power API.
        :param data_identifier: The identifier of the data to retrieve filters for.
        :return: A dictionary containing the filters for the specified data identifier.
        """
        url = self._routes._build_data_identifiers_filters_url(
            data_identifier=data_identifier
        )
        return self._get(url)

    def get_insight_filter_values(self, data_identifier: str, filter_key: str) -> dict:
        """
        Get the filter values for a specific data identifier from the Carbon Arc Power API.
        :param data_identifier: The identifier of the data to retrieve filter values for.
        :param filter_key: The key of the filter to retrieve values for.
        :return: A dictionary containing the filter values for the specified data identifier.
        """
        url = self._routes._build_data_identifiers_filter_values_url(
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
        Get the data for a specific data identifier from the Carbon Arc Power API.
        :param data_identifier: The identifier of the data to retrieve.
        :param payload: The payload to send with the request.
        :param page: The page number to retrieve.
        :param page_size: The number of items per page.
        :param
        data_type: The type of data to retrieve.
        :param aggregation: The aggregation method to use.
        :return: A dictionary containing the data for the specified data identifier.
        """
        url = self._routes._build_data_identifiers_data_url(
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
    ) -> dict:
        """
        Iterate over the data for a specific data identifier from the Carbon Arc Power API.
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
        Get the data for a specific data identifier from the Carbon Arc Power API.
        :param data_identifier: The identifier of the data to retrieve.
        :param payload: The payload to send with the request.
        :param page: The page number to retrieve.
        :param page_size: The number of items per page.
        :param data_type: The type of data to retrieve.
        :param aggregation: The aggregation method to use.
        :return: A pandas DataFrame containing the data for the specified data identifier.
        """
        url = self._routes._build_data_identifiers_data_url(
            data_identifier=data_identifier,
            page=page,
            page_size=page_size,
            data_type=data_type,
            aggregation=aggregation,
        )
        resposne = self._post(url, json=payload)
        return pd.DataFrame(resposne["data"])

    def iter_insight_data_pandas(
        self,
        data_identifier: str,
        payload: dict,
        page_size: int = 100,
        data_type: Literal["dataframe", "timeseries"] = "dataframe",
        aggregation: Literal["sum", "mean", "avg"] = "sum",
    ) -> List[pd.DataFrame]:
        """
        Iterate over the data for a specific data identifier from the Carbon Arc Power API.
        :param data_identifier: The identifier of the data to retrieve.
        :param payload: The payload to send with the request.
        :param page_size: The number of items per page.
        :param data_type: The type of data to retrieve.
        :param
        aggregation: The aggregation method to use.
        :return: A generator that yields pandas DataFrames containing the data for the specified data identifier.
        """
        page = 1
        while True:
            data = self.get_insight_data_pandas(
                data_identifier=data_identifier,
                payload=payload,
                page=page,
                page_size=page_size,
                data_type=data_type,
                aggregation=aggregation,
            )
            if data.empty:
                break
            yield data
            page += 1

    def get_alldata_data_idetifiers(self) -> dict:
        """
        Get the list of data identifiers from the Carbon Arc Power API.
        :return: A dictionary containing the list of data identifiers.
        """
        url = self._routes._build_all_data_identifiers_url()
        return self._get(url)

    def get_alldata_data_metadata(self, data_identifier: str) -> dict:
        """
        Get the metadata for a specific data identifier from the Carbon Arc Power API.
        :param data_identifier: The identifier of the data to retrieve metadata for.
        :return: A dictionary containing the metadata for the specified data identifier.
        """
        url = self._routes._build_builk_identifier_metadata_url(
            data_identifier=data_identifier
        )
        return self._get(url)

    def get_alldata_data_sample(self, data_identifier: str) -> dict:
        """
        Get the sample data for a specific data identifier from the Carbon Arc Power API.
        :param data_identifier: The identifier of the data to retrieve sample data for.
        :return: A dictionary containing the sample data for the specified data identifier.
        """
        url = self._routes._build_builk_identifier_sample_url(
            data_identifier=data_identifier
        )
        return self._get(url)

    def get_alldata_data_manifest(self, data_identifier: str) -> dict:
        """
        Get the manifest for a specific data identifier from the Carbon Arc Power API.
        :param data_identifier: The identifier of the data to retrieve manifest for.
        :return: A dictionary containing the manifest for the specified data identifier.
        """
        url = self._routes._build_builk_identifier_manifest_url(
            data_identifier=data_identifier
        )
        return self._get(url)
    
    def get_alldata_data_stream(
        self,
        url: str,
        chunk_size: int = 1024 * 1024 * 250,  # 250MB
    ):
        """
        Download a file stream from the Carbon Arc Power API.
        :param url: The URL of the file to download.
        :param chunk_size: The size of each chunk to download.
        :return: A generator yielding the raw stream of the file.
        """
        response = self.request_manager.get_stream(url)
        for chunk in response.iter_content(chunk_size=chunk_size):
            yield chunk
    
    def download_alldata_data_file(self, url: str, output_file: str, chunk_size: int = 1024 * 1024 * 250):
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
        
        with open(output_file, 'wb') as f:
            for chunk in self.download_alldata_data_stream(url, chunk_size):
                f.write(chunk)

    def download_alldata_data_to_file(
        self,
        url: str,
        output_dir: str,
        filename: Optional[str] = None,
        chunk_size: int = 1024 * 1024 * 250,  # 250MB
    ) -> str:
        """
        Download a file from the Carbon Arc Power API.
        :param url: The URL of the file to download.
        :param output_dir: The directory to save the downloaded file.
        :param filename: The name of the file to save.
        :param chunk_size: The size of each chunk to download.
        :return: The path to the downloaded file.
        """
        # check if output_dir exists
        if not os.path.exists(output_dir):
            raise FileNotFoundError(f"Output directory {output_dir} does not exist.")

        response = self.request_manager.get_stream(url)
        # Extract filename from response headers
        if not filename:
            filename = (
                response.headers["Content-Disposition"].split("filename=")[1].strip('"')
            )
        # Update the output path to include the filename
        output = os.path.join(output_dir, filename)
        # Download the file in chunks
        with open(output, "wb") as file:
            for chunk in response.iter_content(chunk_size=chunk_size):
                file.write(chunk)
        return output

    def get_graph_data_idetifiers(self) -> dict:
        """
        Get the list of graph data identifiers from the Carbon Arc Power API.
        :return: A dictionary containing the list of graph data identifiers.
        """
        url = self._routes._build_graph_data_identifiers_url()
        return self._get(url)
