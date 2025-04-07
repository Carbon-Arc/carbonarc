import os
import logging
from typing import Literal, Optional, List
import pandas as pd

from carbonarc.auth import TokenAuth
from carbonarc.manager import HttpRequestManager
from carbonarc.routes import Routes
import carbonarc.utils as Utils


class APIClient:
    """
    A client for interacting with the Carbon Arc API.
    """

    def __init__(self, token: str):
        """
        Initialize theAPIClient with an authentication token and user agent.
        :param auth_token: The authentication token to be used for requests.
        :param user_agent: The user agent string to be used for requests.
        """
        self._logger = logging.getLogger(__name__)
        self.auth_token = TokenAuth(token)
        self._routes = Routes()
        self.request_manager = HttpRequestManager(auth_token=self.auth_token)

    def _get(self, url: str, **kwargs):
        return self.request_manager.get(url, **kwargs).json()

    def _post(self, url: str, **kwargs) -> dict:
        return self.request_manager.post(url, **kwargs).json()

    # INSIGHTS
    def get_insight_data_identifiers(self, page: int = 1, page_size: int = 100) -> dict:
        """
        Get the list of data identifiers from the Carbon Arc API.
        :param page: The page number to retrieve.
        :param page_size: The number of items per page.
        :return: A dictionary containing the list of data identifiers.
        """
        # Build the URL for the data identifiers endpoint
        url = self._routes._build_insight_data_identifiers_url(page=page, page_size=page_size)
        return self._get(url)

    def get_insight_metadata(self, data_identifier: str) -> dict:
        """
        Get the metadata for a specific data identifier from the Carbon Arc API.
        :param data_identifier: The identifier of the data to retrieve metadata for.
        :return: A dictionary containing the metadata for the specified data identifier.
        """
        url = self._routes._build_insight_metadata_url(
            data_identifier=data_identifier
        )
        return self._get(url)

    def get_insight_filters(self, data_identifier: str) -> dict:
        """
        Get the filters for a specific data identifier from the Carbon Arc API.
        :param data_identifier: The identifier of the data to retrieve filters for.
        :return: A dictionary containing the filters for the specified data identifier.
        """
        url = self._routes._build_insight_filters_url(
            data_identifier=data_identifier
        )
        return self._get(url)

    def get_insight_confidence(self, data_identifier: str) -> dict:
        """
        Get the confidence for a specific data identifier to the Carbon Arc API.
        :param data_identifier: The identifier of the data to retrieve filters for.
        :return: A dictionary containing the confidence for the specified data identifier.
        """
        url = self._routes._build_insight_confidence_url(
            data_identifier=data_identifier
        )
        return self._post(url)
    
    def get_insight_filter_values(self, data_identifier: str, filter_key: str) -> dict:
        """
        Get the filter values for a specific data identifier from the Carbon Arc API.
        :param data_identifier: The identifier of the data to retrieve filter values for.
        :param filter_key: The key of the filter to retrieve values for.
        :return: A dictionary containing the filter values for the specified data identifier.
        """
        url = self._routes._build_insight_filter_values_url(
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
        url = self._routes._build_insight_data_url(
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

    # ALL DATA
    def get_alldata_data_identifiers(self) -> dict:
        """
        Get the list of data identifiers from the Carbon Arc API.
        :return: A dictionary containing the list of data identifiers.
        """
        url = self._routes._build_alldata_data_identifiers_url()
        return self._get(url)

    def get_alldata_metadata(self, data_identifier: str) -> dict:
        """
        Get the metadata for a specific data identifier from the Carbon Arc API.
        :param data_identifier: The identifier of the data to retrieve metadata for.
        :return: A dictionary containing the metadata for the specified data identifier.
        """
        url = self._routes._build_alldata_metadata_url(
            data_identifier=data_identifier
        )
        return self._get(url)

    def get_alldata_sample(self, data_identifier: str) -> dict:
        """
        Get the sample data for a specific data identifier from the Carbon Arc API.
        :param data_identifier: The identifier of the data to retrieve sample data for.
        :return: A dictionary containing the sample data for the specified data identifier.
        """
        url = self._routes._build_alldata_sample_url(
            data_identifier=data_identifier
        )
        return self._get(url)

    def get_alldata_manifest(self, data_identifier: str) -> dict:
        """
        Get the manifest for a specific data identifier from the Carbon Arc API.
        :param data_identifier: The identifier of the data to retrieve manifest for.
        :return: A dictionary containing the manifest for the specified data identifier.
        """
        url = self._routes._build_alldata_manifest_url(
            data_identifier=data_identifier
        )
        return self._get(url)

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

    # GRAPH DATA
    def get_graph_data_identifiers(self) -> dict:
        """
        Get the list of graph data identifiers from the Carbon Arc API.
        :return: A dictionary containing the list of graph data identifiers.
        """
        url = self._routes._build_graphdata_data_identifiers_url()
        return self._get(url)

    # ONTOLOGY    
    def get_ontology_representations(self, entity:Optional[List[str]]=None, domain:Optional[List[str]]=None) -> dict:
        """
        Get the entity representation from the Carbon Arc API.
        :param entity: The entity to retrieve the representation for.
        :param domain: The domain to retrieve the representation for.
        :return: A dictionary containing the entity representation.
        """
        url = self._routes._build_ontology_representations_url(entity=entity, domain=domain)
        return self._get(url)
    
    def get_ontology_entities(
        self,
        entity: Optional[List[str]] = None,
        domain: Optional[List[str]] = None,
        representation: Optional[List[str]] = None,
        search: Optional[str] = None,
        min_score: float = 0.6,
        page: int = 1,
        page_size: int = 100,
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
        url = self._routes._build_ontology_entities_url(
            entity=entity,
            domain=domain,
            representation=representation,
            search=search,
            min_score=min_score,
            page=page,
            page_size=page_size,
        )
        return self._get(url)

    def iter_ontology_entities(
        self,
        page_size: int = 100,
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
            response = self.get_ontology_entities(
                page=page,
                page_size=page_size,
            )
            if not response:
                break  # Exit if the response is None or invalid
            total_pages = response.get("entities", 0)
            if page > total_pages:
                break
            yield response
            page += 1
    
    def get_ontology_core_entities(self) -> dict:
        # brand, company, people, location
        raise NotImplementedError("get_ontology_core_entities is not implemented yet.")
    
    def get_ontology_domains(self, entity:Optional[str]=None) -> dict:
        # domains
        # optional filter by core entity
        raise NotImplementedError("get_ontology_domains is not implemented yet.")
    
    def get_ontology_framework(self, entity:Optional[List[str]]=None, domain:Optional[List[str]]=None, representation:Optional[List[str]]=None) -> dict:
        # entity, domain, representation
        # optional filter by core entity, domain, representation
        raise NotImplementedError("get_ontology_framework is not implemented yet.")
    
    def get_ontology_hierarchies(self):
        # hierarchies
        raise NotImplementedError("get_ontology_hierarchies is not implemented yet.")
    
    def get_ontology_relationships(self):
        raise NotImplementedError("get_ontology_relationships is not implemented yet.")
    
    def get_entity_metadata(self):
        raise NotImplementedError("get_entity_metadata is not implemented yet.")
    
    
    # DATA SOURCES
    def get_datasources(self) -> dict:
        """
        Get the data sources from the Carbon Arc API.
        :return: A dictionary containing the data sources.
        """
        url = self._routes._build_datasources_url()
        return self._get(url)

    # BUILDER
    def get_subjects(
        self, 
        source:Optional[List[str]]=None,
        entity:Optional[List[str]]=None,
        domain:Optional[List[str]]=None,
        representation:Optional[List[str]]=None,
        ) -> dict:
        raise NotImplementedError("get_subjects is not implemented yet.")

    def get_topics(
        self,
        source:Optional[List[str]]=None,
        subject:Optional[List[str]]=None,
        entity:Optional[List[str]]=None,
        domain:Optional[List[str]]=None,
        representation:Optional[List[str]]=None,
        ) -> dict:
        raise NotImplementedError("get_topics is not implemented yet.")
    
    # HUB
    def get_subscriptions(self) -> dict:
        raise NotImplementedError("get_subscriptions is not implemented yet.")
    
    # BILLING
    def get_wallet_balance(self) -> dict:
        raise NotImplementedError("get_balance is not implemented yet.")
    
    def get_payload_price(self):
        raise NotImplementedError("get_payload_price is not implemented yet.")
    
    # WORKSPACE
    def get_workbooks(self) -> dict:
        raise NotImplementedError("get_subscriptions is not implemented yet.")
    
    def get_insights(self, workbook_id:str) -> dict:
        raise NotImplementedError("get_insights is not implemented yet.")