from typing import Literal, Optional, List


class Routes:
    def __init__(self):
        self.host = "https://platform.carbonarc.co"

    def _build_base_url(
        self,
        product: Literal["entity_explorer", "data", "ontology", "public", "opensource"],
    ) -> str:
        return self.host + "/api/v2/" + product

    # ENTITY EXPLORER
        
    # insight data identifiers
    def _build_insight_data_identifiers_url(self, page: int = 1, page_size: int = 100) -> str:
        return (
            self._build_base_url("entity_explorer")
            + f"/data/data-identifiers?page={page}&size={page_size}"
        )

        
    # all data identifiers
    def _build_alldata_data_identifiers_url(self):
        return self._build_base_url("entity_explorer") + "/alldata/data-identifiers"

    # all data identifiers metadata
    def _build_alldata_metadata_url(self, data_identifier: str) -> str:
        return (
            self._build_base_url("entity_explorer")
            + f"/alldata/{data_identifier}/metadata"
        )
    
    # graph data identifiers
    def _build_graphdata_data_identifiers_url(self) -> str:
        return (
            self._build_base_url("entity_explorer") + "/graphdata/data-identifiers"
        )
        
    
    # DATA     
    
    # insights   
    def _build_insight_filters_url(self, data_identifier: str) -> str:
        return self._build_base_url("data") + f"/insights/{data_identifier}/filters"

    def _build_insight_filter_values_url(
        self, data_identifier: str, filter_key: str
    ) -> str:
        return (
            self._build_base_url("data")
            + f"/insights/{data_identifier}/filters/{filter_key}"
        )

    def _build_insight_metadata_url(self, data_identifier: str) -> str:
        return self._build_base_url("data") + f"/insights/{data_identifier}/metadata"

    def _build_insight_data_url(
        self,
        data_identifier: str,
        page: int = 1,
        page_size: int = 100,
        data_type: Literal["dataframe", "timeseries"] = "dataframe",
        aggregation: Literal["sum", "mean", "avg"] = "sum",
    ) -> str:
        return (
            self._build_base_url("data")
            + f"/insights/{data_identifier}?page={page}&size={page_size}&type={data_type}&aggregation={aggregation}"
        )

    def _build_insight_confidence_url(self, data_identifier: str) -> str:
        return self._build_base_url("data") + f"/insights/{data_identifier}/confidence"



    # all data
    def _build_alldata_sample_url(self, data_identifier: str) -> str:
        return self._build_base_url("data") + f"/alldata/{data_identifier}/sample"

    def _build_alldata_manifest_url(self, data_identifier: str) -> str:
        return self._build_base_url("data") + f"/alldata/{data_identifier}/manifest"


    # ONTOLOGY

    def _build_ontology_representations_url(self, entity:Optional[List[str]]=None, domain:Optional[List[str]]=None) -> str:
        _url = self._build_base_url("ontology") + "/entity-representations"
        if entity or domain:
            _url += "?"
        if entity:
            for e in entity:
                _url += "&entity=" + e
        if domain:
            for d in domain:
                _url += "&entity_domain=" + d
        _url = _url.replace("?&", "?")
        return _url
    
    def _build_ontology_entities_url(
        self,
        entity: Optional[List[str]] = None,
        domain: Optional[List[str]] = None,
        representation: Optional[List[str]] = None,
        search: Optional[str] = None,
        min_score: float = 0.6,
        page: int = 1,
        page_size: int = 100,
        order_by:str = "label",
        order:Literal["asc", "desc"] = "asc",
        limit:int=500) -> str:
        _url = self._build_base_url("ontology") + f"/entities?page={page}&size={page_size}&limit={limit}&order_by={order_by}&order={order}"
        if entity:
            for e in entity:
                _url += "&entity=" + e
        if domain:
            for d in domain:
                _url += "&entity_domain=" + d
        if representation:
            for r in representation:
                _url += "&entity_representation=" + r
        if search:
            _url += "&search=" + search + "&min_score=" + str(min_score)
        
        return _url

    # DATA SOURCES

    def _build_datasources_url(self) -> str:
        return self._build_base_url("public") + "/datasources"