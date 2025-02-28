from typing import Literal

class Routes:
    def __init__(self):
        self.host = "https://platform.carbonarc.co"

    def _build_product_url(self, product: Literal["entity_explorer", "data", "ontology", "public", "opensource"]):
        return (
            self.host
            + "/api/v2/"
            + product
        )

    def _build_data_identifiers_url(self):
        return self._build_product_url("entity_explorer") + "/data/data-identifiers"
    
    def _build_all_data_identifiers_url(self):
        return self._build_product_url("entity_explorer") + "/alldata/data-identifiers"

    def _build_all_data_identifier_metadata_url(self, data_identifier:str):
        return self._build_product_url("entity_explorer") + f"/alldata/{data_identifier}/metadata"
    
    def _build_graph_data_identifiers_url(self):
        return self._build_product_url("entity_explorer") + "/graphdata/data-identifiers"