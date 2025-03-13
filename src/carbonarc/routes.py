from typing import Literal


class Routes:
    def __init__(self):
        self.host = "https://platform.carbonarc.co"

    def _build_product_url(
        self,
        product: Literal["entity_explorer", "data", "ontology", "public", "opensource"],
    ) -> str:
        return self.host + "/api/v2/" + product

    def _build_data_identifiers_url(self, page: int = 1, page_size: int = 100) -> str:
        return (
            self._build_product_url("entity_explorer")
            + f"/data/data-identifiers?page={page}&size={page_size}"
        )

    def _build_data_identifier_metadata_url(self, data_identifier: str) -> str:
        return self._build_product_url("data") + f"/insights/{data_identifier}/metadata"

    def _build_all_data_identifiers_url(self):
        return self._build_product_url("entity_explorer") + "/alldata/data-identifiers"

    def _build_all_data_identifier_metadata_url(self, data_identifier: str) -> str:
        return (
            self._build_product_url("entity_explorer")
            + f"/alldata/{data_identifier}/metadata"
        )

    def _build_graph_data_identifiers_url(self) -> str:
        return (
            self._build_product_url("entity_explorer") + "/graphdata/data-identifiers"
        )

    def _build_data_identifiers_filters_url(self, data_identifier: str) -> str:
        return self._build_product_url("data") + f"/insights/{data_identifier}/filters"

    def _build_data_identifiers_filter_values_url(
        self, data_identifier: str, filter_key: str
    ) -> str:
        return (
            self._build_product_url("data")
            + f"/insights/{data_identifier}/filters/{filter_key}"
        )

    def _build_data_identifiers_data_url(
        self,
        data_identifier: str,
        page: int = 1,
        page_size: int = 100,
        data_type: Literal["dataframe", "timeseries"] = "dataframe",
        aggregation: Literal["sum", "mean", "avg"] = "sum",
    ) -> str:
        return (
            self._build_product_url("data")
            + f"/insights/{data_identifier}?page={page}&size={page_size}&type={data_type}&aggregation={aggregation}"
        )

    def _build_builk_identifier_metadata_url(self, data_identifier: str) -> str:
        return self._build_product_url("entity_explorer") + f"/alldata/{data_identifier}/metadata"

    def _build_builk_identifier_sample_url(self, data_identifier: str) -> str:
        return self._build_product_url("data") + f"/alldata/{data_identifier}/sample"

    def _build_builk_identifier_manifest_url(self, data_identifier: str) -> str:
        return self._build_product_url("data") + f"/alldata/{data_identifier}/manifest"
