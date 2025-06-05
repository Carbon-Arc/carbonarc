from typing import Optional, List, Literal

from carbonarc.base import BaseAPIClient

class OntologyAPIClient(BaseAPIClient):
    """
    A client for interacting with the Carbon Arc Ontology API.
    """

    def __init__(
        self, 
        token: str,
        host: str = "https://platform.carbonarc.co",
        version: str = "v2"
        ):
        """
        Initialize OntologyAPIClient with an authentication token and user agent.
        :param auth_token: The authentication token to be used for requests.
        :param host: The base URL of the Carbon Arc API.
        :param version: The API version to use.
        """
        super().__init__(token=token, host=host, version=version)
        
        self.base_ontology_url = self._build_base_url("ontology")
        
    # TODO: implement
    def get_core(self) -> dict:
        """
        Get the core ontology from the Carbon Arc API.
        :return: A dictionary containing the core ontology.
        """
        url = self.base_ontology_url + "/core"
        return self._get(url)
    
    # TODO: implement
    def get_domains(self, core: Optional[str] = None) -> dict:
        """
        Get the domains from the Carbon Arc API.
        :param core: The core entity to filter domains by (optional).
        :return: A dictionary containing the domains.
        """
        url = self.base_ontology_url + "/domains"
        if core:
            url += f"?core={core}"
        return self._get(url)
    
    # TODO: implement
    def get_representation(self, core: Optional[str] = None, domain: Optional[str] = None):
        """
        Get the representation of a specific core entity in a specific domain from the Carbon Arc API.
        :param core: The core entity to retrieve the representation for.
        :param domain: The domain to retrieve the representation for.
        :return: A dictionary containing the representation of the core entity in the specified domain.
        """
        url = self.base_ontology_url + f"/representation/{core}/{domain}"
        return self._get(url)
    
    # TODO: implement
    def get_entities(self, core: Optional[str] = None, domain: Optional[str] = None, representation: Optional[str] = None) -> dict:
        """ Get the entities from the Carbon Arc API.
        :param core: The core entity to filter entities by (optional).
        :param domain: The domain to filter entities by (optional).
        :param representation: The representation to filter entities by (optional).
        :return: A dictionary containing the entities.
        """
        url = self.base_ontology_url + "/entities"
        if core:
            url += f"?core={core}"
        if domain:
            url += f"&domain={domain}"
        if representation:
            url += f"&representation={representation}"
        return self._get(url)
    
    # TODO: implement
    def get_entity_information(self, entity_id: int) -> dict:
        """
        Get the information for a specific entity from the Carbon Arc API.
        :param entity: The entity to retrieve information for.
        :return: A dictionary containing the information for the specified entity.
        """
        url = self.base_ontology_url + f"/entity/{entity_id}"
        return self._get(url)






























    def _build_ontology_representations_url(self, entity:Optional[List[str]]=None, domain:Optional[List[str]]=None) -> str:
        _url = self.base_ontology_url + "/entity-representations"
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
        _url = self.base_ontology_url + f"/entities?page={page}&size={page_size}&limit={limit}&order_by={order_by}&order={order}"
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
    
    def get_ontology_representations(self, entity:Optional[List[str]]=None, domain:Optional[List[str]]=None) -> dict:
        """
        Get the entity representation from the Carbon Arc API.
        :param entity: The entity to retrieve the representation for.
        :param domain: The domain to retrieve the representation for.
        :return: A dictionary containing the entity representation.
        """
        url = self._build_ontology_representations_url(entity=entity, domain=domain)
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
        url = self._build_ontology_entities_url(
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
    