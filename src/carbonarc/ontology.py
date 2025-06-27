from typing import Optional, List, Literal, Dict, Any, Union

from carbonarc.base.client import BaseAPIClient

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
        
        Args:
            token: The authentication token to be used for requests.
            host: The base URL of the Carbon Arc API.
            version: The API version to use.
        """
        super().__init__(token=token, host=host, version=version)
        
        self.base_ontology_url = self._build_base_url("ontology")
    
    def get_entity_map(self) -> dict:
        """
        Retrieve the entity map.
        """
        url = f"{self.base_ontology_url}/entity-map"
        return self._get(url)
    
    def get_insight_map(self) -> dict:
        """
        Retrieve the insight map.
        """
        url = f"{self.base_ontology_url}/insight-map"
        return self._get(url)

    def get_entities(
        self,
        representation: Optional[List[str]] = None,
        domain: Optional[Union[str, List[str]]] = None,
        entity: Optional[List[Literal["brand", "company", "people", "location"]]] = None,
        subject_ids: Optional[List[int]] = None,
        topic_ids: Optional[List[int]] = None,
        insight_types: Optional[List[Literal["metric", "event", "kpi", "marketshare", "cohort"]]] = None,
        insight_id: Optional[int] = None,
        version: Optional[str] = "latest",
        page: int = 1,
        size: int = 100,
        sort_by: str = "label",
        order: str = "asc"
    ) -> Dict[str, Any]:
        """
        Retrieve entities with filtering and pagination.

        Args:
            entity_representation: List of entity representations to filter by.
            entity_domain: Entity domain(s) to filter by.
            entity: List of entity types to filter by.
            subject_ids: List of subject IDs to filter by.
            topic_ids: List of topic IDs to filter by.
            insight_types: List of insight types to filter by.
            insight_id: Insight ID to filter by.
            page: Page number (default 1).
            size: Number of results per page (default 100).
            sort_by: Field to sort by.
            order: Sort direction ("asc" or "desc").

        Returns:
            Dictionary containing paginated entities.
        """
        params = {
            "page": page,
            "size": size,
            "sort_by": sort_by,
            "order": order
        }
        if subject_ids:
            params["subject_ids"] = subject_ids
        if topic_ids:
            params["topic_ids"] = topic_ids
        if insight_types:
            params["insight_types"] = insight_types
        if insight_id:
            params["insight_id"] = insight_id
        if representation:
            params["entity_representation"] = representation
        if domain:
            params["entity_domain"] = domain
        if entity:
            params["entity"] = entity
        if version:
            params["version"] = version
        url = f"{self.base_ontology_url}/entities"
        return self._get(url, params=params)

    def get_entity_information(self, entity_id: int, representation: str) -> dict:
        """
        Retrieve information for a specific entity.

        Args:
            entity_id: Entity ID.

        Returns:
            Dictionary with entity information.
        """
        params = {"entity_representation": representation}
        url = f"{self.base_ontology_url}/entities/{entity_id}"
        return self._get(url, params=params)

    def get_insights(
        self,
        subject_ids: Optional[List[int]] = None,
        topic_ids: Optional[List[int]] = None,
        insight_types: Optional[List[Literal["metric", "event", "kpi", "marketshare", "cohort"]]] = None,
        entity_id: Optional[int] = None,
        entity_representation: Optional[str] = None,
        entity_domain: Optional[Union[str, List[str]]] = None,
        entity: Optional[Literal["brand", "company", "people", "location"]] = None,
        page: int = 1,
        size: int = 100,
        sort_by: str = "insight_label",
        order: str = "asc"
    ) -> Dict[str, Any]:
        """
        Retrieve insights with filtering and pagination.

        Args:
            subject_ids: List of subject IDs to filter by.
            topic_ids: List of topic IDs to filter by.
            insight_types: List of insight types to filter by.
            entity_id: Entity ID to filter by.
            entity_representation: Entity representation to filter by.
            entity_domain: Entity domain(s) to filter by.
            entity: Entity type to filter by.
            page: Page number (default 1).
            size: Number of results per page (default 100).
            sort_by: Field to sort by.
            order: Sort direction ("asc" or "desc").

        Returns:
            Dictionary containing paginated insights.
        """
        params = {
            "page": page,
            "size": size,
            "sort_by": sort_by,
            "order": order
        }
        if subject_ids:
            params["subject_ids"] = subject_ids
        if topic_ids:
            params["topic_ids"] = topic_ids
        if insight_types:
            params["insight_types"] = insight_types
        if entity_id:
            params["entity_id"] = entity_id
        if entity_representation:
            params["entity_representation"] = entity_representation
        if entity_domain:
            params["entity_domain"] = entity_domain
        if entity:
            params["entity"] = entity
        url = f"{self.base_ontology_url}/insights"
        return self._get(url, params=params)

    def get_insight_information(self, insight_id: int) -> dict:
        """
        Retrieve information for a specific insight.

        Args:
            insight_id: Insight ID.

        Returns:
            Dictionary with insight information.
        """
        url = f"{self.base_ontology_url}/insights/{insight_id}"
        return self._get(url)

    def get_insights_for_entity(self, entity_id: int) -> dict:
        """
        Retrieve insights for a specific entity.
        """
        url = f"{self.base_ontology_url}/entity/{entity_id}/insights"
        return self._get(url)
    
    def get_entities_for_insight(self, insight_id: int) -> dict:
        """
        Retrieve entities for a specific insight.
        """
        url = f"{self.base_ontology_url}/insight/{insight_id}/entities"
        return self._get(url)
    
    def get_subjects(self) -> dict:
        """
        Retrieve all subjects.
        """
        url = f"{self.base_ontology_url}/subjects"
        return self._get(url)
    
    def get_topics(self) -> dict:
        """
        Retrieve all topics.
        """
        url = f"{self.base_ontology_url}/topics"
        return self._get(url)
    
    def get_insights_for_subject(self, subject_id: int) -> dict:
        """
        Retrieve insights for a specific subject.
        """
        url = f"{self.base_ontology_url}/subject/{subject_id}/insights"
        return self._get(url)
    
    def get_insights_for_topic(self, topic_id: int) -> dict:
        """
        Retrieve insights for a specific topic.
        """
        url = f"{self.base_ontology_url}/topic/{topic_id}/insights"
        return self._get(url)
        
    def get_ontology_version(self) -> dict:
        """
        Retrieve the current ontology version.
        """
        url = f"{self.base_ontology_url}/ontology-versions"
        return self._get(url)
    
    def get_ontology_tree(self) -> dict:
        """
        Retrieve the ontology tree.
        """
        url = f"{self.base_ontology_url}/ontology-tree"
        return self._get(url)