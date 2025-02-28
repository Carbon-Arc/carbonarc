import logging
from requests.auth import AuthBase

from carbonarc.exceptions import AuthenticationException
from carbonarc.manager import HttpRequestManager
from carbonarc.routes import Routes


class CarbonArc:
    def __init__(
        self, auth_token:AuthBase
    ):
        self._logger = logging.getLogger(__name__)
        self.auth_token = auth_token
        self._routes = Routes()
        
    def authenticate(self):
        """
        Method to authenticate the request manager. An existing domino client object can
        use this with a new token if the existing credentials expire.
        """
        self.request_manager = HttpRequestManager(
            auth_token=self.auth_token
        )
        
    def _get(self, url:str, **kwargs):
        return self.request_manager.get(url, **kwargs).json()
    
    def _post(self, url:str, **kwargs):
        return self.request_manager.post(url, **kwargs).json()
    
    def entity_explorer_data_identifiers(self):
        url = self._routes._build_data_identifiers_url()
        try:
            return self._get(url)
        except AuthenticationException:
            self._logger.info(
                "Authentication failed. Attempting to re-authenticate and retrying the request."
            )