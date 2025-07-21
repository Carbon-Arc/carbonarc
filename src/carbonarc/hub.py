from typing import Optional, Tuple, Union
from datetime import datetime
from typing import Literal
import json
import os

from carbonarc.utils.client import BaseAPIClient

class HubAPIClient(BaseAPIClient):
    """
    A client for interacting with the Carbon Arc Hub API.
    """

    def __init__(
        self, 
        token: str,
        host: str = "https://api.carbonarc.co",
        version: str = "v2"
        ):
        """
        Initialize HubAPIClient with an authentication token and user agent.
        
        Args:
            token: The authentication token to be used for requests.
            host: The base URL of the Carbon Arc API.
            version: The API version to use.
        """
        super().__init__(token=token, host=host, version=version)
        
        self.base_hub_url = self._build_base_url("hub")
        self.base_webcontent_url = self._build_base_url("webcontent")
    
    def get_webcontent_feeds(self) -> dict:
        """
        Retrieve all webcontent feeds.
        """
        url = f"{self.base_webcontent_url}"
        return self._get(url)
    
    def get_subscribed_feeds(self) -> dict:
        """
        Retrieve all subscribed webcontent feeds.
        """
        url = f"{self.base_webcontent_url}/subscribed"
        return self._get(url)
    
    
    def get_webcontent_manifest(self, webcontent_id: int, webcontent_date: Optional[Tuple[Literal["<", "<=", ">", "=>", "=="], Union[datetime, str]]] = None,) -> dict:
        """
        Retrieve a webcontent manifest by id and date.
        """
        url = f"{self.base_webcontent_url}/{webcontent_id}/manifest"
        if webcontent_date:
            params = {
                "webcontent_date_operator": webcontent_date[0],
                "webcontent_date": webcontent_date[1]
            }
            return self._get(url, params=params)
        else:
            return self._get(url)
    
    def get_webcontent_dataframe(self, webcontent_name: str) -> dict:
        """
        Retrieve a webcontent dataframe by name.
        """
        webcontent_name = webcontent_name.lower()
        url = f"{self.base_webcontent_url}/{webcontent_name}/dataframe"
        return self._get(url)
    
    def get_webcontent_file(self, file_name: str) -> dict:
        """
        Retrieve a webcontent data by file name.
        """
        url = f"{self.base_webcontent_url}/file/{file_name}"
        return self._get(url)
        
    def download_webcontent_file(self, file_name: str, directory: str = "./") -> str:
        """
        Download a webcontent file by name.
        """
        
        # Get full path of directory and ensure it exists
        output_dir = os.path.abspath(directory)
        os.makedirs(output_dir, exist_ok=True)
        
        webcontent = self.get_webcontent_file(file_name)

        with open(os.path.join(output_dir, file_name.split("/")[-1]), 'w') as f:
            json.dump(webcontent, f)

        return webcontent

