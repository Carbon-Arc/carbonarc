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
    
    def get_webcontent_information_by_name(self, webcontent_name: str, page: Optional[int] = None, size: Optional[int] = None) -> dict:
        """
        Retrieve a webcontent information by name and optionally with pagination parameters.
        This includes the webcontent name, webcontent id, description, documentation, and industry information.
        """
        url = f"{self.base_webcontent_url}/{webcontent_name}"
        if page or size:
            page = page if page else 1
            size = size if size else 25
            url += f"?page={page}&size={size}"
        return self._get(url)
    
    def get_webcontent_data(self, webcontent_id: int, webcontent_date: Optional[Tuple[Literal["<", "<=", ">", ">=", "=="], Union[datetime, str]]] = None,) -> dict:
        """
        Retrieve a webcontent data by id and date.

        This returns a dictionary containing the feed metadata and the data for the given specifications
        """
        url = f"{self.base_webcontent_url}/{webcontent_id}/data"
        if webcontent_date:
            params = {
                "webcontent_date_operator": webcontent_date[0],
                "webcontent_date": webcontent_date[1]
            }
            return self._get(url, params=params)
        else:
            return self._get(url)

    def download_webcontent_file(self, webcontent_id: int, 
                                 webcontent_date: Optional[Tuple[Literal["<", "<=", ">", ">=", "=="], Union[datetime, str]]] = None, 
                                 directory: str = "./",
                                 filename: Optional[str] = None) -> str:
        """
        Download a webcontent file by webcontent id and date.
        """

        data = self.get_webcontent_data(webcontent_id, webcontent_date)
        file_name = filename if filename else f"{data['webcontent_name']}_{webcontent_date[0]}_{webcontent_date[1]}.json"
        # Get full path of directory and ensure it exists
        output_dir = os.path.abspath(directory)
        os.makedirs(output_dir, exist_ok=True)
        print(f"Uploading file {file_name} to {output_dir}")
        with open(os.path.join(output_dir, file_name), 'w') as f:
            json.dump(data, f, indent=2)

        return data

