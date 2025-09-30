from typing import Optional, Tuple, Union
from datetime import datetime
from typing import Literal
import logging
import json
import os

from carbonarc.utils.client import BaseAPIClient
PAGE = 1
SIZE = 25
logger = logging.getLogger(__name__)


class HubAPIClient(BaseAPIClient):
    """
    A client for interacting with the Carbon Arc Hub API.

    The HubAPIClient provides access to Carbon Arc's Hub API, which includes
    functionality for retrieving and managing web content feeds, subscriptions,
    and data downloads. This client handles authentication, request formatting,
    and response parsing.

    Key Features:
        - Web content feed management and retrieval
        - Subscription-based access control
        - Data download and file management
        - Date-based filtering and pagination

    Example:
        >>> client = HubAPIClient(token="your_token")
        >>> feeds = client.get_webcontent_feeds()
        >>> subscribed = client.get_subscribed_feeds()
        >>> data = client.get_webcontent_data(webcontent_id=123)

    Note:
        All methods require a valid authentication token. The client will
        automatically handle token inclusion in requests.
    """

    def __init__(
        self, 
        token: str,
        host: str = "https://api.carbonarc.co",
        version: str = "v2"
        ):
        """
        Initialize HubAPIClient with authentication and configuration settings.
        
        This constructor sets up the client with the necessary authentication token
        and API configuration. It inherits base functionality from BaseAPIClient
        and adds Hub-specific endpoint configurations.

        Args:
            token (str): Authentication token for API access. This token should be
                obtained from your Carbon Arc dashboard.
            host (str, optional): Base URL for the Carbon Arc API. Defaults to
                "https://api.carbonarc.co". Only change this if you're using a
                different API environment.
            version (str, optional): API version to use. Defaults to "v2". This
                should match the API version your token is authorized for.

        Raises:
            ValueError: If token is empty or invalid.
            ConnectionError: If the API host is unreachable.

        Note:
            The client will automatically construct the necessary base URLs for
            different API endpoints (hub, webcontent) using the provided host
            and version.
        """
        super().__init__(token=token, host=host, version=version)
        
        self.base_hub_url = self._build_base_url("hub")
        self.base_webcontent_url = self._build_base_url("webcontent")
    
    def get_webcontent_feeds(self, page: Optional[int] = None, size: Optional[int] = None) -> dict:
        """
        Retrieve all available web content feeds.

        This method returns a list of all web content feeds that are available
        in the system, regardless of subscription status. Each feed contains
        metadata about the content source, update frequency, and access requirements.

        Returns:
            dict: A dictionary containing the list of web content feeds 

        Raises:
            HTTPError: If the API request fails
            AuthenticationError: If the authentication token is invalid

        Example:
            >>> client = HubAPIClient(token="your_token")
            >>> feeds = client.get_webcontent_feeds()
            >>> print(f"Found {feeds['total']} feeds")
            >>> for feed in feeds['feeds']:
            ...     print(f"{feed['name']}: {feed['description']}")
        """
        if page or size:
            page = page if page else PAGE
            size = size if size else SIZE
            url = f"{self.base_webcontent_url}?page={page}&size={size}"
        else:
            url = f"{self.base_webcontent_url}/"

        return self._get(url)
    
    def get_subscribed_feeds(self) -> dict:
        """
        Retrieve all web content feeds that the user is subscribed to.

        This method returns a list of web content feeds that the authenticated user
        has active subscriptions for. The response includes detailed metadata about
        each feed and its current status.

        Returns:
            dict: A dictionary containing the list of subscribed feeds
        Raises:
            HTTPError: If the API request fails
            AuthenticationError: If the authentication token is invalid

        Example:
            >>> client = HubAPIClient(token="your_token")
            >>> subscribed = client.get_subscribed_feeds()
            >>> print(f"You have {subscribed['total']} active subscriptions")
            >>> for feed in subscribed['feeds']:
            ...     print(f"{feed['name']} - Next update: {feed['next_update']}")

        Note:
            This endpoint only returns feeds with active subscriptions. For all
            available feeds, use get_webcontent_feeds() instead.
        """
        url = f"{self.base_webcontent_url}/subscribed"
        return self._get(url)
    
    def get_webcontent_information_by_name(self, webcontent_name: str, page: Optional[int] = None, size: Optional[int] = None) -> dict:
        """
        Retrieve detailed information about a web content feed by its name.

        This method returns comprehensive metadata about a specific web content feed,
        including its configuration, documentation, and industry categorization.
        Results can be paginated using the optional page and size parameters.

        Args:
            webcontent_name (str): The unique name identifier of the web content feed.
                This is typically a URL-friendly string (e.g., "tesla.com").
            page (Optional[int], optional): Page number for paginated results.
                Defaults to 1 if size is provided without page.
            size (Optional[int], optional): Number of items per page.
                Defaults to 25 if page is provided without size.

        Returns:
            dict: A dictionary containing detailed feed information

        Raises:
            HTTPError: If the API request fails
            AuthenticationError: If the authentication token is invalid
            ValueError: If page/size parameters are invalid

        Example:
            >>> client = HubAPIClient(token="your_token")
            >>> info = client.get_webcontent_information_by_name("tesla.com")
            >>> print(f"Feed: {info['name']}")
            >>> print(f"Updates: {info['update_frequency']}")
            >>> print(f"Records: {info['metadata']['total_records']}")

        Note:
            If both page and size are omitted, the method returns all available
            information without pagination.
        """
        url = f"{self.base_webcontent_url}/{webcontent_name}"
        if page or size:
            page = page if page else 1
            size = size if size else 25
            url += f"?page={page}&size={size}"
        return self._get(url)
    
    def get_webcontent_data(self, webcontent_id: int, webcontent_date: Optional[Tuple[Literal["<", "<=", ">", ">=", "=="], Union[datetime, str]]] = None, page: Optional[int] = None, size: Optional[int] = None, fetch_all=None) -> dict:
        """
        Retrieve web content data for a specific feed by ID and optional date filter.

        This method returns the actual content data from a web content feed, along
        with metadata about the feed and the returned data. The data can be filtered
        by date using the optional webcontent_date parameter.

        Args:
            webcontent_id (int): The unique identifier of the web content feed.
            webcontent_date (Optional[Tuple[Literal["<", "<=", ">", ">=", "=="], Union[datetime, str]]], optional):
                A tuple containing a comparison operator and a date value to filter the data.
                The date can be provided as either a datetime object or a string in
                ISO format (YYYY-MM-DD). For example: (">=", "2025-01-01") or
                ("<=", datetime(2025, 12, 31)).

        Returns:
            dict: A dictionary containing the feed data and metadata

        Raises:
            HTTPError: If the API request fails
            AuthenticationError: If the authentication token is invalid
            ValueError: If webcontent_date format is invalid
            TypeError: If webcontent_date operator is invalid

        Example:
            >>> from datetime import datetime, timedelta
            >>> client = HubAPIClient(token="your_token")
            >>> # Get data from the last 7 days
            >>> cutoff = datetime.now() - timedelta(days=7)
            >>> data = client.get_webcontent_data(123, (">=", cutoff))
            >>> print(f"Found {data['query_metadata']['total_records']} records")
            >>> for record in data['data']:
            ...     print(f"{record['timestamp']}: {record['content']}")

        Note:
            If webcontent_date is omitted, the method returns the most recent
            data available for the feed, subject to any subscription-based
            restrictions.
        """
        url = f"{self.base_webcontent_url}/{webcontent_id}/data"
        params = {}
        if webcontent_date:
            params['webcontent_date_operator'] = webcontent_date[0]
            params["webcontent_date"] = webcontent_date[1]
        if fetch_all:
            if page or size:
                logger.warning("Page and size are ignored when fetch_all is True")
            params['fetch_all'] = True
        else:
            params['fetch_all'] = False
            if page or size:
                params['page'] = page if page else PAGE
                params['size'] = size if size else SIZE

        return self._get(url, params=params)


    def download_webcontent_file(self, webcontent_id: int, 
                                 webcontent_date: Optional[Tuple[Literal["<", "<=", ">", ">=", "=="], Union[datetime, str]]] = None, 
                                 directory: str = "./",
                                 filename: Optional[str] = None) -> str:
        """
        Download web content data to a local file.

        This method retrieves web content data and saves it to a local file in JSON
        format. The data can be filtered by date, and the output file location and
        name can be customized.

        Args:
            webcontent_id (int): The unique identifier of the web content feed.
            webcontent_date (Optional[Tuple[Literal["<", "<=", ">", ">=", "=="], Union[datetime, str]]], optional):
                A tuple containing a comparison operator and a date value to filter the data.
                The date can be provided as either a datetime object or a string in
                ISO format (YYYY-MM-DD). For example: (">=", "2025-01-01") or
                ("<=", datetime(2025, 12, 31)).
            directory (str, optional): The directory where the file should be saved.
                Defaults to the current directory ("./"). The directory will be
                created if it doesn't exist.
            filename (Optional[str], optional): Custom name for the output file.
                If not provided, the filename will be generated using the feed name
                and date filter (e.g., "tesla_com_gte_2025-01-01.json").

        Returns:
            dict: The downloaded data in dictionary format, with the same structure
            as returned by get_webcontent_data().

        Raises:
            HTTPError: If the API request fails
            AuthenticationError: If the authentication token is invalid
            ValueError: If webcontent_date format is invalid
            TypeError: If webcontent_date operator is invalid
            OSError: If there are file system errors (permissions, disk space, etc.)

        Example:
            >>> from datetime import datetime
            >>> client = HubAPIClient(token="your_token")
            >>> # Download data from January 2025
            >>> data = client.download_webcontent_file(
            ...     webcontent_id=123,
            ...     webcontent_date=(">=", "2025-01-01"),
            ...     directory="downloads/tesla",
            ...     filename="tesla_jan_2025.json"
            ... )
            >>> print(f"Downloaded {data['query_metadata']['total_records']} records")

        Note:
            - The method creates the target directory if it doesn't exist
            - The file is saved with pretty-printing (indentation) for readability
            - The method returns the data dictionary for immediate use in code
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

