import logging
from http import HTTPStatus

import requests
from bs4 import BeautifulSoup
from requests.auth import AuthBase

from carbonarc import __version__
from carbonarc.utils.exceptions import (
    AuthenticationError,
    ForbiddenError,
    RateLimitError,
)


class HttpRequestManager:
    """
    This class is responsible for
    making Http request calls
    """

    def __init__(
        self, auth_token: AuthBase, user_agent: str = f"Python-APIClient/{__version__}"
    ):
        """
        Initialize the HttpRequestManager with an authentication token and user agent.
        :param auth_token: The authentication token to be used for requests.
        :param user_agent: The user agent string to be used for requests.
        """
        if not isinstance(auth_token, AuthBase):
            raise ValueError("auth_token must be an instance of requests.auth.AuthBase")

        self.auth_token = auth_token
        self._logger = logging.getLogger(__name__)
        self.request_session = requests.Session()
        self.request_session.headers.update(
            {
                "User-Agent": user_agent,
                "Accept": "application/json",
            }
        )

    def post(self, url, data=None, json=None, **kwargs) -> requests.Response:
        return self._raise_for_status(
            self.request_session.post(
                url, auth=self.auth_token, data=data, json=json, **kwargs
            )
        )

    def patch(self, url, data=None, json=None, **kwargs) -> requests.Response:
        return self._raise_for_status(
            self.request_session.patch(
                url, auth=self.auth_token, data=data, json=json, **kwargs
            )
        )

    def get(self, url, **kwargs) -> requests.Response:
        return self._raise_for_status(
            self.request_session.get(url, auth=self.auth_token, **kwargs)
        )

    def put(self, url, data=None, **kwargs) -> requests.Response:
        return self._raise_for_status(
            self.request_session.put(url, auth=self.auth_token, data=data, **kwargs)
        )

    def delete(self, url, **kwargs) -> requests.Response:
        return self._raise_for_status(
            self.request_session.delete(url, auth=self.auth_token, **kwargs)
        )

    def get_stream(self, url, **kwargs) -> requests.Response:
        self.request_session.headers.update({"Accept": "application/octet-stream"})
        return self._raise_for_status(
            self.request_session.get(url, auth=self.auth_token, stream=True, **kwargs)
        )

    def _raise_for_status(self, response: requests.Response) -> requests.Response:
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == HTTPStatus.CONFLICT:
                raise AuthenticationError("Conflict error")
            if e.response.status_code == HTTPStatus.TOO_MANY_REQUESTS:
                # The buy/order routes enforce a per-client ceiling and answer
                # with a ``Retry-After`` header. Without this branch the caller
                # gets a bare HTTPError and has to string-match "429" to tell a
                # throttle apart from a server error.
                detail = None
                try:
                    body = e.response.json()
                    if isinstance(body, dict):
                        detail = body.get("detail")
                        # This route class sends detail as an object, not a string.
                        if isinstance(detail, dict):
                            detail = detail.get("message")
                except ValueError:
                    pass
                # Seconds here, but the header is also allowed to carry an
                # HTTP-date, which is not an integer.
                retry_after = e.response.headers.get("Retry-After", "")
                raise RateLimitError(
                    detail or "Rate limit exceeded",
                    status_code=e.response.status_code,
                    response=e.response,
                    retry_after=int(retry_after) if retry_after.isdigit() else None,
                ) from e
            if e.response.status_code == HTTPStatus.FORBIDDEN:
                detail: str | None = None
                try:
                    body = e.response.json()
                    if isinstance(body, dict):
                        detail = body.get("detail")
                except ValueError:
                    pass
                raise ForbiddenError(
                    detail or "Forbidden",
                    status_code=e.response.status_code,
                    response=e.response,
                ) from e
            if not bool(BeautifulSoup(e.response.text, "html.parser").find()):
                self._logger.error(e.response.text)
            else:
                self._logger.debug(e.response.text)
            raise
        return response
