"""Shared helpers for the SDK's unit tests.

Unit tests never call a real API: ``FakeSession`` stands in for the
``requests.Session`` a client sends through, answering from recorded responses
in ``tests/fixtures``. The live suite (``tests/live``) calls a real API.
"""

import json
from pathlib import Path
from typing import Any, List, Optional, Tuple

import pytest
import requests

FIXTURES = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


def make_response(
    status: int = 200, body: Any = None, headers: Optional[dict] = None
) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = b"" if body is None else json.dumps(body).encode()
    response.headers.update(headers or {})
    response.headers.setdefault("Content-Type", "application/json")
    response.url = "https://api.test/v2/train"
    return response


class FakeSession:
    """Answers each request with the next queued response, and records what
    was sent so a test can check the method, URL, params and body."""

    def __init__(self) -> None:
        self.responses: List[requests.Response] = []
        self.calls: List[Tuple[str, str, dict]] = []

    def queue(self, status: int = 200, body: Any = None, headers=None) -> None:
        self.responses.append(make_response(status, body, headers))

    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


@pytest.fixture
def train_session():
    """A ``CarbonArcClient`` against https://api.test whose ``train`` client
    sends through a ``FakeSession``. Yields ``(client, session)``."""
    from carbonarc import CarbonArcClient

    client = CarbonArcClient(token="test-token", host="https://api.test")
    session = FakeSession()
    client.train.request_manager.request_session.request = session.request
    yield client, session
