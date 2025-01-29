import os
import pytest
import requests
from unittest.mock import patch, MagicMock
from prefect_cpln import constants
from prefect_cpln.credentials import CplnCredentials, CplnConfig, CplnClient


@pytest.fixture
def mock_env_token(monkeypatch):
    """Fixture to set a mock CPLN_TOKEN environment variable."""
    monkeypatch.setenv("CPLN_TOKEN", "mock_env_token")


@pytest.fixture
def mock_env_org(monkeypatch):
    """Fixture to set a mock CPLN_ORG environment variable."""
    monkeypatch.setenv("CPLN_ORG", "mock_org")


@pytest.fixture
def mock_env_endpoint(monkeypatch):
    """Fixture to set a mock CPLN_ORG environment variable."""
    monkeypatch.setenv("CPLN_ENDPOINT", "https://api.test.cpln.io")


########################
# Unit Tests: CplnClient
########################


def test_cpln_client_init_with_token():
    """Tests that a provided token is used when initialized."""
    client = CplnClient(token="provided_token")
    assert client.token == "provided_token"


def test_cpln_client_init_no_token_uses_env(mock_env_token):
    """Tests that when no token is provided, environment variable is used."""
    client = CplnClient()
    assert client.token == "mock_env_token"


@patch(
    "prefect_cpln.credentials.requests.request"
)  # Notice the path you patch must match where the code is imported in credentials.py
def test_cpln_client_get(mock_request, mock_env_endpoint):
    # Define constants
    token = "test_token"
    org_name = "example-org"
    path = f"/org/{org_name}"
    endpoint = os.getenv("CPLN_ENDPOINT")
    response = {
        "kind": "org",
        "name": org_name,
    }

    # Define the mock object
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = response
    mock_request.return_value = mock_response

    # Initialize the API client and make the get request
    client = CplnClient(token=token)
    data = client.get(path)

    mock_request.assert_called_once_with(
        "get",
        f"{endpoint}{path}",
        timeout=constants.HTTP_REQUEST_TIMEOUT,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )

    assert data == response


########################
# Unit Tests: CplnConfig
########################


def test_cpln_config_init_no_override(mock_env_token):
    """
    Tests that CplnConfig defaults to the CPLN_TOKEN env var when none is provided.
    """
    config = CplnConfig()
    assert config.token == "mock_env_token"


def test_cpln_config_init_with_token():
    """Tests that a provided token is stored properly in CplnConfig."""
    config = CplnConfig(token="provided_token")
    assert config.token == "provided_token"


def test_cpln_config_get_api_client():
    """Tests that get_api_client returns a valid CplnClient."""
    config = CplnConfig(token="provided_token")
    client = config.get_api_client()
    assert isinstance(client, CplnClient)
    assert client.token == "provided_token"


###########################
# Unit Tests: CplnCredentials
###########################


def test_cpln_credentials_with_config():
    """
    Tests that get_client uses the config's token if config is present.
    """
    config = CplnConfig(token="config_token")
    creds = CplnCredentials(config=config)

    client = creds.get_client()
    assert client.token == "config_token"


def test_cpln_credentials_without_config(mock_env_token):
    """
    Tests that get_client falls back to provided token or env var if config isn't set.
    """
    creds = CplnCredentials()
    client = creds.get_client(token="fallback_token")
    assert client.token == "fallback_token"


#############################
# Integration Test (Optional)
#############################


@pytest.mark.integration
def test_cpln_client_integration_get(mock_env_endpoint):
    # Skip if real token/org not provided
    token = os.getenv("CPLN_TOKEN")
    org = os.getenv("CPLN_ORG")
    if not token or not org or "mock" in org.lower():
        pytest.skip("No valid token/org; skipping integration test.")

    client = CplnClient()
    # This will do a real GET request
    try:
        response = client.get(f"/org/{org}")
        assert isinstance(response, dict)
    except requests.HTTPError as e:
        pytest.fail(f"Integration request failed: {e}")
