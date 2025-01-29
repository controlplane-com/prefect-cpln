import os
import pytest
from unittest.mock import patch, MagicMock
from prefect.server.schemas.core import FlowRun
from prefect_cpln.credentials import CplnConfig, CplnClient
from prefect_cpln.worker import CplnWorkerJobConfiguration, CplnWorker
from prefect_cpln import constants

# ------------------------------------------------------------------
# FIXTURES FOR UNIT TESTS (mocks for environment variables)
# ------------------------------------------------------------------


@pytest.fixture
def mock_env_token(monkeypatch):
    """Sets a mock CPLN_TOKEN environment variable."""
    monkeypatch.setenv("CPLN_TOKEN", "mock_token")


@pytest.fixture
def mock_env_org(monkeypatch):
    """Sets a mock CPLN_ORG environment variable."""
    monkeypatch.setenv("CPLN_ORG", "mock_org")


@pytest.fixture
def mock_env_gvc(monkeypatch):
    """Sets a mock CPLN_GVC environment variable."""
    monkeypatch.setenv("CPLN_GVC", "mock_gvc")


@pytest.fixture
def mock_env_location(monkeypatch):
    """Sets a mock CPLN_LOCATION environment variable."""
    monkeypatch.setenv("CPLN_LOCATION", "mock_location")


@pytest.fixture
def mock_env_endpoint(monkeypatch):
    """Fixture to set a mock CPLN_ORG environment variable."""
    monkeypatch.setenv("CPLN_ENDPOINT", "https://api.test.cpln.io")


@pytest.fixture
def mock_env_logs_endpoint(monkeypatch):
    """Fixture to set a mock CPLN_ORG environment variable."""
    monkeypatch.setenv("CPLN_LOGS_ENDPOINT", "wss://logs.test.cpln.io")


# ------------------------------------------------------------------
# UNIT TESTS: CplnWorkerJobConfiguration
# ------------------------------------------------------------------


def test_config_property():
    """Verify config is stored if provided."""
    c = CplnConfig(token="test_token")
    config = CplnWorkerJobConfiguration(config=c)
    assert config.config == c


def test_org_defaults_to_env(mock_env_org):
    """Verify org is taken from environment if not provided."""
    config = CplnWorkerJobConfiguration()
    assert config.org == "mock_org"


def test_org_explicit_override(mock_env_org):
    """Verify org is taken from explicit constructor argument if provided."""
    config = CplnWorkerJobConfiguration(org="explicit_org")
    assert config.org == "explicit_org"


def test_namespace_defaults_to_env(mock_env_gvc):
    """Verify namespace (GVC) is taken from environment if not provided."""
    config = CplnWorkerJobConfiguration()
    assert config.namespace == "mock_gvc"


def test_namespace_explicit_override(mock_env_gvc):
    """Verify namespace is taken from explicit constructor argument if provided."""
    config = CplnWorkerJobConfiguration(namespace="explicit_gvc")
    assert config.namespace == "explicit_gvc"


def test_location_defaults_to_env(mock_env_location):
    """Verify location is taken from environment if not provided."""
    config = CplnWorkerJobConfiguration()
    assert config.location == "mock_location"


def test_location_explicit_override(mock_env_location):
    """Verify location is taken from explicit constructor argument if provided."""
    config = CplnWorkerJobConfiguration(location="explicit_location")
    assert config.location == "explicit_location"


@patch("prefect_cpln.worker.CplnWorkerJobConfiguration._get_first_location")
def test_location_falls_back_to_first_location(mock_first_loc, mock_env_location):
    """
    If location is not set (env variable is cleared), `_get_first_location` is called,
    and the returned location is used.
    """

    # Clear the environment variable
    os.environ.pop("CPLN_LOCATION", None)
    mock_first_loc.return_value = "aws-eu-central-1"

    config = CplnWorkerJobConfiguration(
        config=CplnConfig(token=os.getenv("CPLN_TOKEN")),
        org="mock_org",
        namespace="mock_gvc",
        location=None,
    )
    config.prepare_location()

    assert config.location == "aws-eu-central-1"
    mock_first_loc.assert_called_once()
