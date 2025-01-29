"""Module to define common exceptions within `prefect_cpln`."""

from requests.exceptions import HTTPError, RequestException


class CplnJobDefinitionError(RequestException):
    """An exception for when a Control Plane job definition is invalid."""


class CplnJobFailedError(RequestException):
    """An exception for when a Control Plane job fails."""


class CplnResourceNotFoundError(HTTPError):
    """An exception for when a Control Plane resource cannot be found by a client."""


class CplnJobTimeoutError(RequestException):
    """An exception for when a Control Plane job times out."""
