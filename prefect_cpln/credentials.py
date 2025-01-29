"""Module for defining Control Plane credential handling and client generation."""

import os
import time
import logging
import requests
from typing import Optional
from prefect.blocks.core import Block
from pydantic.version import VERSION as PYDANTIC_VERSION
from prefect_cpln import constants

if PYDANTIC_VERSION.startswith("2."):
    from pydantic.v1 import Field
else:
    from pydantic import Field


class CplnClient:
    """
    Control Plane API client.
    """

    def __init__(self, token: Optional[str] = None):
        self.token = token
        self.logger = logging.getLogger(__name__)
        self.endpoint = os.getenv("CPLN_ENDPOINT", constants.DEFAULT_DATA_SERVICE_URL)

        if not self.token:
            self.token = os.getenv("CPLN_TOKEN")

    ### Public Methods ###

    def get(self, path: str, **kwargs) -> dict:
        """
        Sends a GET request to the specified API path.

        Args:
            path: The API path to make the request to.
            kwargs: Additional keyword arguments to pass to the `requests.request` method.

        Returns:
            dict: The JSON response as a dictionary.
        """

        # Log the GET request to the specified path
        self.logger.debug(f"[cpln_client_get] making GET request to {path}")

        # Make the GET request to the given path using the centralized retry logic
        response = self._make_cpln_request_with_retry("get", path, **kwargs)

        # Parse the response as JSON and return it to the caller
        return response.json()

    def post(self, path: str, body: dict, **kwargs) -> requests.Response:
        """
        Sends a POST request to the specified resource path.

        Args:
            path: The API path to make the request to.
            body: JSON body of the request.
            kwargs: Additional keyword arguments to pass to the `requests.request` method.

        Returns:
            requests.Response: The response object for the POST request.
        """

        # Log the POST request to the specified path
        self.logger.debug(f"[cpln_client_post] making POST request to {path}")

        # Make the POST request to the given path using the centralized retry logic
        return self._make_cpln_request_with_retry("post", path, json=body, **kwargs)

    def put(self, path: str, body: dict, **kwargs) -> requests.Response:
        """
        Sends a PUT request to the specified resource path.

        Args:
            path: The API path to make the request to.
            body: JSON body of the request.
            kwargs: Additional keyword arguments to pass to the `requests.request` method.

        Returns:
            requests.Response: The response object for the PUT request.
        """

        # Log the PUT request to the specified path
        self.logger.debug(f"[cpln_client_put] making PUT request to {path}")

        # Make the PUT request to the given path using the centralized retry logic
        return self._make_cpln_request_with_retry("put", path, json=body, **kwargs)

    def patch(self, path: str, body: dict, **kwargs) -> requests.Response:
        """
        Sends a PATCH request to the specified resource path.

        Args:
            path: The API path to make the request to.
            body: JSON body of the request.

        Returns:
            requests.Response: The response object for the PATCH request.
        """

        # Log the PATCH request to the specified path
        self.logger.debug(f"[cpln_client_patch] making PATCH request to {path}")

        # Make the PATCH request to the given path using the centralized retry logic
        return self._make_cpln_request_with_retry("patch", path, json=body, **kwargs)

    def delete(self, path: str, **kwargs) -> requests.Response:
        """
        Sends a DELETE request to the specified resource path.

        Args:
            path: The API path to make the request to.
            kwargs: Additional keyword arguments to pass to the `requests.request` method.
        """
        self.logger.debug(f"[cpln_client_delete] making DELETE request to {path}")

        # Make the DELETE request to the given path using the centralized retry logic
        return self._make_cpln_request_with_retry("delete", path, **kwargs)

    ### Private Methods ###
    def _make_cpln_request_with_retry(
        self, method, path, **kwargs
    ) -> requests.Response:
        """
        Centralized retry logic for HTTP requests.

        Args:
            method: The HTTP method to use (e.g., 'get', 'post', 'put', 'patch', 'delete').
            path: The API path to make the request to.
            kwargs: Additional keyword arguments to pass to the `requests.request` method.

        Returns:
            requests.Response: The `requests.Response` object if the request succeeds.

        Raises:
            Exception: If all retry attempts fail or if a non-429 error occurs.
            HTTPError: If status code is >= 400.
        """
        # Initialize the retry count to track the number of attempts
        retry_count = 0

        # Construct the full URL by combining the base URL and the given path
        url = f"{self.endpoint}{path}"

        # Create headers for the request, including the authorization token and content type
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

        # Ensure that the headers are included in the keyword arguments
        kwargs["headers"] = headers

        # Start the retry loop; it will run until the retry limit is reached
        while retry_count <= constants.HTTP_REQUEST_MAX_RETRIES:
            # Log retries, but not the first attempt
            if retry_count > 0:
                self.logger.debug(
                    f"[_make_cpln_request_with_retry] {method.upper()} request to {url}, Retry {retry_count}"
                )

            # Perform the HTTP request using the `requests.request` method
            response = requests.request(
                method, url, timeout=constants.HTTP_REQUEST_TIMEOUT, **kwargs
            )

            # If the response status code is not 429, handle it as a normal response
            if response.status_code != 429:
                # If the response is not successful (status codes >= 400), raise an exception
                try:
                    response.raise_for_status()

                    # Return the response object for successful requests
                    return response
                except requests.HTTPError as e:
                    # Log the error message and raise an exception
                    self.logger.error(
                        f"[_make_cpln_request_with_retry] Http Error: {response.text}"
                    )
                    raise e

            # If the response status code is 429, log a warning and prepare to retry
            self.logger.warning(
                f"[_make_cpln_request_with_retry] Received 429 for {method.upper()} {url} Error: {response.text}, retrying..."
            )

            # Increment the retry count for the next attempt
            retry_count += 1

            # Calculate the delay using exponential backoff: BASE_DELAY * 2^retry_count
            delay = constants.HTTP_REQUEST_RETRY_BASE_DELAY * (2**retry_count)

            # Log the delay to keep track of wait times between retries
            self.logger.debug(
                f"[_make_cpln_request_with_retry] Retry {retry_count}: Waiting for {delay} seconds before retrying request..."
            )

            # Pause execution for the calculated delay time before retrying
            time.sleep(delay)

        # If all retries are exhausted, raise an exception indicating the failure
        raise Exception(
            f"Failed after {constants.HTTP_REQUEST_MAX_RETRIES} retries for {method.upper()} {url}"
        )


class CplnConfig(Block):
    """
    Stores configuration for interaction with the Control Plane platform.

    Attributes:
        token: The Control Plane Service Account token of a specific organization.

    Example:
        Load a saved Control Plane config:
        ```python
        from prefect_cpln.credentials import CplnConfig

        cpln_config_block = CplnConfig.load("BLOCK_NAME")
        ```
    """

    ### Public Properties ###
    token: str = Field(
        default_factory=lambda: os.getenv("CPLN_TOKEN"),
        description="The Control Plane Service Account token of a specific organization. Defaults to the value specified in the environment variable CPLN_TOKEN.",
    )

    ### Private Properties ###
    _block_type_name = "Control Plane Configuration"
    _logo_url = "https://console.cpln.io/resources/logos/controlPlaneLogoOnly.svg"
    _documentation_url = "https://docs.controlplane.com"  # noqa

    ### Public Methods ###

    def get_api_client(self) -> CplnClient:
        """
        Returns a Control Plane API client.
        """

        return CplnClient(token=self.token)


class CplnCredentials(Block):
    """
    Credentials block for generating configured Control Plane API client.

    Attributes:
        config: A `CplnConfig` block holding a JSON Control Plane configuration.

    Example:
        Load a saved Control Plane credentials:
        ```python
        from prefect_cpln.credentials import CplnCredentials

        cpln_credentials = CplnCredentials.load("BLOCK_NAME")
        ```
    """

    ### Public Properties ###
    config: Optional[CplnConfig] = None

    ### Private Properties ###
    _block_type_name = "Control Plane Credentials"
    _logo_url = "https://console.cpln.io/resources/logos/controlPlaneLogoOnly.svg"
    _documentation_url = "https://docs.controlplane.com"  # noqa

    ### Public Methods ###

    def get_client(self, token: Optional[str] = None) -> CplnClient:
        """
        Returns an authenticated Control Plane API client.
        """

        # Get the API client from the specified CplnConfig
        if self.config:
            return self.config.get_api_client()

        # Initialize a new CplnClient with the token specified
        return CplnClient(token=token)
