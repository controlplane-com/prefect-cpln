"""Utilities for working with the Python Control Plane API."""

import json
import asyncio
import os
import websockets
import time
from datetime import datetime
from logging import Logger
from typing import Callable, List, Optional
from urllib.parse import urlencode
from slugify import slugify
from prefect_cpln import constants
from prefect_cpln.common import watch_job_until_completion
from prefect_cpln.credentials import CplnClient


### Classes ###


class CplnLogsMonitor:
    """Allows you to monitor logs of a running job."""

    def __init__(
        self,
        logger: Logger,
        client: CplnClient,
        org: str,
        gvc: str,
        location: str,
        workload_name: str,
        command_id: str,
        interval_seconds: Optional[int] = None,
    ):
        # Received attributes
        self._logger = logger
        self._client = client
        self._org = org
        self._gvc = gvc
        self._location = location
        self._workload_name = workload_name
        self._command_id = command_id
        self._interval_seconds = interval_seconds
        self.logs_url = os.getenv("CPLN_LOGS_ENDPOINT", constants.LOGS_URL)

        # Defined attirbutes
        self.completed = False
        self.job_status = None
        self._websocket = None

    ### Public Methods ###

    async def monitor(self, print_func: Optional[Callable] = None):
        """
        Monitor the job status and logs.

        Args:
            print_func (Optional[Callable]): If provided, it will stream the logs by calling `print_func`
            for every log message received from the logs service.
        """

        # Define tasks
        monitor_status_task = asyncio.create_task(self._monitor_job_status())
        monitor_logs_task = asyncio.create_task(self._monitor_job_logs(print_func))

        # Store tasks into a list
        tasks = [monitor_status_task, monitor_logs_task]

        # Wait for either task to complete
        _, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Ensure WebSocket disconnects if the status monitoring triggers disconnection
        if self._websocket and not self._websocket.closed:
            # Close the WebSocket connection
            await self._websocket.close()

        # Cancel remaining tasks
        for task in pending:
            task.cancel()

    ### Private Methods ###

    async def _monitor_job_status(self):
        """
        Fetch the job status from the API and check if it has completed.
        Sets `self.completed` to True if the job has completed, failed, or cancelled.
        """

        # Watch the job until its completion
        self.job_status = await watch_job_until_completion(
            self._client,
            self._org,
            self._gvc,
            self._workload_name,
            self._command_id,
            self._interval_seconds,
        )

        # If the job completed, then we should get here
        self.completed = True

    async def _monitor_job_logs(self, print_func: Optional[Callable] = None):
        """
        Connect to the logs service via a WebSocket connection and handle incoming messages until job completes.

        Args:
            print_fund (Optional[Callable]): If provided, it will stream the logs by calling `print_func`
            for every log message received from the logs service.
        """

        # Current time in milliseconds
        now_millis = int(time.time() * 1000)

        # Subtract the offset in milliseconds
        offset_millis = constants.JOB_WATCH_OFFSET_MINUTES * 60 * 1000
        offset_time_millis = now_millis - offset_millis

        # Convert to nanoseconds
        nanos = f"{offset_time_millis * 1_000_000}"

        # Construct the query parameters
        query_params = {
            "query": f'{{gvc="{self._org}", location="{self._location}", workload="{self._workload_name}", replica=~"command-{self._command_id}.+"}}',
            "start": nanos,
            "direction": "forward",
            "limit": 1000,
        }

        # Construct the WebSocket URL with the query parameters
        url = f"{self.logs_url}/logs/org/{self._org}/loki/api/v1/tail?{urlencode(query_params)}"

        # Set the authorization token in the headers
        headers = {"Authorization": self._client.token}

        try:
            # Connect to the WebSocket using the WebSocket URL and headers
            async with websockets.connect(
                url,
                extra_headers=headers,
                ping_interval=constants.WEB_SOCKET_PING_INTERVAL_MS,
            ) as websocket:
                # Set the WebSocket connection
                self._websocket = websocket

                try:
                    # Continue receiving messages until the job completes
                    while not self.completed:
                        # Receive a message from the WebSocket
                        message = await websocket.recv()

                        # Parse the message as JSON
                        obj = json.loads(message)

                        # Extract the streams from the message
                        for stream in obj.get("streams", []):
                            # Extract the values from the stream
                            for value in stream.get("values", []):
                                # Extract the timestamp and log message from the value
                                timestamp_ns = int(value[0])
                                log_message = value[1]

                                # Convert timestamp from nanoseconds to a readable format
                                timestamp = datetime.fromtimestamp(
                                    timestamp_ns / 1_000_000_000
                                )

                                # Call the print_func and send the timestamp and log message if specified
                                if print_func is not None:
                                    print_func(f"{timestamp} - {log_message}")

                except websockets.ConnectionClosed as e:
                    self._logger.info(f"Connection to logs service has closed: {e}")

        except Exception as e:
            # Log an error message if an exception occurs while streaming logs
            self._logger.warning(
                (
                    "Error occurred while streaming logs - "
                    "Job will continue to run but logs will "
                    "no longer be streamed to stdout."
                ),
                exc_info=True,
            )

            # Wait for the job to complete
            while not self.completed:
                # Pause for a second before checking the job completion again
                await asyncio.sleep(1)


### Helper Functions ###


def slugify_name(name: str, max_length: int = 45) -> Optional[str]:
    """
    Slugify text for use as a name.

    Keeps only alphanumeric characters and dashes, and caps the length
    of the slug at 45 chars.

    The 45 character length allows room for the k8s utility
    "generateName" to generate a unique name from the slug while
    keeping the total length of a name below 63 characters, which is
    the limit for e.g. label names that follow RFC 1123 (hostnames) and
    RFC 1035 (domain names).

    Args:
        name: The name of the job

    Returns:
        The slugified job name or None if the slugified name is empty
    """
    slug = slugify(
        name,
        max_length=max_length,  # Leave enough space for generateName
        regex_pattern=r"[^a-zA-Z0-9-]+",
    )

    return slug if slug else None


def slugify_label_key(key: str, max_length: int = 63, prefix_max_length=253) -> str:
    """
    Slugify text for use as a label key.

    Keys are composed of an optional prefix and name, separated by a slash (/).

    Keeps only alphanumeric characters, dashes, underscores, and periods.
    Limits the length of the label prefix to 253 characters.
    Limits the length of the label name to 63 characters.

    See https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set

    Args:
        key: The label key

    Returns:
        The slugified label key
    """  # noqa
    if "/" in key:
        prefix, name = key.split("/", maxsplit=1)
    else:
        prefix = None
        name = key

    name_slug = (
        slugify(name, max_length=max_length, regex_pattern=r"[^a-zA-Z0-9-_.]+").strip(
            "_-."  # Must start or end with alphanumeric characters
        )
        or name
    )
    # Fallback to the original if we end up with an empty slug, this will allow
    # Kubernetes to throw the validation error

    if prefix:
        prefix_slug = (
            slugify(
                prefix,
                max_length=prefix_max_length,
                regex_pattern=r"[^a-zA-Z0-9-\.]+",
            ).strip(
                "_-."
            )  # Must start or end with alphanumeric characters
            or prefix
        )

        return f"{prefix_slug}/{name_slug}"

    return name_slug


def slugify_label_value(value: str, max_length: int = 63) -> str:
    """
    Slugify text for use as a label value.

    Keeps only alphanumeric characters, dashes, underscores, and periods.
    Limits the total length of label text to below 63 characters.

    See https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#syntax-and-character-set

    Args:
        value: The text for the label

    Returns:
        The slugified value
    """  # noqa
    slug = (
        slugify(value, max_length=max_length, regex_pattern=r"[^a-zA-Z0-9-_\.]+").strip(
            "_-."  # Must start or end with alphanumeric characters
        )
        or value
    )
    # Fallback to the original if we end up with an empty slug, this will allow
    # Kubernetes to throw the validation error

    return slug
