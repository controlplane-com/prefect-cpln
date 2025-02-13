"""Module to define tasks for interacting with Control Plane jobs."""

import os
import time
import requests
import yaml
import asyncio
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Type, Union
from typing_extensions import Self
from prefect_cpln import constants
from prefect import task
from prefect.blocks.abstract import JobBlock, JobRun
from prefect.utilities.asyncutils import sync_compatible
from prefect_cpln.credentials import CplnClient, CplnCredentials
from prefect_cpln.exceptions import CplnJobTimeoutError
from prefect_cpln.utilities import CplnLogsMonitor

from pydantic import VERSION as PYDANTIC_VERSION

from prefect_cpln.worker import CplnKubernetesConverter, CplnWorkerJobConfiguration

if PYDANTIC_VERSION.startswith("2."):
    from pydantic.v1 import Field
else:
    from pydantic import Field


### Helper Tasks ###


@task
def create_workload(
    cpln_credentials: CplnCredentials,
    org: str,
    gvc: str,
    body: constants.CplnObjectManifest,
) -> constants.CplnObjectManifest:
    """
    Task for creating a Control Plane workload.

    Args:
        cpln_credentials (CplnCredentials): `CplnCredentials` block holding authentication needed to generate the required API client.
        org (str): The organization name.
        gvc (str): The GVC name to create the workload in.
        body (constants.CplnObjectManifest): A dict containing the workload definition.

    Returns:
        constants.CplnObjectManifest: A Control Plane workload.

    Example:
        Create a workload in the default GVC:
        ```python
        from prefect import flow
        from prefect_cpln.credentials import CplnCredentials
        from prefect_cpln.jobs import create_workload

        @flow
        def cpln_orchestrator():
            workload = create_workload(
                cpln_credentials=CplnCredentials.load("cpln-creds"),
                org="ORG_NAME",
                gvc="default",
                body={
                    'kind': 'workload',
                    'name': 'WORKLOAD_NAME',
                    'spec': {
                        ...
                    }
                    ...
                },
            )
        ```
    """

    # Construct the path of the workload
    path = f"/org/{org}/gvc/{gvc}/workload"

    # Get the Control Plane API client
    client = cpln_credentials.get_client()

    # Create / Update the workload
    client.put(path, body)

    # Extract workload name from the manifest
    workload_name = body["name"]

    # Construct workload link
    workload_link = f"{path}/{workload_name}"

    # Fetch and return the workload
    return client.get(workload_link)


@task
def delete_workload(
    cpln_credentials: CplnCredentials,
    org: str,
    gvc: str,
    name: str,
) -> None:
    """
    Task for deleting a Control Plane workload.

    Args:
        cpln_credentials (CplnCredentials): `CplnCredentials` block holding authentication needed to generate the required API client.
        org (str): The organization name.
        gvc (str): The GVC name that the worklaod belongs to.
        name (str): The name of the workload to delete.

    Example:
        Delete "my-workload" in the default GVC:
        ```python
        from prefect import flow
        from prefect_cpln.credentials import CplnCredentials
        from prefect_cpln.jobs import delete_workload

        @flow
        def cpln_orchestrator():
            delete_workload(
                cpln_credentials=CplnCredentials.load("cpln-creds"),
                org="ORG_NAME",
                gvc="default",
                name="my-workload",
            )
        ```
    """

    # Construct the path of the workload to delete
    path = f"/org/{org}/gvc/{gvc}/workload/{name}"

    # Delete the specified workload
    cpln_credentials.get_client().delete(path)


@task
def list_workloads(
    cpln_credentials: CplnCredentials,
    org: str,
    gvc: str,
) -> constants.CplnObjectManifest:
    """
    Task for listing Control Plane workloads.

    Args:
        cpln_credentials (CplnCredentials): `CplnCredentials` block holding authentication needed to generate the required API client.
        org (str): The organization name.
        gvc (str): The GVC name to fetch all workloads from.

    Returns:
        constants.CplnObjectManifest: A list dict containing workload items.

    Example:
        List all workloads in the default GVC:
        ```python
        from prefect import flow
        from prefect_cpln.credentials import CplnCredentials
        from prefect_cpln.jobs import list_workloads

        @flow
        def cpln_orchestrator():
            workloads_list = list_workloads(
                cpln_credentials=CplnCredentials.load("cpln-creds"),
                org="ORG_NAME",
                gvc="default",
            )
        ```
    """

    # Construct the path of the workload list
    path = f"/org/{org}/gvc/{gvc}/workload"

    # Fetch and return a list of workloads
    return cpln_credentials.get_client().get(path)


@task
def patch_workload(
    cpln_credentials: CplnCredentials,
    org: str,
    gvc: str,
    name: str,
    body: constants.CplnObjectManifest,
) -> constants.CplnObjectManifest:
    """
    Task for patching a GVC-scoped Control Plane workload.

    Args:
        cpln_credentials (CplnCredentials): `CplnCredentials` block holding authentication needed to generate the required API client.
        org (str): The organization name.
        gvc (str): The GVC name that the workload belongs to.
        name (str): The name of the workload to patch.
        body (constants.CplnObjectManifest): A Dict containing the workload's specification to patch.

    Returns:
        constants.CplnObjectManifest: The workload specification after the patch gets applied.

    Example:
        Patch a workload in the default GVC:
        ```python
        from prefect import flow
        from prefect_cpln.credentials import CplnCredentials
        from prefect_cpln.jobs import patch_workload

        @flow
        def cpln_orchestrator():
            workload = patch_workload(
                cpln_credentials=CplnCredentials.load("cpln-creds"),
                org="ORG_NAME",
                gvc="default",
                name="WORKLOAD_NAME",
                body={
                    'spec': {
                        ...
                    }
                    ...
                },
            )
        ```
    """

    # Construct the path of the workload
    path = f"/org/{org}/gvc/{gvc}/workload/{name}"

    # Get the Control Plane API client
    client = cpln_credentials.get_client()

    # Make the PATCH request
    client.patch(path, body)

    # Fetch and return the workload
    return client.get(path)


@task
def read_workload(
    cpln_credentials: CplnCredentials,
    org: str,
    gvc: str,
    name: str,
) -> constants.CplnObjectManifest:
    """
    Task for reading a GVC-scoped Control Plane workload.

    Args:
        cpln_credentials (CplnCredentials): `CplnCredentials` block holding authentication needed to generate the required API client.
        org (str): The organization name.
        gvc (str): The GVC name to read the workload from.
        name (str): The name of the workload to get.

    Returns:
        constants.CplnObjectManifest: The workload specification.

    Example:
        Read a workload in the default GVC:
        ```python
        from prefect import flow
        from prefect_cpln.credentials import CplnCredentials
        from prefect_cpln.jobs import read_workload

        @flow
        def cpln_orchestrator():
            workload = read_workload(
                cpln_credentials=CplnCredentials.load("cpln-creds"),
                org="ORG_NAME",
                gvc="default",
                name="WORKLOAD_NAME",
            )
        ```
    """

    # Construct the path of the workload
    path = f"/org/{org}/gvc/{gvc}/workload/{name}"

    # Fetch and return the workload
    return cpln_credentials.get_client().get(path)


@task
def replace_workload(
    cpln_credentials: CplnCredentials,
    org: str,
    gvc: str,
    body: constants.CplnObjectManifest,
) -> dict:
    """
    Task for replacing a GVC-scoped Control Plane workload.

    Args:
        cpln_credentials (CplnCredentials): `CplnCredentials` block holding authentication needed to generate the required API client.
        org (str): The organization name.
        gvc (str): The GVC name to replace the workload in.
        body (constants.CplnObjectManifest): A Dict containing the workload specification.

    Returns:
        constants.CplnObjectManifest: The workload specification after the replacement.

    Example:
        Replace a workload in the default GVC:
        ```python
        from prefect import flow
        from prefect_cpln.credentials import CplnCredentials
        from prefect_cpln.jobs import replace_workload

        @flow
        def cpln_orchestrator():
            workload = replace_workload(
                cpln_credentials=CplnCredentials.load("cpln-creds"),
                org="ORG_NAME",
                gvc="default",
                body={
                    'kind': 'workload',
                    'name': 'WORKLOAD_NAME',
                    'spec': {
                        ...
                    }
                    ...
                },
            )
        ```
    """

    # Construct the path of the workload
    path = f"/org/{org}/gvc/{gvc}/workload"

    # Get the Control Plane API client
    client = cpln_credentials.get_client()

    # Make a PUT request
    client.put(path, body)

    # Extract workload name from the manifest
    workload_name = body["name"]

    # Construct workload link
    workload_link = f"{path}/{workload_name}"

    # Fetch and return the workload
    return client.get(workload_link)


### Classes ###


class CplnJob(JobBlock):
    """A block representing a Control Plane job configuration."""

    ### Public Properties ###
    credentials: CplnCredentials = Field(
        default_factory=CplnCredentials,
        description="The credentials to configure a Control Plane API client from.",
    )
    org: str = Field(
        default_factory=lambda: os.getenv("CPLN_ORG"),
        description=(
            "The Control Plane organization to create jobs within. "
            "Defaults to the value in the environment variable CPLN_ORG. "
            "If you are hosting the worker on Control Plane, the environment variable will be automatically injected to your workload."
        ),
    )
    namespace: str = Field(
        default_factory=lambda: os.getenv("CPLN_GVC"),
        description=(
            "The Control Plane GVC to create jobs within. Defaults to the value in the environment variable CPLN_GVC. "
            "If you are hosting the worker on Control Plane, the environment variable will be automatically injected to your workload."
        ),
    )
    location: str = Field(
        default_factory=lambda: os.getenv("CPLN_LOCATION", "").split("/")[-1],
        description=(
            "The Control Plane GVC location. Defaults to the value in the environment variable CPLN_LOCATION. "
            "If the location is still not found, the first location of the specified GVC will be used. "
            "If you are hosting the worker on Control Plane, the environment variable will be automatically injected to your workload."
        ),
    )
    v1_job: constants.KubernetesObjectManifest = Field(
        default=...,
        title="Job Manifest",
        description=(
            "The Kubernetes job manifest to run. This dictionary can be produced "
            "using `yaml.safe_load`."
        ),
    )
    delete_after_completion: bool = Field(
        default=True,
        description="Whether to delete the job after it has completed.",
    )
    interval_seconds: int = Field(
        default=5,
        description="The number of seconds to wait between job status checks.",
    )
    timeout_seconds: Optional[int] = Field(
        default=None,
        description="The number of seconds to wait for the job run before timing out.",
    )

    ### Private Properties ###
    _block_type_name = "Control Plane Job"
    _block_type_slug = "cpln-job"
    _logo_url = "https://console.cpln.io/resources/logos/controlPlaneLogoOnly.svg"
    _documentation_url = "https://docs.controlplane.com"

    ### Public Methods ###

    @sync_compatible
    async def trigger(self):
        """Create a Control Plane job and return a `ControlPlaneJobRun` object."""

        # Initialize a worker configuration
        configuration = CplnWorkerJobConfiguration(
            config=self.credentials.config,
            org=self.org,
            namespace=self.namespace,
            job_manifest=self.v1_job,
        )

        # Prepare the configuration
        configuration.prepare_for_job_run()

        # Get the Control Plane API client
        client = self.credentials.get_client()

        # Initialize the Control Plane Kubernetes converter
        self.cpln_k8s_converter = CplnKubernetesConverter(
            self.logger,
            client,
            configuration.org,
            configuration.namespace,
            configuration.job_manifest,
        )

        # Convert the Kubernetes job to a Control Plane workload
        workload = self.cpln_k8s_converter.convert()

        # Create reliant resources
        self.cpln_k8s_converter.create_reliant_resources()

        # If no location was specified, use the one from the configuration
        if not self.location:
            self.location = configuration.location

        # Create the workload
        created_workload = create_workload.fn(
            cpln_credentials=self.credentials,
            org=self.org,
            gvc=self.namespace,
            body=workload,
        )

        # Extract the job name from the job manifest
        workload_name = created_workload["name"]

        # Log a message to indicate that we are waiting for workload readiness
        self.logger.info(
            f"Waiting for the cron workload '{workload_name}' to become ready..."
        )

        # Wait for the workload to become ready before starting the job
        self._wait_until_ready(configuration, workload_name, client)

        # Log a message to indicate that workload is ready
        self.logger.info("Workload is ready!")

        # Start the job and get its ID
        command_id = self._start_cpln_job(created_workload)

        # Return a CplnJobRun object
        return CplnJobRun(
            cpln_job=self, workload=created_workload, command_id=command_id
        )

    @classmethod
    def from_yaml_file(
        cls: Type[Self], manifest_path: Union[Path, str], **kwargs
    ) -> Self:
        """
        Create a `CplnJob` from a YAML file.

        Args:
            manifest_path: The YAML file to create the `CplnJob` from.

        Returns:
            A CplnJob object.
        """

        # Load the YAML file into a dict variable
        with open(manifest_path, "r") as yaml_stream:
            yaml_dict = yaml.safe_load(yaml_stream)

        # Return a new CplnJob instance
        return cls(v1_job=yaml_dict, **kwargs)

    ### Private Methods ###

    def _wait_until_ready(
        self,
        configuration: CplnWorkerJobConfiguration,
        workload_name: str,
        client: CplnClient,
    ):
        """
        Wait until the created workload is ready.

        This method repeatedly fetches data from the deployment URL of the workload,
        checking if it is ready. If not ready, it waits before retrying, up to the
        specified timeout in pod_watch_timeout_seconds.

        Args:
            configuration (CplnWorkerJobConfiguration): The worker configuration.
            workload_name (str): The workload name.
            client (CplnClient): The Control Plane API client.

        Raises:
            TimeoutError: If the endpoint does not become ready within the timeout period.
        """

        # Record the start time to track how long the function has been running
        start_time = time.time()

        # Construct the workload link
        workload_link = f"/org/{configuration.org}/gvc/{configuration.namespace}/workload/{workload_name}"

        # Construct the workload deployments link
        workload_deployments_link = f"{workload_link}/deployment"

        # Start an infinite loop that will keep checking until the timeout is reached
        while True:
            # Fetch workload deployments
            deployment_list = client.get(workload_deployments_link)

            # Check if the response indicates readiness
            if self._check_workload_readiness(deployment_list, configuration.location):
                return

            # Check if timeout has been exceeded
            elapsed_time = time.time() - start_time
            if elapsed_time >= configuration.pod_watch_timeout_seconds:
                raise CplnJobTimeoutError(
                    f"Timeout exceeded while waiting for {workload_deployments_link} to become ready."
                )

            # Wait before the next attempt
            time.sleep(constants.RETRY_WORKLOAD_READY_CHECK_SECONDS)

    def _check_workload_readiness(self, deployment_list: dict, location: str) -> bool:
        """
        Check if the selected workload location is ready.

        This method iterates over a list of deployments and checks whether the
        deployment associated with the selected location is marked as ready.

        Args:
            deployment_list (dict): A dictionary containing deployment details,
                                    where each deployment has a name and readiness status.
            location (str): The location name of where the job will run.

        Returns:
            bool: True if the selected location is ready, False otherwise.
        """

        # Iterate over each deployment and see if the current selected location is ready or not
        for deployment in deployment_list["items"]:
            # Skip any other location and look for the selected location
            if deployment["name"] != location:
                continue

            # If we got here, then the location was found, return the value of the ready attribute
            return deployment.get("status", {}).get("ready", False)

        # If deployment was not found, return False
        return False

    def _start_cpln_job(self, workload: constants.CplnObjectManifest) -> str:
        """
        Starts a new job in the specified location and workload.

        Args:
            workload (Manifest): The Control Plane cron workload manifest.

        Returns:
            The job ID of the newly started job.
        """

        # Extract the name of the workload from the manifest
        name = workload["name"]

        # Construct the path to start a new job in the specified location and workload
        path = f"/org/{self.org}/gvc/{self.namespace}/workload/{name}/-command"

        # Construct the command to start a new job
        command = {
            "type": "runCronWorkload",
            "spec": {"location": self.location, "containerOverrides": []},
        }

        # Make a POST request to start a new job in the specified location and workload
        response: requests.Response = self.credentials.get_client().post(path, command)

        # If the response status code is not 201, raise an exception with the response text
        if response.status_code != 201:
            raise Exception(f"Failed to start job: {response.text}")

        # Extract the job ID from the response headers
        id = response.headers["location"].split("/")[-1]

        # Log the successful start of the job and return the job ID
        self.logger.info(
            f"[CplnJob] Started job with ID: {id} in location {self.location}"
        )

        # Set the job ID and exit the function
        return id


class CplnJobRun(JobRun[Dict[str, Any]]):
    """A container representing a run of a Control Plane job."""

    def __init__(
        self,
        cpln_job: CplnJob,
        workload: Dict[str, Any],
        command_id: str,
    ):
        # Received attributes
        self.logs = []
        self._completed = False
        self._cpln_job = cpln_job
        self._workload = workload
        self._workload_name = workload["name"]
        self._command_id = command_id

        # Internally defined attirbutes
        self._client = cpln_job.credentials.get_client()
        self._cpln_logs_monitor = CplnLogsMonitor(
            self.logger,
            self._client,
            cpln_job.org,
            cpln_job.namespace,
            cpln_job.location,
            self._workload_name,
            command_id,
            self._cpln_job.interval_seconds,
        )

    ### Public Methods ###

    @sync_compatible
    async def wait_for_completion(self, print_func: Optional[Callable] = None):
        """
        Waits for the job to complete.

        If the job has `delete_after_completion` set to `True`,
        the job will be deleted if it is observed by this method
        to enter a completed state.

        Args:
            print_func (Optional[Callable]): If provided, it will stream the logs by calling `print_func`
            for every log message received from the logs service.

        Raises:
            RuntimeError: If the Control Plane job fails.
            CplnJobTimeoutError: If the Control Plane job times out.
            ValueError: If `wait_for_completion` is never called.
        """

        # Log a message
        self.logger.info("Waiting for the job to complete.")

        # Initialize the logs list
        self.log = []

        try:
            # Wait for the job to complete and handle logs
            await asyncio.wait_for(
                self._cpln_logs_monitor.monitor(
                    lambda message: self._add_log_message(message, print_func)
                ),
                timeout=self._cpln_job.timeout_seconds,
            )
        except asyncio.TimeoutError:
            raise CplnJobTimeoutError(
                f"Job timed out after {self._cpln_job.timeout_seconds} seconds."
            )

        # If the job was not completed, then raise a runtime error
        if self._cpln_logs_monitor.job_status != "completed":
            raise RuntimeError(
                f"Job {self._workload_name} hasn't complete due to status "
                f"{self._cpln_logs_monitor.job_status}, check the logs for more information."
            )

        # Mark as completed
        self._completed = True

        # Log the complete message
        self.logger.info(f"Job {self._workload_name} has completed.")

        # Perform cleanup if specified by the user
        if self._cpln_job.delete_after_completion:
            await self._cleanup()

    @sync_compatible
    async def fetch_result(self) -> str:
        """
        Fetch the results of the job.

        Returns:
            The logs from the running job.

        Raises:
            ValueError: If this method is called when the job has a non-terminal state.
        """

        # Raise a ValueError exception if the job hasn't completed yet
        if not self._completed:
            raise ValueError(
                "The Control Plane Job run is not in a completed state - "
                "be sure to call `wait_for_completion` before attempting "
                "to fetch the result."
            )

        # Return the logs as single string
        return "\n".join(self.logs)

    ### Private Methods ###

    async def _cleanup(self):
        """Deletes the Control Plane cron job workload."""

        # Perform workload deletion
        delete_workload.fn(
            cpln_credentials=self._cpln_job.credentials,
            org=self._cpln_job.org,
            gvc=self._cpln_job.namespace,
            name=self._workload_name,
        )

        # Delete the reliant resources if the converter is found
        if self._cpln_job.cpln_k8s_converter:
            self._cpln_job.cpln_k8s_converter.delete_reliant_resources()

        # Log the delete message
        self.logger.info(f"Job {self._workload_name} deleted.")

    def _add_log_message(self, message: str, print_func: Optional[Callable] = None):
        """
        Receives a message and adds it to the log list.

        Args:
            message (str): A log message.
        """

        # Call the print func if specified
        if print_func is not None:
            print_func(message)

        # Add the message to the log list
        self.logs.append(message)
