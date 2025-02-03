"""

Module containing the Control Plane worker used for executing flow runs as Control Plane cron jobs.

To start a Control Plane worker, run the following command:

```bash
prefect worker start --pool 'my-work-pool' --type cpln
```

Replace `my-work-pool` with the name of the work pool you want the worker
to poll for flow runs.

### Securing your Prefect Cloud API key
If you are using Prefect Cloud and would like to pass your Prefect Cloud API key to
created jobs via a Control Plane secret, set the
`PREFECT_CPLN_WORKER_STORE_PREFECT_API_IN_SECRET` environment variable before
starting your worker:

```bash
export PREFECT_CPLN_WORKER_STORE_PREFECT_API_IN_SECRET="true"
prefect worker start --pool 'my-work-pool' --type cpln
```

Note that your work will need permission to create secrets in the same GVC(s)
that Control Plane cron jobs are created in to execute flow runs.

### Using a custom Control Plane job manifest template

The default template used for Control Plane job manifests looks like this:
```yaml
---
apiVersion: batch/v1
kind: Job
metadata:
labels: "{{ labels }}"
namespace: "{{ namespace }}"
generateName: "{{ name }}"
spec:
ttlSecondsAfterFinished: "{{ finished_job_ttl }}"
template:
    spec:
    parallelism: 1
    completions: 1
    restartPolicy: Never
    serviceAccountName: "{{ service_account_name }}"
    containers:
    - name: prefect-job
        env: "{{ env }}"
        image: "{{ image }}"
        args: "{{ command }}"
```

Each values enclosed in `{{ }}` is a placeholder that will be replaced with
a value at runtime. The values that can be used a placeholders are defined
by the `variables` schema defined in the base job template.

The default job manifest and available variables can be customized on a work pool
by work pool basis. These customizations can be made via the Prefect UI when
creating or editing a work pool.

For example, if you wanted to allow custom memory requests for a Control Plane work
pool you could update the job manifest template to look like this:

```yaml
---
apiVersion: batch/v1
kind: Job
metadata:
labels: "{{ labels }}"
namespace: "{{ namespace }}"
generateName: "{{ name }}-"
spec:
ttlSecondsAfterFinished: "{{ finished_job_ttl }}"
template:
    spec:
    parallelism: 1
    completions: 1
    restartPolicy: Never
    serviceAccountName: "{{ service_account_name }}"
    containers:
    - name: prefect-job
        env: "{{ env }}"
        image: "{{ image }}"
        args: "{{ command }}"
        resources:
            requests:
                memory: "{{ memory }}Mi"
            limits:
                memory: 128Mi
```

In this new template, the `memory` placeholder allows customization of the memory
allocated to Control Plane jobs created by workers in this work pool, but the limit
is hard-coded and cannot be changed by deployments.

For more information about work pools and workers,
checkout out the [Prefect docs](https://docs.prefect.io/v3/develop/write-flows).
"""

import asyncio
import base64
import logging
import os
import shlex
import anyio.abc
import requests
from tenacity import retry, stop_after_attempt, wait_fixed, wait_random
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
)

import prefect
from prefect.client.schemas import FlowRun
from prefect.server.schemas.core import Flow
from prefect.server.schemas.responses import DeploymentResponse
from prefect.utilities.dockerutils import get_prefect_image_name
from prefect.utilities.pydantic import JsonPatch
from prefect.utilities.templating import find_placeholders
from prefect.exceptions import (
    InfrastructureError,
    InfrastructureNotAvailable,
    InfrastructureNotFound,
)
from prefect.workers.base import (
    BaseJobConfiguration,
    BaseVariables,
    BaseWorker,
    BaseWorkerResult,
)
from prefect_cpln import constants
from prefect_cpln.credentials import CplnClient, CplnConfig
from prefect_cpln.utilities import (
    CplnLogsMonitor,
    slugify_label_key,
    slugify_label_value,
    slugify_name,
)

from pydantic import VERSION as PYDANTIC_VERSION

if PYDANTIC_VERSION.startswith("2."):
    from pydantic.v1 import BaseModel, Field, validator
else:
    from pydantic import BaseModel, Field, validator


### Helpers ###


def _get_base_job_manifest():
    """Returns a base job manifest to use for manifest validation."""
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"labels": {}},
        "spec": {
            "template": {
                "spec": {
                    "parallelism": 1,
                    "completions": 1,
                    "restartPolicy": "Never",
                    "containers": [
                        {
                            "name": "prefect-job",
                        }
                    ],
                }
            }
        },
    }


### Classes ###


class HashableCplnConfig(BaseModel, allow_mutation=False):
    """
    A hashable version of the CplnConfig class.
    Used for caching.
    """

    token: str = Field(
        default_factory=lambda: os.getenv("CPLN_TOKEN"),
        description="The Control Plane Service Account token of a specific organization. Defaults to the value specified in the environment variable CPLN_TOKEN",
    )


class CplnWorkerJobConfiguration(BaseJobConfiguration):
    """
    Configuration class used by the Control Plane worker.

    An instance of this class is passed to the Control Plane worker's `run` method
    for each flow run. It contains all of the information necessary to execute
    the flow run as a Control Plane cron job.

    Attributes:
        name: The name to give to created Control Plane cron job workload.
        command: The command executed in created Control Plane cron jobs to kick off
            flow run execution.
        env: The environment variables to set in created Control Plane cron jobs.
        labels: The tags to set on created Control Place cron jobs.
        org: The Control Plane organization name.
        namespace: The Control Plane GVC name to create Control Plane cron jobs in.
        job_manifest: The Kubernetes job manifest to use to create Control Plane cron jobs.
        config: The Configuration configuration to use for authentication
            to the Control Plane platform.
        job_watch_timeout_seconds: The number of seconds to wait for the job to
            complete before timing out. If `None`, the worker will wait indefinitely.
        pod_watch_timeout_seconds: Number of seconds to watch for the workload creation
            before timing out.
        stream_output: Whether or not to stream the job's output.
    """

    ### Public Properties ###

    config: Optional[CplnConfig] = Field(default=None)
    org: Optional[str] = Field(
        default_factory=lambda: os.getenv("CPLN_ORG"),
        description=(
            "The Control Plane organization to create jobs within. "
            "Defaults to the value in the environment variable CPLN_ORG. "
            "If you are hosting the worker on Control Plane, the environment variable will be automatically injected to your workload."
        ),
    )
    namespace: Optional[str] = Field(
        default_factory=lambda: os.getenv("CPLN_GVC"),
        description=(
            "The Control Plane GVC to create jobs within. Defaults to the value in the environment variable CPLN_GVC. "
            "If you are hosting the worker on Control Plane, the environment variable will be automatically injected to your workload."
        ),
    )
    location: Optional[str] = Field(
        default_factory=lambda: os.getenv("CPLN_LOCATION").split("/")[-1],
        description=(
            "The Control Plane GVC location. Defaults to the value in the environment variable CPLN_LOCATION. "
            "If the location is still not found, the first location of the specified GVC will be used. "
            "If you are hosting the worker on Control Plane, the environment variable will be automatically injected to your workload."
        ),
    )
    job_manifest: Dict[str, Any] = Field(default_factory=_get_base_job_manifest)
    env: Union[Dict[str, Optional[str]], List[Dict[str, Any]]] = Field(
        default_factory=dict
    )
    job_watch_timeout_seconds: Optional[int] = Field(
        default=None,
        description=(
            "Number of seconds to wait for the job to complete before marking it as"
            " crashed. Defaults to `None`, which means no timeout will be enforced."
        ),
    )
    pod_watch_timeout_seconds: int = Field(
        default=constants.HTTP_REQUEST_TIMEOUT,
        description="Number of seconds to watch for pod creation before timing out.",
    )
    stream_output: bool = Field(
        default=True,
        description=(
            "If set, output will be streamed from the job to local standard output."
        ),
    )

    ### Private Properties ###
    # internal-use only
    _api_dns_name: Optional[str] = None  # Replaces 'localhost' in API URL

    ### Public Methods ###

    def prepare_for_flow_run(
        self,
        flow_run: "FlowRun",
        deployment: Optional["DeploymentResponse"] = None,
        flow: Optional["Flow"] = None,
    ):
        """
        Prepares the job configuration for a flow run.

        Ensures that necessary values are present in the job manifest and that the
        job manifest is valid.

        Args:
            flow_run: The flow run to prepare the job configuration for
            deployment: The deployment associated with the flow run used for
                preparation.
            flow: The flow associated with the flow run used for preparation.
        """

        # Call the method from the parent class
        super().prepare_for_flow_run(flow_run, deployment, flow)

        # Update to use first location if not specified
        self.prepare_location()

        # Update configuration env and job manifest env
        self._update_prefect_api_url_if_local_server()
        self._populate_env_in_manifest()

        # Update labels in job manifest
        self._slugify_labels()

        # Add defaults to job manifest if necessary
        self._populate_image_if_not_present()
        self._populate_command_if_not_present()
        self._populate_generate_name_if_not_present()

    def prepare_location(self):
        if not self.location:
            self.location = self._get_first_location(
                self.config, self.org, self.namespace
            )

    ### Private Methods ###

    # Validators #

    @validator("job_manifest")
    def _ensure_metadata_is_present(cls, value: constants.CplnObjectManifest):
        """
        Ensures that the metadata is present in the job manifest.

        Args:
            value (Manifest): The job manifest as specified in the class.

        Returns:
            Manifest: The updated job manifest.
        """

        # If the metadata property is not part of the job manifest, added it
        if "metadata" not in value:
            value["metadata"] = {}

        # Return the upodated job manifest
        return value

    @validator("job_manifest")
    def _ensure_labels_is_present(cls, value: Dict[str, Any]):
        """
        Ensures that the labels are present in the job manifest.

        Args:
            value (Manifest): The job manifest as specified in the class.

        Returns:
            Manifest: The updated job manifest.
        """

        # If the labels property is not part of the job manifest, added it
        if "labels" not in value["metadata"]:
            value["metadata"]["labels"] = {}

        # Return the upodated job manifest
        return value

    @validator("job_manifest")
    def _ensure_namespace_is_present(cls, value: Dict[str, Any], values):
        """
        Ensures that the namespace is present in the job manifest.

        Args:
            value (Manifest): The job manifest as specified in the class.

        Returns:
            Manifest: The updated job manifest.
        """

        # If the namespace property is not part of the job manifest, added it
        if "namespace" not in value["metadata"]:
            value["metadata"]["namespace"] = values["namespace"]

        # Return the upodated job manifest
        return value

    @validator("job_manifest")
    def _ensure_job_includes_all_required_components(cls, value: Dict[str, Any]):
        """
        Ensures that the job manifest includes all required components.

        Args:
            value (Manifest): The job manifest as specified in the class.

        Raises:
            ValueError: If the job is missing required attributes.

        Returns:
            Manifest: The job manifest as is.
        """

        # Get the difference between base job manifest and the specified one
        patch = JsonPatch.from_diff(value, _get_base_job_manifest())

        # Find the missing paths between the two
        missing_paths = sorted([op["path"] for op in patch if op["op"] == "add"])

        # If there are missing paths, raise a ValueError exception
        if missing_paths:
            raise ValueError(
                "Job is missing required attributes at the following paths: "
                f"{', '.join(missing_paths)}"
            )

        # Return the value as is
        return value

    @validator("job_manifest")
    def _ensure_job_has_compatible_values(cls, value: Dict[str, Any]):
        """
        Ensures that the job manifest values are compatible.

        Args:
            value (Manifest): The job manifest as specified in the class.

        Raises:
            ValueError: If the job values are incompatible

        Returns:
            Manifest: The job manifest as is.
        """

        # Get the difference between base job manifest and the specified one
        patch = JsonPatch.from_diff(value, _get_base_job_manifest())

        # Find the incompatible values within the specified job manifest
        incompatible = sorted(
            [
                f"{op['path']} must have value {op['value']!r}"
                for op in patch
                if op["op"] == "replace"
            ]
        )

        # If there are incompatible, raise a ValueError exception
        if incompatible:
            raise ValueError(
                "Job has incompatible values for the following attributes: "
                f"{', '.join(incompatible)}"
            )

        # Return the value as is
        return value

    @validator("env", pre=True)
    def _coerce_env(cls, v):
        """
        Validator for the 'env' field.

        This method processes the 'env' field to ensure its value is properly coerced
        into the desired format. If the input is a list, it is returned unchanged.
        Otherwise, it transforms the input dictionary, converting all non-None
        values to strings and replacing None values with None explicitly.

        Args:
            v (Union[Dict[str, Optional[str]]): The value of the 'env' field, which can be either
                a list or a dictionary.

        Returns:
            Union[Dict[str, Optional[str]]: If the input is a list, it is returned as is. If the input
            is a dictionary, it is transformed as described above.
        """

        # Check if the input value is a list. If so, return it as is
        if isinstance(v, list):
            return v

        # If the input value is a dictionary, coerce its values.
        # Convert all non-None values to strings and replace None with None explicitly.
        return {k: str(v) if v is not None else None for k, v in v.items()}

    def _populate_env_in_manifest(self):
        """
        Populates environment variables in the job manifest.

        When `env` is templated as a variable in the job manifest it comes in as a
        dictionary. We need to convert it to a list of dictionaries to conform to the
        Kubernetes job manifest schema.

        This function also handles the case where the user has removed the `{{ env }}`
        placeholder and hard coded a value for `env`. In this case, we need to prepend
        our environment variables to the list to ensure Prefect setting propagation.
        An example reason the a user would remove the `{{ env }}` placeholder to
        hardcode Kubernetes secrets in the base job template.
        """

        transformed_env = [{"name": k, "value": v} for k, v in self.env.items()]
        template_env = self.job_manifest["spec"]["template"]["spec"]["containers"][
            0
        ].get("env")

        # If user has removed `{{ env }}` placeholder and hard coded a value for `env`,
        # we need to prepend our environment variables to the list to ensure Prefect
        # setting propagation.
        if isinstance(template_env, list):
            self.job_manifest["spec"]["template"]["spec"]["containers"][0]["env"] = [
                *transformed_env,
                *template_env,
            ]
        # Current templating adds `env` as a dict when the kubernetes manifest requires
        # a list of dicts. Might be able to improve this in the future with a better
        # default `env` value and better typing.
        else:
            self.job_manifest["spec"]["template"]["spec"]["containers"][0][
                "env"
            ] = transformed_env

    def _update_prefect_api_url_if_local_server(self):
        """
        If the API URL has been set by the base environment rather than the by the
        user, update the value to ensure connectivity when using a bridge network by
        updating local connections to use the internal host
        """
        # Check if PREFECT_API_URL is set and an internal DNS name is available
        if self.env.get("PREFECT_API_URL") and self._api_dns_name:
            # Replace local hostnames with the internal DNS name to enable bridge network access
            self.env["PREFECT_API_URL"] = (
                self.env["PREFECT_API_URL"]
                .replace("localhost", self._api_dns_name)
                .replace("127.0.0.1", self._api_dns_name)
            )

    def _slugify_labels(self):
        """Slugifies the labels in the job manifest."""

        # Combine existing labels from the job manifest with additional labels provided
        all_labels = {**self.job_manifest["metadata"].get("labels", {}), **self.labels}

        # Slugify each key-value pair and update the labels in the job manifest
        self.job_manifest["metadata"]["labels"] = {
            slugify_label_key(k): slugify_label_value(v) for k, v in all_labels.items()
        }

    def _populate_image_if_not_present(self):
        """
        Ensures that the image is present in the job manifest. Populates the image
        with the default Prefect image if it is not present.

        Raises:
        ValueError: If the job manifest does not have a valid template to verify
            or set the image.
        """

        try:
            # Check if the 'image' key is missing in the container definition
            if (
                "image"
                not in self.job_manifest["spec"]["template"]["spec"]["containers"][0]
            ):
                # Populate the image field with the default Prefect image
                self.job_manifest["spec"]["template"]["spec"]["containers"][0][
                    "image"
                ] = get_prefect_image_name()
        except KeyError:
            # Raise an error if the job manifest template is invalid
            raise ValueError(
                "Unable to verify image due to invalid job manifest template."
            )

    def _populate_command_if_not_present(self):
        """
        Ensures that the command is present in the job manifest. Populates the command
        with the `prefect -m prefect.engine` if a command is not present.

        Raises:
            ValueError: If the job manifest template is invalid, or if the 'command' is not
                a string or list.
        """

        try:
            # Retrieve the command arguments from the container definition.
            command = self.job_manifest["spec"]["template"]["spec"]["containers"][
                0
            ].get("args")

            # Populate the command field with the default base flow run command if missing
            if command is None:
                self.job_manifest["spec"]["template"]["spec"]["containers"][0][
                    "args"
                ] = shlex.split(self._base_flow_run_command())

            # If the command is a string, split it into a list for proper execution
            elif isinstance(command, str):
                self.job_manifest["spec"]["template"]["spec"]["containers"][0][
                    "args"
                ] = shlex.split(command)

            # Raise an error if the command is not a string or list
            elif not isinstance(command, list):
                raise ValueError(
                    "Invalid job manifest template: 'command' must be a string or list."
                )
        except KeyError:
            # Raise an error if the job manifest template is invalid and the command cannot be verified
            raise ValueError(
                "Unable to verify command due to invalid job manifest template."
            )

    def _populate_generate_name_if_not_present(self):
        """Ensures that the generateName is present in the job manifest."""

        # Retrieve the `generateName` field from the job manifest metadata, defaulting to an empty string if absent
        manifest_generate_name = self.job_manifest["metadata"].get("generateName", "")

        # Determine if the `generateName` contains any unresolved placeholders
        has_placeholder = len(find_placeholders(manifest_generate_name)) > 0

        # Check if the `generateName` field is set to a hyphen, indicating improper population during template rendering
        manifest_generate_name_templated_with_empty_string = (
            manifest_generate_name == "-"
        )

        # Handle cases where `generateName` is absent, improperly populated, or contains placeholders
        if (
            not manifest_generate_name
            or has_placeholder
            or manifest_generate_name_templated_with_empty_string
            or manifest_generate_name == "None-"
        ):
            # Initialize a new `generateName`
            generate_name = None

            # Attempt to generate a name by slugifying the job's `name` attribute
            if self.name:
                generate_name = slugify_name(self.name)

            # Fallback to "prefect-job" if slugifying the name fails
            if not generate_name:
                generate_name = "prefect-job"

            # Update the `generateName` field in the job manifest with the new value
            self.job_manifest["metadata"]["generateName"] = f"{generate_name}"

    def _get_first_location(self, config: CplnConfig, org: str, namespace: str):
        """
        Get the first location of the specified GVC (namespace).

        Raises:
            ValueError: If no location was found in the GVC.

        Returns:
            str: The name of the first location that the GVC is using.
        """

        # Get the GVC
        gvc = config.get_api_client().get(f"/org/{org}/gvc/{namespace}")

        # Check if a location is found in the GVC
        if gvc.get("spec", {}).get("staticPlacement", {}).get("locationLinks", []):
            # Extract the first location of the specified GVC
            location: str = gvc["spec"]["staticPlacement"]["locationLinks"][0]

            # Return the location name
            return location.split("/")[-1]
        else:
            # Raise an error if no location is found in the GVC
            raise ValueError(
                "ERROR: No location found in GVC. Please specify a location in the job or GVC."
            )

    ### Private Static Methods ###

    @staticmethod
    def _base_flow_run_labels(flow_run: "FlowRun") -> Dict[str, str]:
        """
        Generate a dictionary of labels for a flow run job.
        """
        return {
            "prefect.io/flow-run-id": str(flow_run.id),
            "prefect.io/flow-run-name": flow_run.name,
            "prefect.io/version": slugify_label_value(
                prefect.__version__.split("+")[0]
            ),
        }


class CplnKubernetesConverter:
    """Convert a Kubernetes job manifest to a Control Plane cron workload manifest."""

    def __init__(self, configuration: CplnWorkerJobConfiguration):
        self._configuration = configuration

    ### Public Methods ###

    def convert(self):
        """
        Converts the Kubernetes Job Manifest to a Control Plane Cron Workload Manifest.

        Returns:
            Manifest: The Control Plane Cron Workload Manifest.
        """

        # Get the pod spec from the job
        pod_spec = self._configuration.job_manifest["spec"]["template"]["spec"]

        # Get and map volumes
        volume_name_to_volume = self._map_and_get_kubernetes_job_volumes(pod_spec)

        # Create workload spec
        workload_spec = {
            "type": "cron",
            "containers": self._build_cpln_workload_containers(
                volume_name_to_volume, pod_spec["containers"][0]
            ),
            "job": self._build_cpln_workload_job_spec(pod_spec),
            "defaultOptions": {
                "capacityAI": False,
                "debug": False,
                "suspend": True,  # Prefect will be the one triggering schedules jobs, not us
            },
            "firewallConfig": {
                "external": {
                    "inboundAllowCIDR": [],
                    "inboundBlockedCIDR": [],
                    "outboundAllowCIDR": ["0.0.0.0/0"],
                    "outboundAllowHostname": [],
                    "outboundAllowPort": [],
                    "outboundBlockedCIDR": [],
                },
                "internal": {
                    "inboundAllowType": "none",
                    "inboundAllowWorkload": [],
                },
            },
        }

        # Set identity link
        if pod_spec.get("serviceAccountName"):
            # Extract the service account name
            service_account_name = pod_spec["serviceAccountName"]

            # Set the identity link
            workload_spec["identity_link"] = (
                f"//gvc/{self._configuration.namespace}/identity/{service_account_name}"
            )

        # Return the Control Plane cron workload manifest
        return {
            "kind": "workload",
            "name": self._configuration.job_manifest["metadata"]["generateName"],
            "tags": self._configuration.job_manifest["metadata"]["labels"],
            "spec": workload_spec,
        }

    ### Private Static Methods ###

    @staticmethod
    def _map_and_get_kubernetes_job_volumes(pod_spec: dict) -> dict:
        """
        Map Kubernetes job spec volumes to their names.

        Args:
            pod_spec: The Kubernetes job spec.

        Returns:
            dict: The volume name to volume mapping.
        """

        # Initialize the volume name to volume mapping
        volume_name_to_volume = {}

        # Map volume names to volumes
        for volume in pod_spec.get("volumes", []):
            volume_name_to_volume[volume["name"]] = volume

        # Return the volume name to volume mapping
        return volume_name_to_volume

    @staticmethod
    def _build_cpln_workload_containers(
        kubernetes_volumes: dict, kubernetes_container: dict
    ) -> list:
        """
        Builds a Control Plane cron workload container from a Kubernetes container.

        Args:
            kubernetes_volumes: The Kubernetes Job volumes.
            kubernetes_container: The Kubernetes container.

        Returns:
            list: The Control Plane cron workload containers.
        """

        # Get container resources
        resources = CplnKubernetesConverter._convert_kubernetes_job_resources(
            kubernetes_container
        )

        # Build workload container
        container = {
            "name": kubernetes_container["name"],
            "image": kubernetes_container["image"],
            "cpu": resources["cpu"],
            "memory": resources["memory"],
            "args": kubernetes_container["args"],
        }

        # Set command if specified
        if kubernetes_container.get("command"):
            container["command"] = " ".join(kubernetes_container["command"])

        # Set args if specified
        if kubernetes_container.get("args"):
            container["args"] = kubernetes_container["args"]

        # Set working directory
        if kubernetes_container.get("workingDir"):
            container["workingDir"] = kubernetes_container["workingDir"]

        # Set environment variables
        if kubernetes_container.get("env"):
            container["env"] = kubernetes_container["env"]

        # Set volume mounts
        if kubernetes_container.get("volumeMounts"):

            container["volumes"] = CplnKubernetesConverter._build_cpln_workload_volumes(
                kubernetes_volumes, kubernetes_container["volumeMounts"]
            )

        # Set lifecycle
        if kubernetes_container.get("lifecycle"):
            container["lifecycle"] = kubernetes_container["lifecycle"]

        # Return the containers list
        return [container]

    @staticmethod
    def _build_cpln_workload_job_spec(pod_spec: dict) -> dict:
        """
        Build Control Plane cron workload job spec from Kubernetes spec.

        Args:
            pod_spec: The Kubernetes job spec.

        Returns:
            dict: The Control Plane cron workload job spec.
        """

        # Job spec defaults
        schedule = "* * * * *"
        concurrency_policy = "Forbid"
        history_limit = 5
        restart_policy = "Never"

        # Define job spec
        job_spec = {
            "schedule": schedule,
            "concurrencyPolicy": concurrency_policy,
            "historyLimit": history_limit,
            "restartPolicy": restart_policy,
        }

        # Set restart policy
        if pod_spec.get("restartPolicy"):
            job_spec["restartPolicy"] = pod_spec["restartPolicy"]

        # Set active deadline seconds if found
        if pod_spec.get("activeDeadlineSeconds"):
            job_spec["activeDeadlineSeconds"] = pod_spec["activeDeadlineSeconds"]

        # Return job spec
        return job_spec

    @staticmethod
    def _convert_kubernetes_job_resources(container: dict) -> dict:
        """
        Convert Kubernetes job resources to Control Plane resources. Defaults to 50m CPU and 128Mi.

        Args:
            container: The Kubernetes container.

        Returns:
            dict: The Control Plane resources.
        """

        # Pick default values
        cpu = "50m"
        memory = "128Mi"

        # Get the resources from the container
        if container.get("resources", {}).get("limits"):
            # Get the CPU limits
            if container["resources"]["limits"].get("cpu"):
                cpu = container["resources"]["limits"]["cpu"]

            # Get the memory limits
            if container["resources"]["limits"].get("memory"):
                memory = container["resources"]["limits"]["memory"]

        # Return the resources
        return {
            "cpu": cpu,
            "memory": memory,
        }

    @staticmethod
    def _build_cpln_workload_volumes(
        kubernetes_volumes, kubernetes_volume_mounts
    ) -> list:
        """
        Process the volume mounts specified for a container.

        Args:
            kubernetes_volumes: The Kubernetes Job volumes.
            kubernetes_volume_mounts: The Kubernetes container volume mounts.

        Returns:
            list: The Control Plane cron workload volumes.
        """

        # Initialize the volumes list
        volumes = []

        # Process each volume mount
        for k8s_volume_mount in kubernetes_volume_mounts:
            # Skip if the volume is not found
            if not kubernetes_volumes.get(k8s_volume_mount["name"]):
                continue

            # Get the volume
            k8s_volume = kubernetes_volumes[k8s_volume_mount["name"]]

            # Process persistent volume claims
            if k8s_volume.get("persistentVolumeClaim"):
                uri = f"cpln://volumeset/{k8s_volume['persistentVolumeClaim']['claimName']}"
                path = k8s_volume_mount["mountPath"]

                # Append volume to the list
                volumes.append({"uri": uri, "path": path})

                # Skip to the next volume
                continue

            # Process config maps
            if k8s_volume.get("configMap"):
                CplnKubernetesConverter._mount_volume(
                    kubernetes_volumes,
                    k8s_volume_mount,
                    k8s_volume["configMap"]["name"],
                    k8s_volume["configMap"].get("items"),
                )

                # Skip to the next volume
                continue

            # Process secrets
            if k8s_volume.get("secret"):
                CplnKubernetesConverter._mount_volume(
                    kubernetes_volumes,
                    k8s_volume_mount,
                    k8s_volume["secret"]["secretName"],
                    k8s_volume["secret"].get("items"),
                )

                # Skip to the next volume
                continue

            # Process projected volumes
            if k8s_volume.get("projected"):
                # Process sources
                for projected_source in k8s_volume["projected"]["sources"]:
                    # Process config maps
                    if projected_source.get("configMap"):
                        CplnKubernetesConverter._mount_volume(
                            kubernetes_volumes,
                            k8s_volume_mount,
                            projected_source["configMap"]["name"],
                            projected_source["configMap"].get("items"),
                        )
                    # Process secrets
                    if projected_source.get("secret"):
                        CplnKubernetesConverter._mount_volume(
                            kubernetes_volumes,
                            k8s_volume_mount,
                            projected_source["secret"]["name"],
                            projected_source["secret"].get("items"),
                        )

                # Skip to the next volume
                continue

            # Mount a shared volume
            if k8s_volume.get("emptyDir"):
                uri = f"scratch://{k8s_volume['name']}"
                path = k8s_volume_mount["mountPath"]

                # Append subpath if it exists
                if k8s_volume_mount.get("subPath"):
                    path = f"{path}/{k8s_volume_mount['subPath']}"

                # Append the volume to the list
                volumes.append({"uri": uri, "path": path})

            # Handle next volume

        # Return the volumes list
        return volumes

    @staticmethod
    def _mount_volume(
        volumes: List[dict],
        k8s_volume_mount: dict,
        secret_name: str,
        volume_items: Optional[List[dict]] = None,
    ) -> None:
        """
        Mount a volume to the container.

        Args:
            volumes: The list of volumes to be updated.
            k8s_volume_mount: The Kubernetes volume mount specification.
            secret_name: The name of the secret to be mounted.
            volume_items: Optional list of volume items.
        """

        # Prepare volume
        uri = f"cpln://secret/{secret_name}"
        path = k8s_volume_mount["mountPath"]

        # Process items and return
        if volume_items:
            # Iterate over each item and add a new volume
            for item in volume_items:
                # If a sub path is specified, then we will only pick and handle one item
                if k8s_volume_mount.get("subPath"):
                    # Find the path that the sub path is referencing
                    if k8s_volume_mount["subPath"] == item["path"]:
                        # Add new volume
                        volumes.append(
                            {
                                "uri": f"{uri}.{item['key']}",
                                "path": f"{path}/{item['path']}",
                            }
                        )

                        # Exit iteration because we have found the item we are looking for
                        break

                    # Skip to next iteration in order to find the specified sub path
                    continue

                # Add a new volume for each item
                volumes.append(
                    {"uri": f"{uri}.{item['key']}", "path": f"{path}/{item['path']}"}
                )

            # Volume items case has been handled, return
            return

        # Reference secret property
        if k8s_volume_mount.get("subPath"):
            uri = f"{uri}.{k8s_volume_mount['subPath']}"

        # Add a new volume
        volumes.append({"uri": uri, "path": path})


class CplnWorkerVariables(BaseVariables):
    """
    Default variables for the Control Plane worker.

    The schema for this class is used to populate the `variables` section of the default
    base job template.
    """

    config: Optional[CplnConfig] = Field(
        default=None,
        description="The Control Plane config to use for job creation.",
    )
    org: Optional[str] = Field(
        default_factory=lambda: os.getenv("CPLN_ORG"),
        description=(
            "The Control Plane organization to create jobs within. "
            "Defaults to the value in the environment variable CPLN_ORG. "
            "If you are hosting the worker on Control Plane, the environment variable will be automatically injected to your workload."
        ),
    )
    namespace: Optional[str] = Field(
        default_factory=lambda: os.getenv("CPLN_GVC"),
        description=(
            "The Control Plane GVC to create jobs within. Defaults to the value in the environment variable CPLN_GVC. "
            "If you are hosting the worker on Control Plane, the environment variable will be automatically injected to your workload."
        ),
    )
    location: Optional[str] = Field(
        default_factory=lambda: os.getenv("CPLN_LOCATION").split("/")[-1],
        description=(
            "The Control Plane GVC location. Defaults to the value in the environment variable CPLN_LOCATION. "
            "If the location is still not found, the first location of the specified GVC will be used. "
            "If you are hosting the worker on Control Plane, the environment variable will be automatically injected to your workload."
        ),
    )
    image: Optional[str] = Field(
        default=None,
        description="The image reference of a container image to use for created jobs. "
        "If not set, the latest Prefect image will be used.",
        examples=["docker.io/prefecthq/prefect:3-latest"],
    )
    service_account_name: Optional[str] = Field(
        default=None, description="The Control Plane identity to use for this job."
    )
    job_watch_timeout_seconds: Optional[int] = Field(
        default=None,
        description=(
            "Number of seconds to wait for the job to complete before marking it as"
            " crashed. Defaults to `None`, which means no timeout will be enforced."
        ),
    )
    pod_watch_timeout_seconds: int = Field(
        default=constants.HTTP_REQUEST_TIMEOUT,
        description="Number of seconds to watch for the workload creation before timing out.",
    )
    stream_output: bool = Field(
        default=True,
        description=(
            "If set, output will be streamed from the job to local standard output."
        ),
    )


class CplnWorkerResult(BaseWorkerResult):
    """Contains information about the final state of a completed process"""


class CplnWorker(BaseWorker):
    """Prefect worker that executes flow runs within Control Plane Cron Jobs."""

    ### Public Properties ###
    type: str = "cpln"
    job_configuration = CplnWorkerJobConfiguration
    job_configuration_variables = CplnWorkerVariables

    ### Private Properties ###
    _description = (
        "Execute flow runs within jobs scheduled on the Control Plane platform. Requires a "
        "token and an organization name, make sure you define them in the `job_configuration`."
    )
    _display_name = "Control Plane"
    _logo_url = "https://console.cpln.io/resources/logos/controlPlaneLogoOnly.svg"
    _documentation_url = "https://docs.controlplane.com"  # noqa

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._created_secrets = {}

    ### Public Methods ###

    async def run(
        self,
        flow_run: "FlowRun",
        configuration: CplnWorkerJobConfiguration,
        task_status: Optional[anyio.abc.TaskStatus] = None,
    ) -> CplnWorkerResult:
        """
        Executes a flow run within a Control Plane Cron Job and waits for the flow run
        to complete.

        Args:
            flow_run: The flow run to execute
            configuration: The configuration to use when executing the flow run.
            task_status: The task status object for the current flow run. If provided,
                the task will be marked as started.

        Returns:
            CplnWorkerResult: A result object containing information about the
                final state of the flow run
        """

        # Get the logger
        logger = self.get_flow_run_logger(flow_run)

        # Get the client from the configuration
        client = self._get_cpln_client(configuration)

        # Log a message to indicate that the job is being created
        logger.info("Creating Control Plane job...")

        # Create the cron workload
        workload = self._create_workload(configuration, client)

        # Extract the job name from the job manifest
        workload_name = workload["name"]

        # Log a message indicating wait duration
        logger.info("Waiting 15 seconds before starting the job")

        # Wait a couple of seconds before starting the job
        await asyncio.sleep(15)

        # Start the job
        job_id = self._start_job(configuration, client, workload)

        # Log the successful start of the job and the job ID
        logger.info(
            f"[CplnWorker] Started job with ID: {job_id} in location {configuration.location}"
        )

        # Get infrastructure pid
        pid = self._get_infrastructure_pid(
            configuration.org, configuration.namespace, workload
        )

        # Indicate that the job has started
        if task_status is not None:
            task_status.started(pid)

        # Monitor the job until completion
        status_code = await self._watch_job(
            logger, workload_name, job_id, configuration, client
        )

        # Delete the cron workload
        await self._delete_workload(pid, configuration)

        # Return worker result with pid and status code
        return CplnWorkerResult(identifier=pid, status_code=status_code)

    async def kill_infrastructure(
        self,
        infrastructure_pid: str,
        configuration: CplnWorkerJobConfiguration,
        grace_seconds: int = constants.DEFAULT_GRACE_SECONDS,
    ):
        """
        Deletes the cron workload for a cancelled flow run based on the provided infrastructure PID
        and run configuration.

        Args:
            infrastructure_pid: The PID of the infrastructure.
            configuration: The configuration to use when executing the flow run.
            grace_seconds: The number of seconds to wait before killing the job.
        """

        await self._delete_workload(infrastructure_pid, configuration, grace_seconds)

    async def teardown(self, *exc_info):
        """
        Performs teardown operations for the current instance, ensuring that any
        created resources are properly cleaned up.

        Args:
            *exc_info: Optional exception information, typically passed during the teardown
                process if an error occurred.
        """

        # Call the parent class teardown method for base cleanup logic
        await super().teardown(*exc_info)

        # Perform cleanup of any secrets created during the lifecycle of this instance
        self._clean_up_created_secrets()

    ### Private Methods ###

    def _get_cpln_client(self, configuration: CplnWorkerJobConfiguration) -> CplnClient:
        """
        Get an authenticated Control Plane API client.

        Args:
            configuration: The configuration to retrieve the API client from.

        Returns:
            CplnClient: A configured Control Plane API client.
        """

        # If the user has configured a CplnConfig, then get the API client
        if configuration.config is not None:
            return configuration.config.get_api_client()

        # Create a new API client using the CPLN_TOKEN environment variable for authentication
        return CplnConfig().get_api_client()

    @retry(
        stop=stop_after_attempt(constants.RETRY_MAX_ATTEMPTS),
        wait=wait_fixed(constants.RETRY_MIN_DELAY_SECONDS)
        + wait_random(
            constants.RETRY_MIN_DELAY_JITTER_SECONDS,
            constants.RETRY_MAX_DELAY_JITTER_SECONDS,
        ),
        reraise=True,
    )
    def _create_workload(
        self, configuration: CplnWorkerJobConfiguration, client: CplnClient
    ) -> constants.CplnObjectManifest:
        """
        Creates a Control Plane job from a Kubernetes job manifest.

        Args:
            configuration (CplnWorkerJobConfiguration): The configuration containing the
                job manifest and associated parameters.
            client (CplnClient): The API client used to communicate with the Control Plane.

        Returns:
            constants.Manifest: The manifest of the created or updated job.

        Raises:
            InfrastructureError: If there is an issue retrieving or creating the job.
            requests.exceptions.HTTPError: If there are HTTP errors that cannot be handled.
        """

        # Check if the Prefect API key should be stored as a secret and replace it in the configuration
        if os.environ.get(
            constants.PREFECT_CPLN_WORKER_STORE_PREFECT_API_IN_SECRET, ""
        ).strip().lower() in ("true", "1"):
            self._replace_api_key_with_secret(
                configuration=configuration, client=client
            )

        # Convert the Kubernetes manifest to a Control Plane workload manifest
        workload_manifest = CplnKubernetesConverter(configuration).convert()

        # Extract the name of the workload from the manifest
        name = workload_manifest["name"]

        try:
            # Attempt to retrieve the workload by its name if it already exists
            client.get(
                f"/org/{configuration.org}/gvc/{configuration.namespace}/workload/{name}",
                True,
            )

            # If no exception has been thrown, then update the workload with the new manifest
            client.put(
                f"/org/{configuration.org}/gvc/{configuration.namespace}/workload/",
                workload_manifest,
            )

            # Return the updated workload
            return client.get(
                f"/org/{configuration.org}/gvc/{configuration.namespace}/workload/{name}"
            )

        except requests.exceptions.HTTPError as e:
            # If the workload doesn't exist, raise an error if the status code is not 404
            if e.response.status_code != 404:
                raise InfrastructureError(
                    f"Failed to retrieve workload '{name}': {str(e.response.text)}"
                ) from e

        # If workload doesn't exist, create a new one
        client.post(
            f"/org/{configuration.org}/gvc/{configuration.namespace}/workload",
            workload_manifest,
            timeout=configuration.pod_watch_timeout_seconds,
        )

        # Retrieve and return the newly created workload for confirmation
        return client.get(
            f"/org/{configuration.org}/gvc/{configuration.namespace}/workload/{name}"
        )

    def _start_job(
        self,
        configuration: CplnWorkerJobConfiguration,
        client: CplnClient,
        manifest: constants.CplnObjectManifest,
    ):
        """
        Starts a new job in the specified location and workload.

        Args:
            configuration (CplnWorkerJobConfiguration): The configuration containing the
                job manifest and associated parameters.
            client (CplnClient): The API client used to communicate with the Control Plane.
            manifest: The Control Plane cron workload manifest.

        Returns:
            The job ID of the newly started job.
        """

        # Extract the name of the workload from the manifest
        name = manifest["name"]

        # Construct the path to start a new job in the specified location and workload
        path = f"/org/{configuration.org}/gvc/{configuration.namespace}/workload/{name}/-command"

        # Construct the command to start a new job
        command = {
            "type": "runCronWorkload",
            "spec": {"location": configuration.location, "containerOverrides": []},
        }

        # Make a POST request to start a new job in the specified location and workload
        response = client.post(path, command)

        # If the response status code is not 201, raise an exception with the response text
        if response.status_code != 201:
            raise Exception(f"Failed to start job: {response.text}")

        # Extract the job ID from the response headers
        id = response.headers["location"].split("/")[-1]

        # Set the job ID and exit the function
        return id

    async def _delete_workload(
        self,
        infrastructure_pid: str,
        configuration: CplnWorkerJobConfiguration,
        grace_seconds: int = 30,
    ):
        """
        Removes the given Job from the Control Plane platform.

        Args:
            infrastructure_pid: The PID of the infrastructure.
            configuration: The configuration to use when executing the flow run.
            grace_seconds: The number of seconds to wait before killing the job.

        Raises:
            InfrastructureNotAvailable: If the job is running in a different namespace or organization
                than expected.
            InfrastructureNotFound: If the job cannot be found on the Control Plane platform.
            requests.RequestException: If an error occurs during the deletion request.
        """

        # Retrieve the API client to interact with the Control Plane platform
        client: CplnClient = configuration.config.get_api_client()

        # Parse the infrastructure PID to extract the organization ID, namespace, and job name
        job_org_name, job_namespace, workload_name = self._parse_infrastructure_pid(
            infrastructure_pid
        )

        # Check if the job is running in the expected namespace
        if job_namespace != configuration.namespace:
            raise InfrastructureNotAvailable(
                f"Unable to kill job {workload_name!r}: The job is running in namespace "
                f"{job_namespace!r} but this worker expected jobs to be running in "
                f"namespace {configuration.namespace!r} based on the work pool and "
                "deployment configuration."
            )

        # Check if the job is running in the expected organization
        if job_org_name != configuration.org:
            raise InfrastructureNotAvailable(
                f"Unable to kill job {workload_name!r}: The job is running on another "
                "Control Plane organization than the one specified by the infrastructure PID."
            )

        # Attempt to delete the job's cron workload.
        try:
            # Wait for the grace period before killing the job
            await asyncio.sleep(grace_seconds)

            # Delete the job
            client.delete(
                f"/org/{configuration.org}/gvc/{configuration.namespace}/workload/{workload_name}"
            )
        except requests.RequestException as e:
            # Raise an error if the job was not found
            if e.response.status_code == 404:
                raise InfrastructureNotFound(
                    f"Unable to kill job {workload_name!r}: The job was not found."
                ) from e
            else:
                # Raise the original exception if the error is not due to the job not being found
                raise

    def _replace_api_key_with_secret(
        self, configuration: CplnWorkerJobConfiguration, client: CplnClient
    ) -> constants.CplnObjectManifest:
        """
        Replaces the PREFECT_API_KEY environment variable with a Control Plane secret.

        Args:
            configuration: The configuration to use when executing the flow run.
            client (CplnClient): The API client used to communicate with the Control Plane.

        Returns
            Manifest: The secret manifest.
        """

        # Get the environment variables from the job manifest
        manifest_env = configuration.job_manifest["spec"]["template"]["spec"][
            "containers"
        ][0].get("env")

        # Find the PREFECT_API_KEY environment variable in the manifest env
        manifest_api_key_env = next(
            (
                env_entry
                for env_entry in manifest_env
                if env_entry.get("name") == "PREFECT_API_KEY"
            ),
            {},
        )

        # Extract the PREFECT_API_KEY value from the environment variable
        api_key = manifest_api_key_env.get("value")

        # Skip replacing the PREFECT_API_KEY environment variable if it wasn't found
        if not api_key:
            return

        # Construct the secret name
        secret_name = f"prefect-{slugify_name(self.name)}-api-key"

        # Construct the secret manifest
        secret = self._create_secret(
            name=secret_name,
            value=api_key,
            org=configuration.org,
            client=client,
        )

        # Store configuration so that we can delete the secret when the worker shuts down
        self._created_secrets[secret["name"]] = configuration

        # Construct a new environment variable entry
        new_api_env_entry = {
            "name": "PREFECT_API_KEY",
            "valueFrom": {"secretKeyRef": {"name": secret_name, "key": "value"}},
        }

        # Update the manifest environment variables with the new entry
        manifest_env = [
            entry if entry.get("name") != "PREFECT_API_KEY" else new_api_env_entry
            for entry in manifest_env
        ]

        # Update the job manifest in the specified configuration
        configuration.job_manifest["spec"]["template"]["spec"]["containers"][0][
            "env"
        ] = manifest_env

    def _clean_up_created_secrets(self):
        """
        Deletes any secrets created during the worker's operation.

        Raises:
            requests.RequestException: If a deletion request fails, the exception is logged
                and then re-raised.
        """

        # Iterate through the secrets created during the worker's operation
        for key, configuration in self._created_secrets.items():
            # Retrieve the API client for the current secret's configuration
            client = self._get_cpln_client(configuration)

            try:
                # Attempt to delete the secret
                client.delete(f"/org/{configuration.org}/secret/{key}")
            except requests.RequestException as e:
                # Log a warning if the secret deletion fails
                self._logger.warning(
                    "Failed to delete created secret with the exception below"
                )

                # Re-raise the exception to propagate the error
                raise e

    def _create_secret(
        self, name: str, value: str, org: str, client: CplnClient
    ) -> constants.CplnObjectManifest:
        """
        Creates or updates a secret in the Control Plane platform.

        Args:
            name (str): The name of the secret to be created or updated.
            value (str): The value of the secret to be stored.
            org (str): The organization name.
            client (CplnClient): The API client used to communicate with the Control Plane.

        Returns:
            constants.Manifest: The manifest of the created or updated secret.

        Raises:
            requests.RequestException: If an error occurs while retrieving, creating,
                or updating the secret, except for 404 errors, which are handled.
        """

        # Construct the API endpoint links for the secret and its parent
        secret_parent_link = f"/org/{org}/secret"
        secret_reveal_link = f"/org/{org}/secret/{name}/-reveal"

        # Encode the secret value in Base64 format
        encoded_value = base64.b64encode(value.encode("utf-8")).decode("utf-8")

        try:
            # Attempt to retrieve the workload by its name if it already exists
            current_secret = client.get(secret_reveal_link)

            # Update the secret data
            current_secret["data"] = {"encoding": "plain", "payload": encoded_value}

            # Perform secret update
            client.put(secret_parent_link, body=current_secret)

            # Get the updated secret
            secret = client.get(secret_reveal_link)

        except requests.RequestException as e:
            # Raise the exception if it's not a 404 error
            if e.response.status_code != 404:
                raise

            # If the secret doesn't exist, create a new one with the specified data
            secret = {
                "kind": "Secret",
                "name": name,
                "type": "opaque",
                "data": {
                    "encoding": "plain",
                    "payload": encoded_value,
                },
            }

            # Create the secret
            client.post(secret_parent_link, body=secret)

            # Retrieve the newly created secret
            secret = client.get(secret_reveal_link)

        # Return the created or updated secret's manifest
        return secret

    def _get_infrastructure_pid(
        self, org: str, gvc: str, workload: constants.CplnObjectManifest
    ) -> str:
        """
        Generates a Control Plane infrastructure PID.
        The PID is in the format: "<org uid>:<namespace>:<job name>".

        Args:
            org (str): The name of the organization.
            gvc (str): The name of the GVC that the job belongs to.
            workload (constants.Manifest): The Control Plane workload manifest.
            client (CplnClient): The API client used to communicate with the Control Plane.

        Returns:
            str: The PID of the infrastructure.
        """

        # Extract the job name
        workload_name = workload["name"]

        # Construct the infrastructure PID and return it
        return f"{org}:{gvc}:{workload_name}"

    def _parse_infrastructure_pid(
        self, infrastructure_pid: str
    ) -> Tuple[str, str, str]:
        """
        Parse a Control Plane infrastructure PID into its component parts.

        Args:
            infrastructure_pid (str): The infrastructure PID to parse.

        Returns:
            str: An organization ID, namespace, and job name.
        """

        # Split the infrastructure PID into its component parts
        org_name, namespace, workload_name = infrastructure_pid.split(":", 2)

        # Return the organization ID, namespace, and job name
        return org_name, namespace, workload_name

    async def _watch_job(
        self,
        logger: logging.Logger,
        workload_name: str,
        job_id: str,
        configuration: CplnWorkerJobConfiguration,
        client: CplnClient,
    ) -> int:
        """
        Watches and monitors a job on the Control Plane platform, logging its status
        and handling its lifecycle stages.

        Args:
            logger (logging.Logger): Logger instance for recording job status updates.
            workload_name (str): The name of the job being monitored.
            job_id (str): The unique ID of the job being monitored.
            configuration (CplnWorkerJobConfiguration): The configuration object containing
                details about the job's namespace and organization.
            client (CplnClient): The API client for interacting with the Control Plane.

        Returns:
            int: A status code representing the final state of the job:
                - 0: Job completed successfully.
                - 1: Job failed.
                - 2: Job was cancelled.
                - 3: Job is still running but logs can no longer be streamed.
        """

        # Log a message to indicate that the job is being monitored
        logger.info(f"Job {workload_name!r}: Monitoring job...")

        # Initialize the logs monitor to stream logs for the specified job
        logs_monitor = CplnLogsMonitor(
            logger,
            client,
            configuration.org,
            configuration.namespace,
            configuration.location,
            workload_name,
            job_id,
        )

        try:
            # Wait for the job to complete and handle logs
            await asyncio.wait_for(
                logs_monitor.monitor(lambda message: print(message)),
                timeout=configuration.job_watch_timeout_seconds,
            )
        except asyncio.TimeoutError:
            logger.error(
                f"Job {workload_name!r}: Job did not complete within "
                f"timeout of {configuration.job_watch_timeout_seconds}s."
            )
            return -1

        # Make a GET request to fetch the job status
        data = client.get(
            f"/org/{configuration.org}/gvc/{configuration.namespace}/workload/{workload_name}/-command/{job_id}"
        )

        # Extract the lifecycle stage from the response data
        lifecycle_stage = data.get("lifecycleStage")

        # Determine the status code if not successful
        if lifecycle_stage == "failed":
            logger.error(f"Job {workload_name!r}: Job has failed.")
            return 1

        if lifecycle_stage == "cancelled":
            logger.error(f"Job {workload_name!r}: Job has been cancelled.")
            return 2

        if lifecycle_stage == "pending" or lifecycle_stage == "running":
            # Job is still running
            logger.error(
                "An error occurred while waiting for the job to compelte - exiting...",
                exc_info=True,
            )

            # Return 3 to indicate that the job is still running
            return 3

        # Log a message to indicate that the job has completed successfully
        logger.info(f"Job {workload_name!r}: Job has completed successfully.")

        # Return 0 to indicate that the job has completed successfully
        return 0
