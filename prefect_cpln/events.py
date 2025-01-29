import asyncio
from logging import Logger
from typing import Dict, List, Optional
from prefect.events import Event, RelatedResource, emit_event
from prefect_cpln import constants
from prefect_cpln.common import watch_job_until_completion
from prefect_cpln.credentials import CplnClient
from prefect_cpln.exceptions import CplnJobTimeoutError


class CplnEventsReplicator:
    """Replicates Control Plane workload events to Prefect events."""

    def __init__(
        self,
        logger: Logger,
        client: CplnClient,
        org: str,
        gvc: str,
        workload_name: str,
        command_id: str,
        worker_resource: Dict[str, str],
        related_resources: List[RelatedResource],
        timeout_seconds: int,
    ):
        # Received attributes
        self._logger = logger
        self._client = client
        self._org = org
        self._gvc = gvc
        self._workload_name = workload_name
        self._command_id = command_id
        self._timeout_seconds = timeout_seconds

        # Internally defined attirbutes
        self._state = "READY"
        self._task = None

        # All events emitted by this replicator have the pod itself as the
        # resource. The `worker_resource` is what the worker uses when it's
        # the primary resource, so here it's turned into a related resource
        # instead.
        worker_resource["prefect.resource.role"] = "worker"
        worker_related_resource = RelatedResource(__root__=worker_resource)
        self._related_resources = related_resources + [worker_related_resource]

    ### Special Methods ###

    async def __aenter__(self):
        """Start the Control Plane event watcher when entering the context."""
        self._task = asyncio.create_task(self._replicate_workload_events())
        self._state = "STARTED"
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Stop the Control Plane event watcher and ensure all tasks are completed before exiting the context."""
        self._state = "STOPPED"
        if self._task:
            self._task.cancel()

    ### Private Methods ###

    def _workload_as_resource(
        self, workload: constants.CplnObjectManifest
    ) -> Dict[str, str]:
        """
        Convert a workload to a resource dictionary.

        Args:
            workload (constants.CplnObjectManifest): The workload specification.

        Retruns
            dict: The workload as Prefect resource.
        """

        # Extract the workload id
        workload_id = workload["id"]

        # Construct and return the resource
        return {
            "prefect.resource.id": f"prefect.cpln.workload.{workload_id}",
            "prefect.resource.name": workload["name"],
            "cpln.gvc": self._gvc,
        }

    async def _replicate_workload_events(self):
        """Replicate Control Plane workload events as Prefect Events."""

        # Define tasks
        monitor_events_task = asyncio.create_task(self._monitor_job_events())
        monitor_status_task = asyncio.create_task(
            watch_job_until_completion(
                self._client,
                self._org,
                self._gvc,
                self._workload_name,
                self._command_id,
            )
        )

        # Store tasks into a list
        tasks = [monitor_events_task, monitor_status_task]

        try:
            # Execute tasks asynchronously until the status task exits first,
            # which will result in terminating the events monitoring task
            _, pending = await asyncio.wait(
                tasks,
                timeout=self._timeout_seconds,
                return_when=asyncio.FIRST_COMPLETED,
            )
        except asyncio.TimeoutError:
            # Raise an error if the job did not complete within the timeout
            raise CplnJobTimeoutError(
                f"Job {self._workload_name}: Job did not complete within "
                f"timeout of {self._timeout_seconds}s."
            )

        # Cancel remaining tasks
        for task in pending:
            task.cancel()

    async def _monitor_job_events(self, workload: constants.CplnObjectManifest):
        """
        Monitor Control Plane job events.

        Args:
            workload (constants.CplnObjectManifest): The workload manifest object for the job
                being monitored.
        """

        # Maintain a set of seen event IDs to avoid processing duplicate events
        seen_events = set()

        # Tracks the last emitted event for sequential event handling
        last_event = None

        # Construct the workload link
        workload_link = (
            f"/org/{self._org}/gvc/{self._gvc}/workload/{self._workload_name}"
        )

        # Fetch the workload
        workload = self._client.get(workload_link)

        # Keep watching until completed
        while True:

            # Get the eventlog of the workload
            eventlog = self._client.get(f"{workload_link}/-eventlog")

            # Iterate over event logs and emit them
            for event in eventlog["items"]:
                # Attempt to extract the id of the event
                event_id = event.get("id")

                # Process unseen events only
                if event_id and event_id not in seen_events:
                    # Mark the event as seen
                    seen_events.add(event_id)

                    # Emit the event
                    last_event = await self._emit_workload_event(
                        workload, event, last_event=last_event
                    )

            # Pause before fetching events again
            await asyncio.sleep(constants.EVENT_CHECK_DELAY_SECONDS)

    async def _emit_workload_event(
        self,
        workload: constants.CplnObjectManifest,
        workload_event: constants.CplnObjectManifest,
        last_event: Optional[Event] = None,
    ) -> Event:
        """
        Emit a Prefect event for a Control Plane workload event.

        Args:
            workload (constants.CplnObjectManifest): The workload that the event belongs to.
            workload_event (constants.CplnObjectManifest): The workload event dict.
            last_event (Event): The previous Prefect event that was emitted.

        Returns:
            Event: The emitted Prefect event.
        """

        # Get the workload as a resource
        resource = self._workload_as_resource(workload)

        # Extract the status of the event
        status = workload_event["status"]

        # If the event errored, add the reason to the resource
        if status == "errored":
            resource["cpln.reason"] = workload_event.get("context", {}).get(
                "message", "Unknown"
            )

        # Emit and return the event
        return emit_event(
            event=f"prefect.cpln.pod.{status}",
            resource=resource,
            related=self._related_resources,
            follows=last_event,
        )
