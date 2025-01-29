import asyncio
from typing import Optional
from prefect_cpln import constants
from prefect_cpln.credentials import CplnClient


async def watch_job_until_completion(
    client: CplnClient,
    org: str,
    gvc: str,
    workload_name: str,
    command_id: str,
    interval_seconds: Optional[int] = None,
) -> str:
    """
    Recusively fetch the command and check the lifecycle value for completion.

    Args:
        client (CplnClient): The Control Plane API client.
        org (str): The organization name.
        gvc (str): The GVC name where the workload lives.
        workload_name (str): The workload name where the command is running.
        command_id (str): The command ID.

    Returns:
        str: The job status upon completion.
    """

    lifecycle_stage = None
    delay_seconds = (
        interval_seconds
        if interval_seconds is not None
        else constants.COMMAND_STATUS_CHECK_DELAY
    )

    # Continue monitoring the job status until completion
    while True:
        # Construct the command link
        command_link = (
            f"/org/{org}/gvc/{gvc}/workload/{workload_name}/-command/{command_id}"
        )

        # Make a GET request to fetch the job status
        command = client.get(command_link)

        # Extract the lifecycle stage from the response data and return it
        lifecycle_stage = command["lifecycleStage"]

        # If the job has completed, failed, or cancelled, set the flag to disconnect
        if lifecycle_stage in constants.LIFECYCLE_FINAL_STAGES:
            # Set the completed flag to True to indicate completion
            break

        # Pause for a delay before checking the job status again
        await asyncio.sleep(delay_seconds)

    # Return the final lifecycle stage of the job
    return lifecycle_stage
