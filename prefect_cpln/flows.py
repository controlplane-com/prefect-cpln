"""A module to define flows interacting with Control Plane jobs."""

from typing import Callable, Optional
from prefect import flow, task
from prefect_cpln.jobs import CplnJob, CplnJobRun


@flow
async def run_namespaced_job(
    cpln_job: CplnJob, print_func: Optional[Callable] = None
) -> str:
    """
    Flow for running a GVC-scoped Control Plane job.

    Args:
        cpln_job (CplnJob): The `CplnJob` block that specifies the job to run.
        print_func (Optional[Callable]): A function to print the logs from the job.

    Returns:
        str: A string containing the logs from the job.

    Raises:
        RuntimeError: If the created Control Plane job attains a failed status.

    Example:

        ```python
        from prefect_cpln import CplnJob, run_namespaced_job
        from prefect_cpln.credentials import CplnCredentials

        run_namespaced_job(
            cpln_job=CplnJob.from_yaml_file(
                credentials=CplnCredentials.load("cpln-creds"),
                manifest_path="path/to/job.yaml",
            )
        )
        ```
    """

    # Trigger the Control Plane job using the provided `CplnJob` block
    cpln_job_run: CplnJobRun = task(cpln_job.trigger)()

    # Wait for the job to complete while optionally printing logs
    await task(cpln_job_run.wait_for_completion.aio)(cpln_job_run, print_func)

    # Fetch and return the results of the completed job
    return task(cpln_job_run.fetch_result)()
