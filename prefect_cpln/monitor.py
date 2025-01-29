import logging
import os
import asyncio
from prefect_cpln.utilities import CplnLogsMonitor
from prefect_cpln.credentials import CplnClient

# Ensure logging is configured before any logging call
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],  # Ensure logs go to the console
)

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    client = CplnClient()
    org = os.getenv("CPLN_ORG")
    gvc = os.getenv("CPLN_GVC")
    location = "aws-eu-central-1"
    job_name = "simulate-running-app-job"
    job_id = "14a06f57-eeaa-4955-b03d-cf2203f0fcd7"

    logs_monitor = CplnLogsMonitor(
        logger,
        client,
        org,
        gvc,
        location,
        job_name,
        job_id,
    )

    logger.info("Monitoring started...")
    asyncio.run(logs_monitor.monitor(lambda message: print(message)))
    logger.info("Monitoring finished.")
