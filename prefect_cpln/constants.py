from typing import Any, Dict, List

# General
DEFAULT_GRACE_SECONDS: int = 30

# Jobs Related
COMMAND_STATUS_CHECK_DELAY: int = 5
JOB_WATCH_OFFSET_MINUTES: int = 5
LIFECYCLE_FINAL_STAGES: List[str] = ["completed", "failed", "cancelled"]

# Events Related
EVENT_CHECK_DELAY_SECONDS: int = 5

# Retry Related
RETRY_MAX_ATTEMPTS = 3
RETRY_MIN_DELAY_SECONDS = 1
RETRY_MIN_DELAY_JITTER_SECONDS = 0
RETRY_MAX_DELAY_JITTER_SECONDS = 3
RETRY_WORKLOAD_READY_CHECK_SECONDS = 2

# API Request Configuration
HTTP_REQUEST_TIMEOUT: int = 60
HTTP_REQUEST_RETRY_BASE_DELAY: int = 1
HTTP_REQUEST_MAX_RETRIES: int = 5
WEB_SOCKET_PING_INTERVAL_MS = 30 * 1000

# API Endpoints
DEFAULT_DATA_SERVICE_URL: str = "https://api.cpln.io"
LOGS_URL: str = "wss://logs.cpln.io"

# Object Structure Definitions
CplnObjectManifest = Dict[str, Any]
KubernetesObjectManifest = Dict[str, Any]

# Environment Variables
PREFECT_CPLN_WORKER_STORE_PREFECT_API_IN_SECRET: str = (
    "PREFECT_CPLN_WORKER_STORE_PREFECT_API_IN_SECRET"
)
