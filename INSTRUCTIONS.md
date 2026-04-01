# Using Control Plane with Prefect

## Overview

This guide walks you through integrating Prefect with Control Plane by setting up a Prefect server and workers within your Control Plane environment. You will clone a customized Prefect repository, build and push a Docker image, and deploy workloads for both the server and workers.

## Prerequisites

Ensure you have the following installed and configured:

- [Git](https://git-scm.com/)
- [Docker](https://www.docker.com)
- [Prefect v2](https://docs-2.prefect.io/latest/getting-started/installation/) CLI
- A [Control Plane](https://console.cpln.io) account with superuser privileges
- [Control Plane CLI](https://docs.controlplane.com/reference/cli) installed and [authenticated](https://docs.controlplane.com/guides/manage-profile) with your Control Plane account

## Build and Push the Prefect Docker Image

### 1. Clone the Repository

Clone the Prefect repository customized for Control Plane:

```bash
git clone https://github.com/controlplane-com/prefect.git
cd prefect
```

### 2. Checkout the Required Tag

Switch to the `cpln-2.20.25-20260401` tag:

```bash
git checkout tags/cpln-2.20.25-20260401
```

### 3. Build and Push the Image

Use `cpln` CLI to build and push the Prefect image to your private registry:

```bash
cpln image build --name prefect:cpln-2.20.25-20260401 --push
```

## Create a Service Account Key

Before creating the Control Plane Work Pool, you must generate a service account key with superuser privileges. This key will be required for authentication in the `cpln` configuration.

To generate the key:

1. Navigate to the [Control Plane Console](https://console.cpln.io).
2. Create a service account and assign it to the superusers group.
3. Navigate to keys and generate a key for the service account.
4. Store the key securely, as it will be used in the `Control Plane Configuration` block when creating the Work Pool.

## Deploy the Prefect Server

To host the Prefect server on Control Plane, create a workload, along with a GVC, identity, volume set, and necessary policies. In the [Control Plane Console](https://console.cpln.io), click on the apply button on the top right corner, and paste the following manifest:

```yaml
kind: gvc
name: prefect
spec:
  loadBalancer:
    dedicated: false
    trustedProxies: 0
  staticPlacement:
    locationLinks:
      - //location/aws-eu-central-1 # Modify as needed
---
kind: policy
name: prefect-worker-readlogs-prefect
description: prefect-worker-readlogs-prefect
tags: {}
target: all
targetKind: org
bindings:
  - permissions:
      - readLogs
    principalLinks:
      - //gvc/prefect/identity/prefect
---
kind: policy
name: prefect-worker-manage-prefect
tags: {}
target: all
targetKind: workload
bindings:
  - permissions:
      - connect
      - create
      - delete
      - edit
      - exec
      - exec.runCronWorkload
      - exec.stopReplica
      - manage
      - view
    principalLinks:
      - //gvc/prefect/identity/prefect
---
kind: volumeset
name: prefect
gvc: prefect
spec:
  fileSystemType: ext4
  initialCapacity: 10
  performanceClass: general-purpose-ssd
---
kind: identity
name: prefect
gvc: prefect
---
kind: workload
name: prefect-server
gvc: prefect
spec:
  type: stateful
  identityLink: //gvc/prefect/identity/prefect
  containers:
    - name: prefect
      image: //image/prefect:cpln-2.20.25-20260401 # The image that we pushed in the previous step
      cpu: 500m
      memory: 512Mi
      command: prefect
      args:
        - server
        - start
        - "--host"
        - 0.0.0.0
      ports:
        - number: 4200
          protocol: http
      volumes:
        - uri: cpln://volumeset/prefect
          path: /root/.prefect
  defaultOptions:
    capacityAI: false
    debug: false
    suspend: false
    timeoutSeconds: 5
    autoscaling:
      metric: disabled
      minScale: 2
      maxScale: 2
      maxConcurrency: 0
      scaleToZeroDelay: 300
      target: 100
  firewallConfig:
    external:
      inboundAllowCIDR: []
      inboundBlockedCIDR: []
      outboundAllowCIDR:
        - 0.0.0.0/0
      outboundAllowHostname: []
      outboundAllowPort: []
      outboundBlockedCIDR: []
    internal:
      inboundAllowType: same-org
      inboundAllowWorkload: []
```

## Create a Control Plane Work Pool in Prefect

Once the Prefect server is running, create a Control Plane Work Pool in Prefect.

- Navigate to the Prefect UI and create a new work pool.
- Select Control Plane as the type.
- Name it `cpln` (or another name, please don't forget to update references accordingly down below).
- Leave the Organization and Location fields blank unless you wish to override defaults. Control Plane injects `CPLN_ORG`, `CPLN_GVC`, and `CPLN_LOCATION` environment variables automatically, so you don't have to set Organization, GVC and location manually.
- Use the service account key that was created earlier in the `Control Plane Configuration` for authentication.
- Add the following environment variable to the work pool:

```json
{ "PREFECT_API_URL": "http://prefect-server.prefect.cpln.local/api" }
```

## Deploy a Prefect Worker

In order to run jobs on Control Plane, you will need a cpln worker running on Control Plane that can communicate internally with the Prefect server and trigger jobs.

After creating a Control Plane work pool on your Prefect server, head back to the [Control Plane Console](https://console.cpln.io) and click on the apply button on the top right corner, and paste the following manifest:

```yaml
kind: workload
name: prefect-worker
gvc: prefect
spec:
  type: standard
  identityLink: //gvc/prefect/identity/prefect
  containers:
    - name: prefect
      image: //image/prefect:cpln-2.20.25-20260401 # The image that we pushed in the previous step
      cpu: 500m
      memory: 512Mi
      command: prefect
      args:
        - worker
        - start
        - "--pool"
        - cpln
      env:
        - name: PREFECT_API_URL
          value: http://prefect-server.prefect.cpln.local/api # The internal endpoint of the Prefect server
  defaultOptions:
    capacityAI: false
    debug: false
    suspend: false
    timeoutSeconds: 5
    autoscaling:
      metric: disabled
      maxScale: 1
      minScale: 1
      maxConcurrency: 0
      scaleToZeroDelay: 300
      target: 100
  firewallConfig:
    external:
      inboundAllowCIDR: []
      inboundBlockedCIDR: []
      outboundAllowCIDR:
        - 0.0.0.0/0
      outboundAllowHostname: []
      outboundAllowPort: []
      outboundBlockedCIDR: []
    internal:
      inboundAllowType: none
      inboundAllowWorkload: []
```

## Configure Local Prefect CLI

To deploy jobs from your local machine, you need to configure your Prefect profile to use the API URL of your Prefect server.

Obtain the endpoint of the Prefect server from the workload info page of the `prefect-server` workload. You can find this by navigating to the [Control Plane Console](https://console.cpln.io), selecting your Prefect workload, and copying the Canonical Endpoint (Global) endpoint.

Once you obtain the endpoint, replace it with the `ENDPOINT_HERE` down below and don't forget to keep the `/api` there.

```bash
prefect config set PREFECT_API_URL=ENDPOINT_HERE/api
```

## Use `prefect-cpln` in Your Flow

If you wish to use [prefect-cpln](https://github.com/controlplane-com/prefect-cpln) flows and tasks, and especially `CplnJob`, you can install the depenency using this command:

```bash
pip install --no-cache-dir git+https://github.com/controlplane-com/prefect-cpln.git
```

## Deploy a Flow to Your Work Pool

This is an example deployment that you can deploy on your `cpln` Prefect work pool.

Create a file `create_deployment.py`:

```python
from prefect import flow

if __name__ == "__main__":
    flow.from_source(
        source="https://github.com/prefecthq/demos.git",
        entrypoint="my_workflow.py:show_stars",
    ).deploy(
        name="show-stars-deployment",
        work_pool_name="cpln",
        parameters={
            "github_repos": [
                "PrefectHQ/prefect",
                "pydantic/pydantic",
                "huggingface/transformers",
            ]
        },
    )
```

Create the deployment:

```bash
python create_deployment.py
```

## Run a Namespaced Job

This is an example use of the `CplnJob` after configuring a `CplnJob` block on the Prefect server.

Create a file `create_deployment.py`:

```python
from prefect import flow, get_run_logger
from prefect_cpln.flows import run_namespaced_job  # this is a flow
from prefect_cpln.jobs import CplnJob

cpln_job_block = CplnJob.load("my-cpln-job")

@flow
def cpln_orchestrator():
    # run the flow and send logs to the parent flow run's logger
    logger = get_run_logger()
    run_namespaced_job(cpln_job_block, print_func=logger.info)


if __name__ == "__main__":
    cpln_orchestrator()
```

Execute the job:

```bash
python job.py
```

## Using an Infrastructure Block

If you don't wish to create and use a Control Plane work pool, you can use the `Control Plane Infrastructure` block to define how Prefect flow runs as a job with a Control Plane cron workload. This block functions similarly to the `KubernetesJob` block in Prefect.

### Creating an Agent Work Pool

Before setting up the infrastructure, you need to create an agent work pool and name it `cpln-agent-work-pool` (This name will be used later on. If you wish to name it something else, make sure you change the name below as well). This work pool will allow the Prefect agent to manage flow runs using the `Control Plane Infrastructure`.

### Setting Up the Infrastructure

You can find the `Control Plane Infrastructure` in the Prefect UI under the Blocks page. Alternatively, you can set it up programatically using the code below.

This script will:

- Create a `Control Plane Infrastructure` block.
- Create a `Control Plane Infrastructure Config` block, which is designed to work specifically with the `Control Plane Infrastructure` block (similar to the `Control Plane Configuration` block).

Ensure you have Prefect version `cpln-2.20.25-20260401` installed locally on your machine. To install it run (Ensure the repository is checked out at the `cpln-2.20.25-20260401` tag):

```bash
pip install .
```

Use the following code to create the infrastructure block.

```python
import os
from prefect.infrastructure import CplnInfrastructure, CplnInfrastructureConfig

cpln_infra_config_block = CplnInfrastructureConfig(token=os.getenv("CPLN_TOKEN")) # Your Control Plane token here
kubernetes_manifest = CplnInfrastructure.job_from_file("job_template.yaml") # The path to a Kubernetes job YAML file

cpln_infra_block = CplnInfrastructure(
    config=cpln_infra_config_block,
    job=kubernetes_manifest,
    image="/org/epoch/image/prefect-repo-info-workflow:v1", # The image that contains the flow code in Python
    env={"PREFECT_API_URL": "http://{PREFECT_ENDPOINT_HERE}/api"},
)

cpln_infra_config_block.save("cpln-infra-config")
cpln_infra_block.save("cpln-infra")
```

### Running a Deployment Using the Infrastructure

Once the `Control Plane Infrastructure` block is created, you can use it in a Prefect deployment.

```python
from prefect.deployments import Deployment
from prefect.infrastructure import CplnInfrastructure

# Define CplnJob infrastructure
control_plane_infrastructure_block = CplnInfrastructure.load("cpln-infra")

# Build the deployment
deployment = Deployment(
    name="repo-info-deployment",
    flow_name="repo_info",
    path="/opt/prefect/flows",
    entrypoint="repo_info.py:repo_info",
    infrastructure=control_plane_infrastructure_block,
    work_pool_name="cpln-agent-work-pool",
)

# Apply the deployment
deployment.apply()
```

## Debugging & Troubleshooting

### How Things Are Linked Together

Every flow run execution creates a chain of linked resources across Prefect and Control Plane:

```
Flow Run (Prefect) → Infrastructure PID → CPLN Workload → CPLN Command → Job Execution
```

#### Infrastructure PID

When a flow run is submitted, the agent stores an **infrastructure PID** on the flow run in the format:

```
org:gvc:workload_name:command_id
```

Example: `my-org:my-gvc:courageous-jaguar:9b38a647-cd2b-47e7-987b-8c4f8b114be6`

This PID links the Prefect flow run to the exact CPLN workload and command. You can find it in the Prefect UI under the flow run's details.

#### Workload Tags

Every workload created by Prefect is tagged for identification:

| Tag                     | Value    | Purpose                                         |
| ----------------------- | -------- | ----------------------------------------------- |
| `cpln/createdByPrefect` | `true`   | Identifies Prefect-managed workloads            |
| `cpln/specHash`         | `<hash>` | Groups workloads with identical specs for reuse |
| `cpln/prefectJobType`   | `<type>` | Distinguishes job types (e.g., standard, cron)  |

#### Command Tags

Every command (job execution) is tagged with flow run metadata:

| Tag                      | Value                   | Purpose                                        |
| ------------------------ | ----------------------- | ---------------------------------------------- |
| `prefect.io/flow-run-id` | `<uuid>`                | Links the command back to the Prefect flow run |
| `prefect.io/flow-name`   | `<name>`                | The flow name for human identification         |
| `cpln/prefectAgent`      | `<agent-workload-link>` | Which agent submitted this job                 |

### Log Areas and How to Query Them

All CPLN-related logs use the prefix `[CPLN]` followed by an area identifier. This enables hierarchical filtering with LogQL.

#### Log Format

```
[CPLN] <Area> | Flow: <name>, RunID: <id>, GVC: <gvc>, Workload: <name>, Cmd: <id> > <message>
```

#### Areas

| Area                | LogQL Filter                     | What It Covers                                                               |
| ------------------- | -------------------------------- | ---------------------------------------------------------------------------- |
| All CPLN            | `\|= "[CPLN]"`                   | Everything CPLN-related                                                      |
| Job lifecycle       | `\|= "[CPLN] Job"`               | Job creation, start, monitoring, completion, failure                         |
| Workload management | `\|= "[CPLN] Workload"`          | Workload creation, readiness checks, discovery                               |
| Sync CPLN→Prefect   | `\|= "[CPLN] Sync CPLN→Prefect"` | Detecting CPLN job failures and updating Prefect state                       |
| Sync Prefect→CPLN   | `\|= "[CPLN] Sync Prefect→CPLN"` | Detecting completed Prefect flows with active CPLN jobs, sending stopReplica |
| Kill / Cancel       | `\|= "[CPLN] Kill"`              | Flow run cancellation, stopReplica from kill path                            |
| Cleanup             | `\|= "[CPLN] Cleanup"`           | Orphaned workload deletion (24h TTL)                                         |
| Log streaming       | `\|= "[CPLN] Logs"`              | WebSocket log streaming from CPLN                                            |
| Initialization      | `\|= "[CPLN] Init"`              | Agent CPLN client creation and validation                                    |

#### Cross-Filtering

Combine area filters with entity identifiers to narrow down:

```bash
# All logs for a specific flow run
cpln logs '{gvc="<gvc>", workload="<agent-workload>"}' --org <org> --since 1h \
  |= "[CPLN]" |= "<flow-run-id>" -o raw

# All logs for a specific CPLN workload
cpln logs '{gvc="<gvc>", workload="<agent-workload>"}' --org <org> --since 1h \
  |= "[CPLN]" |= "<workload-name>" -o raw

# All logs for a specific command
cpln logs '{gvc="<gvc>", workload="<agent-workload>"}' --org <org> --since 1h \
  |= "[CPLN]" |= "<command-id>" -o raw
```

### Common Debugging Scenarios

#### Flow run completed on Prefect but CPLN job is still running

This typically happens when sidecar containers don't exit after the main container finishes.

1. Find the flow run ID in the Prefect UI
2. Query the agent logs for the Prefect→CPLN sync:
   ```bash
   cpln logs '{gvc="<gvc>", workload="<agent-workload>"}' --org <org> --since 1h \
     |= "[CPLN] Sync Prefect→CPLN" |= "<flow-run-id>" -o raw
   ```
3. Look for `"Sending stopReplica"` — if present, the sync detected the mismatch
4. Look for `"terminated successfully"` — if present, the job was stopped
5. If no sync logs appear, check init logs for client creation errors: `|= "[CPLN] Init"`

#### Flow run stuck in RUNNING on Prefect but CPLN job no longer exists

1. Query the agent logs for the CPLN→Prefect sync:
   ```bash
   cpln logs '{gvc="<gvc>", workload="<agent-workload>"}' --org <org> --since 1h \
     |= "[CPLN] Sync CPLN→Prefect" |= "<flow-run-id>" -o raw
   ```
2. Look for `"Got 404"` — the CPLN command no longer exists
3. After 5 consecutive 404s (~5 minutes at 60s intervals), the agent automatically marks the flow run as CRASHED
4. If not resolving: check the infrastructure PID on the flow run — it may be malformed or reference a different org

#### Job failed to start

1. Query the job lifecycle logs:
   ```bash
   cpln logs '{gvc="<gvc>", workload="<agent-workload>"}' --org <org> --since 1h \
     |= "[CPLN] Job" |= "<flow-run-id>" -o raw
   ```
2. Look for `"Creating Control Plane job..."` followed by error messages
3. Check workload readiness:
   ```bash
   cpln logs '{gvc="<gvc>", workload="<agent-workload>"}' --org <org> --since 1h \
     |= "[CPLN] Workload" |= "<flow-run-id>" -o raw
   ```

#### Agent not syncing or not detecting stale jobs

1. Verify the sync loops are running (these log at debug level when idle):
   ```bash
   cpln logs '{gvc="<gvc>", workload="<agent-workload>"}' --org <org> --since 5m \
     |= "[CPLN] Sync" -o raw
   ```
2. Check for initialization errors:
   ```bash
   cpln logs '{gvc="<gvc>", workload="<agent-workload>"}' --org <org> --since 5m \
     |= "[CPLN] Init" -o raw
   ```

### Understanding the Bidirectional Sync

The agent runs two sync loops every 60 seconds (configurable via `PREFECT_AGENT_CPLN_MONITOR_INTERVAL`):

**`sync_cpln_to_prefect`** (CPLN is source of truth → updates Prefect)

- Queries Prefect for RUNNING flow runs with infrastructure PIDs
- For each, checks the CPLN command's lifecycle stage
- If CPLN says `completed` / `failed` / `cancelled` → updates the Prefect flow run state to match
- If the command returns 404 for 5 consecutive cycles → marks the flow run as CRASHED

**`sync_prefect_to_cpln`** (Prefect is source of truth → stops CPLN jobs)

- Queries CPLN for running `runCronWorkload` commands scoped to this agent (via `cpln/prefectAgent` tag)
- For each, reads the `prefect.io/flow-run-id` tag and checks Prefect for the flow run state
- If the flow run is terminal (COMPLETED / FAILED / CRASHED / CANCELLED) but the CPLN command is still running → sends `stopReplica`
- Tracks termination progress and logs duration until the stop completes
