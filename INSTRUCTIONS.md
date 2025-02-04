# Using Control Plane with Prefect

## Overview

This guide walks you through integrating Prefect with Control Plane by setting up a Prefect server and workers within your Control Plane environment. You will clone a customized Prefect repository, build and push a Docker image, and deploy workloads for both the server and workers.

## Prerequisites

Ensure you have the following installed and configured:

- [Git](https://git-scm.com/)
- [Docker](https://www.docker.com)
- [Prefect v2](https://docs-2.prefect.io/latest/getting-started/installation/) CLI
- A [Control Plane](https://console.cpln.io) account with superuser privileges
- [Control Plane CLI](https://docs.controlplane.com/reference/cli) installed and [authenticated](https://docs.controlplane.com/guides/manage-profile) with your Control Plane account.

## Build and Push the Prefect Docker Image

### 1. Clone the Repository

Clone the Prefect repository customized for Control Plane:

```bash
git clone https://github.com/controlplane-com/prefect.git
cd prefect
```

### 2. Checkout the Required Tag

Switch to the `cpln-2.20.16` tag:

```bash
git checkout tags/cpln-2.20.16
```

### 3. Build and Push the Image

Use `cpln` CLI to build and push the Prefect image to your private registry:

```bash
cpln image build --name prefect:cpln-2.20.16 --push
```

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
      image: //image/prefect:cpln-2.20.16 # The image that we pushed in the previous step
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
- Name it `cpln` (or another name, updating references accordingly).
- Leave the Organization and Location fields blank unless you wish to override defaults. Control Plane injects `CPLN_ORG`, `CPLN_GVC`, and `CPLN_LOCATION` automatically, so you don't have to see Organization, GVC and location.
- You don't have to create a `CplnConfig`, the worker that we will configure below is going to have the sufficient policies to create workloads and read their logs. If you still wish to create `CplnConfig`, you will need to obtain a [service account](https://docs.controlplane.com/guides/create-service-account) key and use it in the Token field.

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
      image: //image/prefect:cpln-2.20.16 # The image that we pushed in the previous step
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
