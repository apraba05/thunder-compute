# Virtual GPU Multiplexer (bin-packing scheduler over a shared pool)

Systems-level scheduling/bin-packing logic, stateful coordination via Redis, and GitOps-driven deployment of infra services on Kubernetes.

**Live demo:** https://thunder-compute.ashanpraba.com

The demo runs entirely in the browser against seeded data — no API keys,
no accounts, and no external services required.

## Stack

- Go
- Redis
- Kubernetes (kind)
- Helm
- ArgoCD
- Python

## How it works

- Spin up a local kind cluster; deploy Redis via a minimal Helm chart to hold allocation state.
- Write a Go service (vgpu-scheduler) with a REST endpoint that accepts {compute, mem} requests and bin-packs them across a configurable number of simulated physical GPU 'slots', storing allocations in Redis.
- A naive-mode toggle that does 1:1 job-to-GPU assignment, so the demo can show before/after utilization.
- Write a small Python script that fires 10-15 concurrent simulated workloads with randomized resource asks against the scheduler API.
- A /metrics endpoint that prints per-slot utilization %, and commit the Helm chart to a git repo synced via ArgoCD to show GitOps deployment.
- Record a single take: apply the ArgoCD app, run the job submitter, and show utilization jump from the naive baseline to the multiplexed result in the terminal.

## Running locally

```bash
cd src
bash run.sh
```

Then open the printed URL. A prebuilt static version of the UI lives in
`src/web/` and can be opened directly with no server.
