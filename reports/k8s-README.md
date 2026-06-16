# TalkGo GKE Worker

`worker-staging.yaml` deploys TalkGo as a Disha-compatible staging GKE worker.
`worker.yaml` deploys TalkGo as a Disha-compatible prod GKE worker.

Required deploy config:

```text
.staging.env: GKE_DEPLOYMENT_NAME=disha-go-voice-worker-staging
.prod.env:    GKE_DEPLOYMENT_NAME=disha-go-voice-worker-prod
```

The TalkGo deployment runs in the existing voice-worker clusters. The deploy
scripts read their env files themselves; do not export these in the shell. The
deployment name is separate from the Artifact Registry repository name, so use:

```text
staging: GKE_DEPLOYMENT_NAME=disha-go-voice-worker-staging, ARTIFACT_REPOSITORY_NAME=disha-voice-worker-staging
prod:    GKE_DEPLOYMENT_NAME=disha-go-voice-worker-prod, ARTIFACT_REPOSITORY_NAME=disha-voice-worker-prod
```

Required Kubernetes secret for TalkGo runtime env:

```bash
kubectl create secret generic talk-go-worker-env \
  --namespace "$GKE_NAMESPACE" \
  --dry-run=client -o yaml \
  --from-env-file=<.staging.env or .prod.env> \
  | kubectl apply -f -
```

Keep runtime env in the environment-specific file, including `FAST_API_PORT=7860`, Pyroscope
credentials, and `PERF_DIAGNOSTICS_ENABLED`. The manifest injects
`GKE_DEPLOYMENT_NAME` directly so the worker registration name matches the
Kubernetes deployment name. The S3 env values should match the Python worker
because TalkGo uploads the same `debug_log_data/{conversation_id}/log_data.json`
object.

The manifests preserve Disha backend worker scheduling, affinity, KEDA, and PDB
shape. Prod `worker.yaml` uses the Disha prod worker resource posture; staging
`worker-staging.yaml` keeps the current lower staging sizing. Both manifests
mirror Disha backend `k8s/worker.yaml`'s scaling-buffer-schedule Postgres scaler
query, with one TalkGo change: halve the scheduled buffer before adding the
deployment's active/reserved/provisioning worker count. The deploy script reads
`DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`, and `DB_NAME` from the env file
so it can create the KEDA Postgres connection Secret.

Build, push, and deploy staging:

```bash
./deploy-staging.sh
```

Build, push, and deploy prod:

```bash
./deploy-prod.sh
```

The staging script defaults to:

```text
GCP_PROJECT_ID=curelinkai
GKE_NAMESPACE=staging
GKE_DEPLOYMENT_NAME=disha-go-voice-worker-staging
ARTIFACT_REPOSITORY_NAME=disha-voice-worker-staging
GKE_CLUSTER_NAME=disha-voice-worker-staging
GKE_CLUSTER_LOCATION=us-east1
```

The prod script defaults to:

```text
GCP_PROJECT_ID=curelinkai
GKE_NAMESPACE=prod
GKE_DEPLOYMENT_NAME=disha-go-voice-worker-prod
ARTIFACT_REPOSITORY_NAME=disha-voice-worker-prod
GKE_CLUSTER_NAME=disha-voice-worker-prod
GKE_CLUSTER_LOCATION=us-east4
```

`deploy-prod.sh` also refuses to continue when obvious staging values remain in
`.prod.env`, including staging-looking API, Redis, bucket, repository, cluster,
or namespace values. Update `.prod.env` with the real prod Disha endpoints and
secrets before running it.

Each script builds and pushes `latest`, refreshes `talk-go-worker-env` from its
local env file, applies the matching manifest with a fresh
`POD_TEMPLATE_VERSION`, and waits for rollout. The template-version label is
what forces Kubernetes to replace pods and pull the newly pushed `latest` image.

Set `PERF_DIAGNOSTICS_ENABLED=1` in the env file for a profiled run. That
single flag enables Go/Python Pyroscope startup, `process_usage`, and
`audio_timing`. Leave it at `0` for normal runs so the 20ms audio hot path skips
timing instrumentation.
