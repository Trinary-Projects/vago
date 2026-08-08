# TalkGo GKE Worker

`worker-staging.yaml` deploys TalkGo as a Disha-compatible staging GKE worker.
`worker.yaml` deploys TalkGo as a Disha-compatible prod GKE worker.

Deployment and runtime environment variables live in AWS Systems Manager
Parameter Store in `ap-south-1`:

```text
/vago/prod/KEY:    base value used by prod and staging
/vago/staging/KEY: staging-only override for the same KEY
```

The TalkGo deployment runs in the existing voice-worker clusters. The deploy
scripts fetch and decrypt the Parameter Store values, merge staging over prod,
and write the result to the git-ignored `.temp-deploy.env` with mode `0600`.
The file is intentionally retained after deployment for inspection and is
replaced by the next deploy. It contains plaintext secrets and must not be
shared. Local `.env`, `.staging.env`, and `.prod.env` files are not read or
modified during deployment.

The deployment name is separate from the Artifact Registry repository name, so
the effective Parameter Store values must be:

```text
staging: GKE_DEPLOYMENT_NAME=disha-go-voice-worker-staging, ARTIFACT_REPOSITORY_NAME=disha-voice-worker-staging
prod:    GKE_DEPLOYMENT_NAME=disha-go-voice-worker-prod, ARTIFACT_REPOSITORY_NAME=disha-voice-worker-prod
```

Keep shared runtime env under `/vago/prod/`, including `FAST_API_PORT=7860`,
Pyroscope credentials, and `PERF_DIAGNOSTICS_ENABLED`. Add a key under
`/vago/staging/` only when staging needs a different value. Parameters are
stored as `SecureString` values and the deployer's AWS identity needs
`ssm:GetParametersByPath` plus permission to decrypt them. Set
`VAGO_SSM_REGION` or `VAGO_SSM_PREFIX` in the deployer's shell only when an
alternate region or path is intentionally required.

The manifest injects
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
`DB_USER`, `DB_PASSWORD`, `DB_HOST`, `DB_PORT`, and `DB_NAME` from the temporary
merged env file so it can create the KEDA Postgres connection Secret.

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
the resolved prod Parameter Store environment, including staging-looking API,
Redis, bucket, repository, cluster, or namespace values.

Each script builds and pushes `latest`, refreshes `talk-go-worker-env` from its
temporary Parameter Store env file, applies the matching manifest with a fresh
`POD_TEMPLATE_VERSION`, and waits for rollout. The template-version label is
what forces Kubernetes to replace pods and pull the newly pushed `latest` image.

Set `/vago/prod/PERF_DIAGNOSTICS_ENABLED` to `1` for a profiled run everywhere,
or create `/vago/staging/PERF_DIAGNOSTICS_ENABLED` for a staging-only override.
That single flag enables Go/Python Pyroscope startup, `process_usage`, and
`audio_timing`. Leave it at `0` for normal runs so the 20ms audio hot path skips
timing instrumentation.
