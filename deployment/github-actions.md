# GitHub Actions Kubernetes deployments

Status: staging cloud identities, Kubernetes RBAC and GitHub environment configured on 2026-09-30. First GitHub-hosted test pending on `codex/github-actions-staging-test`. This branch temporarily deploys staging on push; remove that trigger after testing. Production setup is still pending.

## Running a deployment once enabled

In GitHub web or GitHub Mobile: open this repository → Actions → **Deploy to Kubernetes** → **Run workflow**, choose `main`, and select `staging` or `prod`. The workflow file must be on the default branch before manual dispatch appears. Mobile supports manual dispatch; use the web run page for live log streaming if the native app does not expose it.

Each phase has its own status and logs. Docker uses plain BuildKit progress; migration errors remain in the migration step. After Kubernetes is connected, best-effort diagnostics run even on failure. Failed runs include the last 100 current/previous container log lines for unhealthy pods created by that attempt; fetched credential values are masked before these logs are read. The summary records the commit and image tag. Nothing posts to Slack in this first CI version.

Vago phases: fetch configuration → registry/cluster connection check → Docker build → push → runtime Secret update → manifest apply → rollout. There is no migration command in the existing Vago deployment scripts, so none is added.

Actions deploys the checked-out commit and never pulls a newer commit mid-run. Images use `<commit>-<run-id>-<attempt>` tags; local deploys still default to `latest`. A rerun gets a new tag. No automatic rollback is performed, especially for database migrations. Do not rerun a migration blindly after a timeout: inspect the migration step and database state first.

Vago serializes deployments per environment. Staging and prod may run concurrently.

GitHub's default concurrency keeps one running and at most one pending run per group; a newer pending request can replace an older pending request. This is not a durable FIFO queue. Do not start a local deployment while an Actions deployment is active: the GitHub lock does not cover laptop commands.

## One-time setup (administrator)

The staging configuration below has been applied. Production remains a setup reference and has not been applied. During the branch test, staging also permits the exact `codex/github-actions-staging-test` branch; the GCP provider is currently restricted to the staging subject. Use a separate identity for each repo/environment. Runtime application AWS credentials remain managed by the existing SSM paths; the Actions AWS role only fetches configuration.

### 1. GitHub environments

Create `staging` and `prod` under repository Settings → Environments. Restrict **both** to the `main` branch (selected branches, not all protected branches). Staging inherits production SSM values, so it must also run only trusted code. Keep deployment approval optional according to the emergency access policy you want. A required reviewer adds a wait before cloud authentication.

Set these environment variables (not repository secrets):

| Variable | Staging | Prod |
|---|---|---|
| `AWS_ROLE_ARN` | `arn:aws:iam::730335316179:role/github-vago-staging` | `arn:aws:iam::730335316179:role/github-vago-prod` |
| `GCP_SERVICE_ACCOUNT` | `github-vago-staging@curelinkai.iam.gserviceaccount.com` | `github-vago-prod@curelinkai.iam.gserviceaccount.com` |
| `GCP_WORKLOAD_IDENTITY_PROVIDER` | `projects/859728685429/locations/global/workloadIdentityPools/github-deploy/providers/vago` | same |

No GCP JSON key or static AWS key is needed for deployment authentication. The application environment is still fetched from SSM and installed as the existing Kubernetes Secret.

### 2. AWS OIDC and SSM read role

Create the account-level OIDC provider once: URL `https://token.actions.githubusercontent.com`, audience `sts.amazonaws.com`. Reuse it for both repos. For each environment, create the role named above with this trust policy, replacing `ENV` with `staging` or `prod`:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {"Federated": "arn:aws:iam::730335316179:oidc-provider/token.actions.githubusercontent.com"},
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {"StringEquals": {
      "token.actions.githubusercontent.com:aud": "sts.amazonaws.com",
      "token.actions.githubusercontent.com:sub": "repo:Trinary-Projects/vago:environment:ENV"
    }}
  }]
}
```

Attach an inline policy allowing only `ssm:GetParametersByPath` on `arn:aws:ssm:ap-south-1:730335316179:parameter/vago/prod/*`. The staging role also needs `arn:aws:ssm:ap-south-1:730335316179:parameter/vago/staging/*` because the existing fetcher merges a production base and staging overrides. This is existing behavior, not full credential isolation between environments.

For SecureStrings encrypted under a customer-managed KMS key, add `kms:Decrypt` on that exact key ARN, constrained with `kms:ViaService = ssm.ap-south-1.amazonaws.com`, and permit the role in the key policy. The AWS-managed `alias/aws/ssm` case normally needs no additional customer key policy. Confirm actual parameter key metadata before provisioning; do not grant wildcard KMS access.

AWS environment subjects do not encode the branch. The GitHub environment branch restriction above is essential.

### 3. GCP Workload Identity Federation

Create pool `github-deploy` in project `curelinkai` once, then provider `vago` with issuer `https://token.actions.githubusercontent.com`. Map:

```text
google.subject=assertion.sub
attribute.repository_id=assertion.repository_id
attribute.repository_owner_id=assertion.repository_owner_id
attribute.ref=assertion.ref
attribute.workflow_ref=assertion.workflow_ref
```

Use this provider attribute condition (numeric IDs resist organization/repository name reuse):

```text
assertion.repository_owner_id == '72670721' &&
assertion.repository_id == '1167931098' &&
assertion.ref == 'refs/heads/main' &&
assertion.workflow_ref == 'Trinary-Projects/vago/.github/workflows/deploy-k8s.yml@refs/heads/main' &&
assertion.sub in ['repo:Trinary-Projects/vago:environment:staging', 'repo:Trinary-Projects/vago:environment:prod']
```

Create the two service accounts listed in the GitHub variables table. On **each service account**, grant `roles/iam.workloadIdentityUser` only to its exact matching subject:

```text
principal://iam.googleapis.com/projects/859728685429/locations/global/workloadIdentityPools/github-deploy/subject/repo:Trinary-Projects/vago:environment:ENV
```

Enable IAM Service Account Credentials API (`iamcredentials.googleapis.com`) and Security Token Service API (`sts.googleapis.com`) if needed. No JSON deployment key is required. Grant each service account:

- `roles/artifactregistry.writer` on its own environment's Artifact Registry repository below, not the whole project.
- `roles/container.clusterViewer` on `curelinkai` to retrieve cluster connection metadata. Kubernetes write privileges come from the scoped RBAC below; do not grant project Owner, Editor, IAM admin, or cluster-admin to CI.

| Environment | Registry repository (region) | GKE cluster (region) | Namespace |
|---|---|---|---|
| staging | `disha-voice-worker-staging` (`us-east1`) | `disha-voice-worker-staging` (`us-east1`) | `staging` |
| prod | `disha-voice-worker-prod` (`us-east1`) | `disha-voice-worker-prod` (`us-east4`) | `prod` |

The workflow obtains one-hour service-account access tokens immediately before push, and refreshes them before applying manifests. `CLOUDSDK_AUTH_ACCESS_TOKEN` is passed to gcloud and the GKE auth plugin, so a long Docker build does not consume the deployment token's lifetime. Generated `gha-creds-*.json` files are excluded from Git and Docker contexts.

### 4. Kubernetes authorization and existing infrastructure

For each row in the table, an existing cluster administrator should obtain that cluster's credentials and apply the supplied RBAC template. Confirm the namespace already exists. Example for staging (change the three target values together for prod):

```bash
export GKE_NAMESPACE=staging
export GCP_SERVICE_ACCOUNT=github-vago-staging@curelinkai.iam.gserviceaccount.com
export CLUSTER_NAME=disha-voice-worker-staging
export CLUSTER_REGION=us-east1
gcloud container clusters get-credentials "$CLUSTER_NAME" --region "$CLUSTER_REGION" --project curelinkai
envsubst '$GKE_NAMESPACE $GCP_SERVICE_ACCOUNT' < deployment/github-actions-rbac.yaml \
  | kubectl --context "gke_curelinkai_${CLUSTER_REGION}_${CLUSTER_NAME}" apply -f -
```

RBAC grants read access to rollout state, events and pod logs, plus get/create/patch/update for the resource types applied by the existing manifests in that namespace. It allows writing runtime Secrets, as the existing script does. Disha additionally needs the `pod-reader` Role/RoleBinding and narrowly scoped access to the existing named cluster-level KEDA authentication object; its bootstrap template covers those.

The existing Vago rollout timeout remains 10 minutes. Its pod termination grace period is 90 minutes, so a draining worker may outlast this wait: a red timeout needs inspection and does not prove that Kubernetes stopped rolling out. Keeping this initial script behavior avoids changing worker lifecycle policy; tune the wait separately after a real run. KEDA CRDs and existing node pools must already exist.

### 5. Network access and activation

Before calling this ready for emergencies, verify an actual hosted runner can reach the Kubernetes API endpoint. Credentials alone do not establish reachability. If existing firewall/private endpoint restrictions block hosted runners, use a runner in the existing trusted network (or an explicitly configured private connection), instead of opening the database to the internet.

Review and merge the code to `main`, configure the identities/environments, and run a supervised first staging deployment. This is a real staging deployment, not a dry run. Record the successful run URL and test that you can dispatch from your phone. On 2026-09-30, the staged phases were successfully run locally against this staging cluster, including rollout and HTTP health/readiness checks. This did not test GitHub-hosted runners or OIDC; no production deployment was run.

The local commands remain available for GitHub outages (`./deploy-staging.sh` and `./deploy-prod.sh`). This fallback needs a clean trusted checkout, local credentials/network access, and Docker. Actions is not an independent fallback during a GitHub outage, and a hosted run does not by itself prove emergency reliability.

## Offline validation

Requires Bash, Python 3, `envsubst` (gettext) and `jq`; no cloud credentials are used by the tests.

```bash
python3 .github/tests/test_deploy.py
bash -n deploy-worker.sh
# If actionlint is installed:
actionlint .github/workflows/deploy-k8s.yml
```

The regression tests use temporary directories and mocked cloud commands. They prove phase failure propagation, target guards and image substitution, not cloud IAM, network access, dependency installation or a real rollout.

Sources: [GitHub manual dispatch](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/manually-run-a-workflow), [GitHub Mobile dispatch](https://github.blog/changelog/2024-07-30-run-workflows-set-as-workflow_dispatch-manually/), [Google authentication action](https://github.com/google-github-actions/auth), [GKE authorization](https://cloud.google.com/kubernetes-engine/docs/how-to/role-based-access-control).

Local test follow-up: all five new staging workers were ready with zero container restarts at verification. Existing PodDisruptionBudgets `talk-go-worker-pdb` and `disha-go-voice-worker-staging-pdb` select the same pods; their overlap was recorded, not changed.
