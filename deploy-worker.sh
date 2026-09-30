#!/usr/bin/env bash
# Shared implementation for deploy-prod.sh / deploy-staging.sh.
set -Eeuo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"
DEPLOY_ENVIRONMENT="${1:?Expected staging or prod}"
DEPLOY_PHASE="${2:-all}"
case "$DEPLOY_ENVIRONMENT" in
    prod) MANIFEST="k8s/worker.yaml"; DEFAULT_CLUSTER_LOCATION=us-east4 ;;
    staging) MANIFEST="k8s/worker-staging.yaml"; DEFAULT_CLUSTER_LOCATION=us-east1 ;;
    *) echo "Expected staging or prod" >&2; exit 2 ;;
esac
case "$DEPLOY_PHASE" in all|prepare|authenticate|build|push|secret|apply|rollout|diagnostics) ;; *) echo "Unknown deployment phase" >&2; exit 2 ;; esac
umask 077
ENV_FILE="$ROOT_DIR/.temp-deploy.env"
trap 'code=$?; echo "Deployment phase failed: $DEPLOY_PHASE (exit $code)" >&2; exit "$code"' ERR

load_env_file() {
  local file="$1"
  local line key value
  while IFS= read -r line || [[ -n "$line" ]]; do
    line="${line%$'\r'}"
    [[ -z "$line" || "$line" == \#* ]] && continue
    if [[ "$line" == export\ * ]]; then
      line="${line#export }"
    fi
    [[ "$line" == *=* ]] || continue

    key="${line%%=*}"
    key="${key//[[:space:]]/}"
    value="${line#*=}"
    if [[ "$key" =~ ^[A-Za-z_][A-Za-z0-9_]*$ ]]; then
      printf -v "$key" '%s' "$value"
      export "$key"
    fi
  done < "$file"
}

prepare() {
    required_commands=(aws jq docker kubectl envsubst gcloud git)
    for cmd in "${required_commands[@]}"; do
      if ! command -v "$cmd" >/dev/null 2>&1; then
        echo "Missing required command: $cmd" >&2
        exit 1
      fi
    done

    # Prod images are built from the working tree (docker build .), so the deploy
    # must run from an up-to-date, clean main checkout or the pushed image can
    # silently miss merged changes.
    if [[ "$DEPLOY_ENVIRONMENT" == "prod" && "${GITHUB_ACTIONS:-}" != "true" ]]; then
        current_branch="$(git rev-parse --abbrev-ref HEAD)"
        if [[ "$current_branch" != "main" ]]; then
          echo "Refusing prod deploy: current branch is '${current_branch}', not main." >&2
          exit 1
        fi
        if [[ -n "$(git status --porcelain)" ]]; then
          echo "Refusing prod deploy: working tree has uncommitted or untracked changes:" >&2
          git status --short >&2
          exit 1
        fi
        echo "Pulling latest main..."
        git pull --ff-only origin main
    fi

    if [[ "${GITHUB_ACTIONS:-}" == "true" ]]; then
        [[ "$(git rev-parse HEAD)" == "$GITHUB_SHA" ]] || { echo "Checkout differs from requested commit" >&2; exit 1; }
        if [[ "$DEPLOY_ENVIRONMENT" == "prod" && "$GITHUB_REF" != "refs/heads/main" ]]; then
            echo "Production deployments require main" >&2
            exit 1
        fi
    fi
    "$ROOT_DIR/fetch-deploy-env.sh" "$DEPLOY_ENVIRONMENT" "$ENV_FILE"
}

authenticate() {
    gcloud auth configure-docker us-east1-docker.pkg.dev --quiet
    echo "Fetching credentials for cluster ${GKE_CLUSTER_NAME} (${GKE_CLUSTER_LOCATION})..."
    gcloud container clusters get-credentials "$GKE_CLUSTER_NAME" \
      --region "$GKE_CLUSTER_LOCATION" \
      --project "$GCP_PROJECT_ID"

    if ! kubectl config get-contexts -o name | grep -qx "$KUBE_CONTEXT"; then
      echo "Expected kube context '${KUBE_CONTEXT}' not found after get-credentials." >&2
      echo "Check GKE_CLUSTER_NAME / GKE_CLUSTER_LOCATION / GCP_PROJECT_ID." >&2
      exit 1
    fi
    # Test the API endpoint and namespace read access before later phases mutate anything.
    kubectl --context "$KUBE_CONTEXT" --request-timeout=20s get deployments -n "$GKE_NAMESPACE" -o name

}

build() {
    docker build --platform=linux/amd64 -t "$IMAGE" .
}

push() {
    docker push "$IMAGE"
}

update_secret() {
    echo "Updating talk-go-worker-env secret from the temporary Parameter Store environment..."
    kubectl --context "$KUBE_CONTEXT" create secret generic talk-go-worker-env \
      --namespace "$GKE_NAMESPACE" \
      --from-env-file="$ENV_FILE" \
      --dry-run=client -o yaml \
      | kubectl --context "$KUBE_CONTEXT" apply -f -
}

apply_manifest() {
    echo "Applying Kubernetes manifest..."
    export GCP_PROJECT_ID
    export ARTIFACT_REPOSITORY_NAME
    export GKE_NAMESPACE
    export GKE_DEPLOYMENT_NAME
    export POD_TEMPLATE_VERSION

    envsubst '$IMAGE $GKE_DEPLOYMENT_NAME $ARTIFACT_REPOSITORY_NAME $GKE_NAMESPACE $GCP_PROJECT_ID $POD_TEMPLATE_VERSION $DB_USER $DB_PASSWORD $DB_HOST $DB_PORT $DB_NAME' \
      < "$MANIFEST" \
      | kubectl --context "$KUBE_CONTEXT" apply -f -
}

rollout() {
    echo "Waiting for rollout..."
    kubectl --context "$KUBE_CONTEXT" rollout status "deployment/${GKE_DEPLOYMENT_NAME}" \
      --namespace "$GKE_NAMESPACE" \
      --timeout="${DEPLOY_ROLLOUT_TIMEOUT:-10m}"
}

diagnostics() {
    kubectl --context "$KUBE_CONTEXT" get deployments,pods --namespace "$GKE_NAMESPACE" -l "app=$GKE_DEPLOYMENT_NAME" -o wide
    kubectl --context "$KUBE_CONTEXT" get events --namespace "$GKE_NAMESPACE" --field-selector type=Warning --sort-by=.lastTimestamp
    if [[ "${GITHUB_ACTIONS:-}" == "true" && "${DEPLOY_FAILED:-}" == "true" ]]; then
        # Only inspect unhealthy pods from this workflow attempt, not older application traffic.
        failed_pods=$(kubectl --context "$KUBE_CONTEXT" get pods -n "$GKE_NAMESPACE" \
            -l "pod-template-hash-binpack=$DEPLOY_POD_VERSION" -o json \
            | jq -r '.items[] | select(.status.phase != "Running" or ((.status.containerStatuses // []) | any(.ready != true))) | .metadata.name')
        for pod in $failed_pods; do
            echo "::group::Startup logs: $pod"
            kubectl --context "$KUBE_CONTEXT" logs "$pod" -n "$GKE_NAMESPACE" --all-containers=true --tail=100 --timestamps=true || true
            kubectl --context "$KUBE_CONTEXT" logs "$pod" -n "$GKE_NAMESPACE" --all-containers=true --tail=100 --timestamps=true --previous || true
            echo "::endgroup::"
        done
    fi
}


if [[ "$DEPLOY_PHASE" == "all" || "$DEPLOY_PHASE" == "prepare" ]]; then
    prepare
fi
[[ "$DEPLOY_PHASE" == "prepare" ]] && exit 0
load_env_file "$ENV_FILE"
GCP_PROJECT_ID="${GCP_PROJECT_ID:-curelinkai}"
ARTIFACT_REPOSITORY_NAME="${ARTIFACT_REPOSITORY_NAME:-disha-voice-worker-$DEPLOY_ENVIRONMENT}"
GKE_NAMESPACE="${GKE_NAMESPACE:-$DEPLOY_ENVIRONMENT}"
GKE_DEPLOYMENT_NAME="${GKE_DEPLOYMENT_NAME:-disha-go-voice-worker-$DEPLOY_ENVIRONMENT}"

# Target cluster is pinned here so the deploy NEVER follows whatever the local
# kubectl current-context happens to be. All kubectl calls below run against
# "$KUBE_CONTEXT" explicitly.
GKE_CLUSTER_NAME="${GKE_CLUSTER_NAME:-disha-voice-worker-$DEPLOY_ENVIRONMENT}"
GKE_CLUSTER_LOCATION="${GKE_CLUSTER_LOCATION:-$DEFAULT_CLUSTER_LOCATION}"
KUBE_CONTEXT="gke_${GCP_PROJECT_ID}_${GKE_CLUSTER_LOCATION}_${GKE_CLUSTER_NAME}"
if [[ "$DEPLOY_ENVIRONMENT" == "prod" ]]; then
    if [[ "$GKE_DEPLOYMENT_NAME" != "disha-go-voice-worker-prod" ]]; then
      echo "Refusing prod deploy: GKE_DEPLOYMENT_NAME must be disha-go-voice-worker-prod." >&2
      exit 1
    fi
    if [[ "${ENVIRONMENT:-}" != "production" && "${ENVIRONMENT:-}" != "prod" ]]; then
      echo "Refusing prod deploy: ENVIRONMENT must be production or prod in Parameter Store." >&2
      exit 1
    fi
    if [[ -z "${DISHA_API_URL:-}${API_BASE_URL:-}" ]]; then
      echo "Refusing prod deploy: DISHA_API_URL or API_BASE_URL must be set in Parameter Store." >&2
      exit 1
    fi
    if [[ "$GKE_NAMESPACE" == "staging" || "$GKE_CLUSTER_NAME" == *staging* || "$ARTIFACT_REPOSITORY_NAME" == *staging* ]]; then
      echo "Refusing prod deploy: staging-looking deployment values remain in Parameter Store." >&2
      exit 1
    fi
    for var in DISHA_API_URL API_BASE_URL DISHA_REDIS_URL AWS_BUCKET_NAME; do
      value="${!var-}"
      if [[ "$value" == *staging* ]]; then
        echo "Refusing prod deploy: ${var} still looks like a staging value in Parameter Store." >&2
        exit 1
      fi
    done
else
    if [[ "$GKE_DEPLOYMENT_NAME" != "disha-go-voice-worker-staging" ]]; then
      echo "Refusing staging deploy: GKE_DEPLOYMENT_NAME must be disha-go-voice-worker-staging." >&2
      exit 1
    fi
    if [[ "$GKE_NAMESPACE" == "prod" || "$GKE_CLUSTER_NAME" == *prod* || "$ARTIFACT_REPOSITORY_NAME" == *prod* ]]; then
      echo "Refusing staging deploy: prod-looking deployment values remain after applying staging overrides." >&2
      exit 1
    fi
fi
timestamp="$(date -u +%Y%m%d%H%M%S)"
POD_TEMPLATE_VERSION="${POD_TEMPLATE_VERSION:-v${timestamp}}"
IMAGE_REPOSITORY="us-east1-docker.pkg.dev/${GCP_PROJECT_ID}/${ARTIFACT_REPOSITORY_NAME}/talk-go-worker"
IMAGE="${IMAGE_REPOSITORY}:${DEPLOY_IMAGE_TAG:-latest}"
export IMAGE
POD_TEMPLATE_VERSION="${DEPLOY_POD_VERSION:-$POD_TEMPLATE_VERSION}"
missing_db_env=()
for var in DB_USER DB_PASSWORD DB_HOST DB_PORT DB_NAME; do
  if [[ -z "${!var:-}" ]]; then
    missing_db_env+=("$var")
  fi
done
if (( ${#missing_db_env[@]} > 0 )); then
  echo "Missing DB env required for ${MANIFEST}: ${missing_db_env[*]}" >&2
  exit 1
fi

case "$DEPLOY_PHASE" in
    authenticate) authenticate ;;
    build) build ;;
    push) push ;;
    secret) update_secret ;;
    apply) apply_manifest ;;
    rollout) rollout ;;
    diagnostics) diagnostics ;;
    all)
        authenticate
        build
        push
        update_secret
        apply_manifest
        rollout
        diagnostics
        echo "Done."
        ;;
esac
