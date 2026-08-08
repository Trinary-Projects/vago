#!/usr/bin/env bash
set -euo pipefail

# Resolve the deploy environment from the prod base plus optional overrides.

if (( $# != 2 )); then
  echo "Usage: $0 <prod|staging> <output-env-file>" >&2
  exit 1
fi

environment="$1"
output_file="$2"

if [[ "$environment" != "prod" && "$environment" != "staging" ]]; then
  echo "Unsupported deploy environment: ${environment}" >&2
  exit 1
fi

case "$(basename "$output_file")" in
  .env|.prod.env|.staging.env)
    echo "Refusing to overwrite local environment file: ${output_file}" >&2
    exit 1
    ;;
esac

for cmd in aws jq; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Missing required command: $cmd" >&2
    exit 1
  fi
done

ssm_region="${VAGO_SSM_REGION:-ap-south-1}"
ssm_prefix="${VAGO_SSM_PREFIX:-/vago}"
ssm_prefix="${ssm_prefix%/}"
prod_path="${ssm_prefix}/prod/"
environment_path="${ssm_prefix}/${environment}/"

umask 077
prod_json="$(mktemp "${TMPDIR:-/tmp}/vago-prod-parameters.XXXXXX")"
environment_json="$(mktemp "${TMPDIR:-/tmp}/vago-${environment}-parameters.XXXXXX")"
rendered_env="$(mktemp "${TMPDIR:-/tmp}/vago-deploy-env.XXXXXX")"

cleanup() {
  rm -f -- "$prod_json" "$environment_json" "$rendered_env"
}
trap cleanup EXIT

fetch_path() {
  local path="$1"
  local destination="$2"

  aws ssm get-parameters-by-path \
    --region "$ssm_region" \
    --path "$path" \
    --with-decryption \
    --output json \
    --no-cli-pager > "$destination"

  if ! jq -e --arg path "$path" '
    (.Parameters | type == "array") and
    all(.Parameters[];
      (.Name | startswith($path)) and
      ((.Name | ltrimstr($path)) | test("^[A-Za-z_][A-Za-z0-9_]*$")) and
      (.Value | type == "string") and
      ((.Value | contains("\n")) | not) and
      ((.Value | contains("\r")) | not)
    )
  ' "$destination" >/dev/null; then
    echo "Invalid parameter name or multiline value under ${path}" >&2
    exit 1
  fi
}

echo "Fetching base deploy environment from ${prod_path} (${ssm_region})..." >&2
fetch_path "$prod_path" "$prod_json"

prod_count="$(jq '.Parameters | length' "$prod_json")"
if (( prod_count == 0 )); then
  echo "No parameters found under required base path ${prod_path}" >&2
  exit 1
fi

if [[ "$environment" == "prod" ]]; then
  printf '%s\n' '{"Parameters":[]}' > "$environment_json"
else
  echo "Fetching staging overrides from ${environment_path} (${ssm_region})..." >&2
  fetch_path "$environment_path" "$environment_json"
fi

jq -r -s \
  --arg prod_path "$prod_path" \
  --arg environment_path "$environment_path" '
    . as $responses |
    reduce $responses[0].Parameters[] as $parameter ({};
      ($parameter.Name | ltrimstr($prod_path)) as $key |
      .[$key] = $parameter.Value
    ) |
    reduce $responses[1].Parameters[] as $parameter (.;
      ($parameter.Name | ltrimstr($environment_path)) as $key |
      .[$key] = $parameter.Value
    ) |
    to_entries | sort_by(.key)[] | "\(.key)=\(.value)"
  ' "$prod_json" "$environment_json" > "$rendered_env"

chmod 600 "$rendered_env"
mv "$rendered_env" "$output_file"
chmod 600 "$output_file"

parameter_count="$(wc -l < "$output_file" | tr -d ' ')"
echo "Prepared temporary deploy environment with ${parameter_count} parameters." >&2
