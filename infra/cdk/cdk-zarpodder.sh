#!/usr/bin/env bash
#
# Run any `cdk` command with the zarpodder creds from starepandas/.config.
# CDK reads AWS creds from the env chain, but zarpodder's keys live only in
# the project .config file — this wrapper extracts them into env vars
# (via command substitution, so the secret is never echoed) and execs cdk.
#
# Usage (from anywhere):
#   infra/cdk/cdk-zarpodder.sh deploy  StarePodsInfraStack --require-approval never
#   infra/cdk/cdk-zarpodder.sh destroy StarePodsInfraStack
#   infra/cdk/cdk-zarpodder.sh diff    StarePodsInfraStack
#   infra/cdk/cdk-zarpodder.sh synth                      # (no creds needed, but harmless)
#
# Override the interpreter with PYBIN=... if the conda env path differs.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/../.." && pwd)"
PYBIN="${PYBIN:-/opt/miniconda3/envs/starepandas_3.12_v3/bin/python}"
CFG="$REPO/starepandas/.config"

if [[ ! -f "$CFG" ]]; then
  echo "error: $CFG not found (zarpodder creds expected there)" >&2
  exit 1
fi

_extract() {  # $1 = key name in _AWS_S3_STORAGE_OPTIONS
  "$PYBIN" -c "import os;os.environ['STAREPANDAS_AWS_CONFIG']='$CFG';import starepandas.staredataframe as s;s._load_config_from_default_locations();print(s._AWS_S3_STORAGE_OPTIONS['$1'])"
}

export JSII_SILENCE_WARNING_UNTESTED_NODE_VERSION=1
export AWS_DEFAULT_REGION=us-west-2
export CDK_DEFAULT_ACCOUNT=637388276731
export CDK_DEFAULT_REGION=us-west-2
export AWS_ACCESS_KEY_ID="$(_extract key)"
export AWS_SECRET_ACCESS_KEY="$(_extract secret)"

echo "cdk $* — as $(echo "$AWS_ACCESS_KEY_ID" | cut -c1-8)… in $AWS_DEFAULT_REGION" >&2
cd "$HERE"
exec cdk "$@"
