#!/usr/bin/env bash
# Build (and optionally push) the STARE-PODS cloud worker image.
#
#   ./infra/worker/build.sh          # wheel + image + verification
#   ./infra/worker/build.sh --push   # …then ECR login, tag, push
#
# Exists because the Dockerfile installs a PRE-BUILT wheel from
# infra/worker/dist/ (versioneer can't resolve the worktree .git inside the
# container). A `docker buildx build` against a stale wheel cache-hits every
# layer and silently reproduces the OLD image byte-for-byte — same digest,
# none of your changes (this happened on 2026-08-21). This script makes that
# impossible: it always rebuilds the wheel from the current tree, and after
# the image build it extracts starepandas/_version.py from the image
# filesystem (docker create + cp — no amd64 emulation) and fails unless the
# version inside the image matches the wheel it just built.
#
# A gcc "exit -11" segfault compiling psycopg2 under emulation is transient —
# just re-run (docs/path_c_runbook.md §6f).

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DIST_DIR="$REPO_ROOT/infra/worker/dist"
IMAGE="starepods/worker:dev"
ECR_REGISTRY="637388276731.dkr.ecr.us-west-2.amazonaws.com"
CONDA_ENV="starepandas_3.12_v3"

cd "$REPO_ROOT"

echo "==> 1/4 Rebuilding the wheel from the current tree"
rm -f "$DIST_DIR"/*.whl
conda run -n "$CONDA_ENV" python setup.py bdist_wheel -d "$DIST_DIR" >/dev/null
WHEEL="$(ls "$DIST_DIR"/*.whl)"
# starepandas-<version>-py3-none-any.whl -> <version>
WHEEL_VERSION="$(basename "$WHEEL" | sed -E 's/^starepandas-(.+)-py3-none-any\.whl$/\1/')"
echo "    $WHEEL_VERSION"

echo "==> 2/4 Building $IMAGE (linux/amd64)"
docker buildx build --platform=linux/amd64 --provenance=false --sbom=false \
    -f infra/worker/Dockerfile -t "$IMAGE" --load .

echo "==> 3/4 Verifying the image contains the wheel just built"
CID="$(docker create --platform linux/amd64 "$IMAGE")"
trap 'docker rm -f "$CID" >/dev/null' EXIT
IMAGE_VERSION="$(docker cp "$CID":/usr/local/lib/python3.12/site-packages/starepandas/_version.py - \
    | tar -xO | sed -nE 's/.*"version": "([^"]+)".*/\1/p' | head -1)"
if [[ "$IMAGE_VERSION" != "$WHEEL_VERSION" ]]; then
    echo "ERROR: image carries starepandas $IMAGE_VERSION but the wheel is" >&2
    echo "       $WHEEL_VERSION — the build cache served a stale layer." >&2
    exit 1
fi
echo "    image version matches: $IMAGE_VERSION"

if [[ "${1:-}" != "--push" ]]; then
    echo "==> 4/4 Skipping push (pass --push to tag + push to ECR)"
    exit 0
fi

echo "==> 4/4 Pushing to $ECR_REGISTRY/$IMAGE"
# No aws CLI needed — mint the ECR token with boto3 using .config creds
# (docs/path_c_runbook.md §6f).
conda run -n "$CONDA_ENV" python - <<'EOF' | docker login --username AWS --password-stdin "$ECR_REGISTRY"
import base64, boto3
cfg = dict(l.strip().split('=', 1) for l in open('starepandas/.config')
           if '=' in l and not l.startswith('#'))
t = boto3.client('ecr', region_name='us-west-2', aws_access_key_id=cfg['key'],
                 aws_secret_access_key=cfg['secret']
    ).get_authorization_token()['authorizationData'][0]['authorizationToken']
print(base64.b64decode(t).decode().split(':', 1)[1], end='')
EOF
docker tag "$IMAGE" "$ECR_REGISTRY/$IMAGE"
docker push "$ECR_REGISTRY/$IMAGE"
