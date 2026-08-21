# STARE-PODS cloud worker — Path C C-2.
#
# Build context: project root. Build via the wrapper script — it rebuilds the
# wheel first and verifies the image isn't a stale-cache reproduction:
#   ./infra/worker/build.sh          # build + verify
#   ./infra/worker/build.sh --push   # …then ECR login, tag, push
#
# Two-stage build:
#   Stage 1 (builder) — has compilers + libpq-dev so source-only deps
#                       (psycopg2) can build. We install starepandas + its
#                       runtime deps into /install via pip --prefix.
#   Stage 2 (runtime) — slim base + minimal runtime apt libs + the
#                       /install tree copied from stage 1. No compilers,
#                       no -dev headers, no source tree.
#
# Why the pre-built wheel: starepandas/.git here is a *worktree pointer
# file* that points outside the build context, so versioneer inside the
# container would return "" and setuptools would reject the empty
# version. Building the wheel on the host (where the real git resolves
# correctly) sidesteps that.
#
# Why we don't substitute psycopg2-binary: pip treats `psycopg2` and
# `psycopg2-binary` as separate distributions, so the wheel's
# `psycopg2>=2.9.6` constraint isn't satisfied by the -binary package.
# Compiling psycopg2 from source in the builder stage is cleaner than
# patching setup.cfg.
#
# Runtime config is environment-driven; see starepandas/cloud/worker.py
# for the supported env vars. RDS + S3 credentials come from the same
# .config file the local-host pipeline uses, mounted at
# /etc/starepods/.config via the ECS task definition's `secrets` block
# (§C4) and pointed to by STAREPANDAS_AWS_CONFIG.

# ── Stage 1: build ──────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        libpq-dev \
        libhdf4-0-alt \
        libhdf4-alt-dev \
        libgeos-dev \
        libproj-dev \
        proj-data \
        proj-bin \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY infra/worker/dist/*.whl /tmp/

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir --prefix=/install /tmp/*.whl \
    && rm /tmp/*.whl

# ── Stage 2: runtime ────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq5 \
        libhdf4-0-alt \
        libgeos-c1v5 \
        libproj25 \
        proj-data \
        libexpat1 \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local

RUN useradd --create-home --uid 1001 starepods \
    && mkdir -p /tmp/starepods-worker /etc/starepods \
    && chown -R starepods /tmp/starepods-worker
USER starepods

ENV STAREPANDAS_AWS_CONFIG=/etc/starepods/.config \
    WORK_DIR=/tmp/starepods-worker \
    PYTHONUNBUFFERED=1

ENTRYPOINT ["python", "-m", "starepandas.cloud.worker"]
