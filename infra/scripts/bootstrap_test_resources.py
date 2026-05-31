#!/usr/bin/env python3
"""Stand up the AWS resources needed for the C-2 local Docker integration test.

Creates (idempotent — safe to re-run):

* SQS queue ``starepods-tickets-test`` with visibility timeout 3600 s
  (§C10 #5 Option A: avoid mid-ticket redelivery without heartbeats).
* DynamoDB table ``StarePodsJobs-test`` (PK ``job_id``).
* DynamoDB table ``StarePodsFailures-test`` (PK ``job_id``, SK ``granule_uri``).
* ECR repository ``starepods/worker``.

Every resource is tagged ``Project=starepods`` so the §C9 Budgets alarm
will pick it up later. Reads credentials from the same ``.config`` file
``starepandas/_load_config_from_default_locations`` consumes.

Usage::

    conda run -n starepandas_3.12_v3 python infra/scripts/bootstrap_test_resources.py
"""

from __future__ import annotations

import json
import sys
import time
from typing import Any, Dict

import os
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

# .config lives next to the package; force the loader to find it before
# any AWS client is created.
_PKG_CFG = Path(__file__).resolve().parents[2] / "starepandas" / ".config"
if _PKG_CFG.exists() and not os.environ.get("STAREPANDAS_AWS_CONFIG"):
    os.environ["STAREPANDAS_AWS_CONFIG"] = str(_PKG_CFG)

import starepandas  # noqa: E402,F401 — side-effect: loads .config
from starepandas.staredataframe import (  # noqa: E402
    _AWS_S3_STORAGE_OPTIONS,
    _load_config_from_default_locations,
)

_load_config_from_default_locations()

REGION = "us-west-2"
TAGS = [{"Key": "Project", "Value": "starepods"}]
QUEUE_NAME = "starepods-tickets-test"
JOBS_TABLE = "StarePodsJobs-test"
FAILURES_TABLE = "StarePodsFailures-test"
ECR_REPO = "starepods/worker"
VISIBILITY_TIMEOUT = "3600"  # 1 hour, §C10 #5 Option A


def _client(service: str):
    """boto3 client wired to the same .config creds the library uses."""
    # Re-read in case the loader populated this after import.
    from starepandas.staredataframe import _AWS_S3_STORAGE_OPTIONS as opts
    region = (opts.get("client_kwargs") or {}).get("region_name") or REGION
    return boto3.client(
        service,
        region_name=region,
        aws_access_key_id=opts.get("key"),
        aws_secret_access_key=opts.get("secret"),
        aws_session_token=opts.get("token"),
    )


# ── SQS ──────────────────────────────────────────────────────────────────


def ensure_queue() -> str:
    sqs = _client("sqs")
    try:
        url = sqs.create_queue(
            QueueName=QUEUE_NAME,
            Attributes={
                "VisibilityTimeout": VISIBILITY_TIMEOUT,
                "ReceiveMessageWaitTimeSeconds": "20",  # long-poll on the server side too
                "MessageRetentionPeriod": "86400",
            },
            tags={t["Key"]: t["Value"] for t in TAGS},
        )["QueueUrl"]
        print(f"✓ created queue {QUEUE_NAME} → {url}")
    except sqs.exceptions.QueueNameExists:
        url = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
        # Apply attributes idempotently — useful if a previous run set them differently.
        sqs.set_queue_attributes(
            QueueUrl=url,
            Attributes={
                "VisibilityTimeout": VISIBILITY_TIMEOUT,
                "ReceiveMessageWaitTimeSeconds": "20",
            },
        )
        sqs.tag_queue(QueueUrl=url, Tags={t["Key"]: t["Value"] for t in TAGS})
        print(f"✓ queue {QUEUE_NAME} already exists → {url}")
    return url


# ── DynamoDB ─────────────────────────────────────────────────────────────


def _ensure_table(name: str, key_schema, attribute_defs) -> None:
    ddb = _client("dynamodb")
    try:
        ddb.create_table(
            TableName=name,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_defs,
            BillingMode="PAY_PER_REQUEST",
            Tags=TAGS,
        )
        print(f"✓ creating table {name}…")
        ddb.get_waiter("table_exists").wait(TableName=name)
        print(f"✓ table {name} ACTIVE")
    except ddb.exceptions.ResourceInUseException:
        ddb.get_waiter("table_exists").wait(TableName=name)
        print(f"✓ table {name} already exists")


def ensure_jobs_table() -> None:
    _ensure_table(
        JOBS_TABLE,
        [{"AttributeName": "job_id", "KeyType": "HASH"}],
        [{"AttributeName": "job_id", "AttributeType": "S"}],
    )


def ensure_failures_table() -> None:
    _ensure_table(
        FAILURES_TABLE,
        [
            {"AttributeName": "job_id", "KeyType": "HASH"},
            {"AttributeName": "granule_uri", "KeyType": "RANGE"},
        ],
        [
            {"AttributeName": "job_id", "AttributeType": "S"},
            {"AttributeName": "granule_uri", "AttributeType": "S"},
        ],
    )


# ── ECR ──────────────────────────────────────────────────────────────────


def ensure_ecr_repo() -> str:
    ecr = _client("ecr")
    try:
        repo = ecr.create_repository(
            repositoryName=ECR_REPO,
            imageScanningConfiguration={"scanOnPush": True},
            tags=TAGS,
        )["repository"]
        print(f"✓ created ECR repo {ECR_REPO} → {repo['repositoryUri']}")
    except ecr.exceptions.RepositoryAlreadyExistsException:
        repo = ecr.describe_repositories(repositoryNames=[ECR_REPO])["repositories"][0]
        print(f"✓ ECR repo {ECR_REPO} already exists → {repo['repositoryUri']}")
    return repo["repositoryUri"]


# ── main ─────────────────────────────────────────────────────────────────


def main() -> int:
    out: Dict[str, Any] = {"region": REGION}
    out["queue_url"] = ensure_queue()
    ensure_jobs_table()
    out["jobs_table"] = JOBS_TABLE
    ensure_failures_table()
    out["failures_table"] = FAILURES_TABLE
    out["ecr_uri"] = ensure_ecr_repo()
    print()
    print("Bootstrap complete. Worker env vars:")
    print(f"  SQS_QUEUE_URL={out['queue_url']}")
    print(f"  JOBS_TABLE_NAME={out['jobs_table']}")
    print(f"  FAILURES_TABLE_NAME={out['failures_table']}")
    print(f"  AWS_REGION={out['region']}")
    print(f"  ECR_URI={out['ecr_uri']}")
    print()
    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
