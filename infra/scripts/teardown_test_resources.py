#!/usr/bin/env python3
"""Tear down the C-2 test AWS resources created by ``bootstrap_test_resources.py``.

Keeps the ECR repository by default (image push takes a few minutes; we
usually want to keep it across iterations). Pass ``--all`` to delete it too.

Usage::

    conda run -n starepandas_3.12_v3 python infra/scripts/teardown_test_resources.py
    conda run -n starepandas_3.12_v3 python infra/scripts/teardown_test_resources.py --all
"""

from __future__ import annotations

import argparse
import sys

import boto3
from botocore.exceptions import ClientError

import os
from pathlib import Path

_PKG_CFG = Path(__file__).resolve().parents[2] / "starepandas" / ".config"
if _PKG_CFG.exists() and not os.environ.get("STAREPANDAS_AWS_CONFIG"):
    os.environ["STAREPANDAS_AWS_CONFIG"] = str(_PKG_CFG)

import starepandas  # noqa: E402,F401 — side-effect: loads .config
from starepandas.staredataframe import _load_config_from_default_locations  # noqa: E402

_load_config_from_default_locations()

REGION = "us-west-2"
QUEUE_NAME = "starepods-tickets-test"
JOBS_TABLE = "StarePodsJobs-test"
FAILURES_TABLE = "StarePodsFailures-test"
ECR_REPO = "starepods/worker"


def _client(service: str):
    """boto3 client wired to the same .config creds the library uses."""
    from starepandas.staredataframe import _AWS_S3_STORAGE_OPTIONS as opts
    region = (opts.get("client_kwargs") or {}).get("region_name") or REGION
    return boto3.client(
        service,
        region_name=region,
        aws_access_key_id=opts.get("key"),
        aws_secret_access_key=opts.get("secret"),
        aws_session_token=opts.get("token"),
    )


def delete_queue() -> None:
    sqs = _client("sqs")
    try:
        url = sqs.get_queue_url(QueueName=QUEUE_NAME)["QueueUrl"]
        sqs.delete_queue(QueueUrl=url)
        print(f"✓ deleted queue {QUEUE_NAME}")
    except sqs.exceptions.QueueDoesNotExist:
        print(f"· queue {QUEUE_NAME} absent")


def delete_table(name: str) -> None:
    ddb = _client("dynamodb")
    try:
        ddb.delete_table(TableName=name)
        ddb.get_waiter("table_not_exists").wait(TableName=name)
        print(f"✓ deleted table {name}")
    except ddb.exceptions.ResourceNotFoundException:
        print(f"· table {name} absent")


def delete_ecr_repo() -> None:
    ecr = _client("ecr")
    try:
        ecr.delete_repository(repositoryName=ECR_REPO, force=True)
        print(f"✓ deleted ECR repo {ECR_REPO}")
    except ecr.exceptions.RepositoryNotFoundException:
        print(f"· ECR repo {ECR_REPO} absent")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--all",
        action="store_true",
        help="Also delete the ECR repository (default: keep it).",
    )
    args = p.parse_args()

    delete_queue()
    delete_table(JOBS_TABLE)
    delete_table(FAILURES_TABLE)
    if args.all:
        delete_ecr_repo()
    return 0


if __name__ == "__main__":
    sys.exit(main())
