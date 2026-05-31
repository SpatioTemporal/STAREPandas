#!/usr/bin/env python3
"""End-to-end integration test for the C-2 worker.

Drives the real worker module (``starepandas.cloud.worker.Worker``) against
the real test resources bootstrapped by ``bootstrap_test_resources.py``:

  1. Pre-clean SQS + DDB state for the test job_id.
  2. Send one hand-crafted ticket with one local GMI granule.
  3. Run the worker in-process; assert clean idle exit.
  4. Assert DDB jobs counter == 1, one progress row in failures table.
  5. Re-enqueue the same ticket → re-run worker.
  6. Assert counter is STILL 1 (§C10 #3 idempotency).
  7. Tear down test state.

Run::

    conda run -n starepandas_3.12_v3 python infra/scripts/integration_test_c2.py
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from pathlib import Path

# Pin .config so the library + boto3 share creds.
_PKG_CFG = Path(__file__).resolve().parents[2] / "starepandas" / ".config"
os.environ.setdefault("STAREPANDAS_AWS_CONFIG", str(_PKG_CFG))

import boto3  # noqa: E402

import starepandas  # noqa: E402,F401
from starepandas import staredataframe as _sdf  # noqa: E402
from starepandas.cloud.worker import Worker, WorkerConfig  # noqa: E402

_sdf._load_config_from_default_locations()

REGION = "us-west-2"
QUEUE_URL = "https://sqs.us-west-2.amazonaws.com/637388276731/starepods-tickets-test"
JOBS_TABLE = "StarePodsJobs-test"
FAILURES_TABLE = "StarePodsFailures-test"
GRANULE = (
    "/Users/thatdaihaiton/Workspace/STARE/L1C_Data_Samples/GPM/2025/Jan_1_2/"
    "1C.GPM.GMI.XCAL2016-C.20250101-S034347-E051659.061567.V07B.HDF5"
)
S3_OUTPUT_PREFIX = "s3://zarrpods/storage-c2-test"
JOB_ID = f"c2-test-{uuid.uuid4().hex[:8]}"
WORK_DIR = "/tmp/starepods-worker-test"


def _client(service: str):
    opts = _sdf._AWS_S3_STORAGE_OPTIONS
    return boto3.client(
        service,
        region_name=REGION,
        aws_access_key_id=opts.get("key"),
        aws_secret_access_key=opts.get("secret"),
    )


def _send_ticket(sqs, granule_uris):
    body = json.dumps({
        "job_id": JOB_ID,
        "instrument": "GMI",
        "s3_prefix": S3_OUTPUT_PREFIX,
        "granule_uris": granule_uris,
        "options": {"scan": "S1"},  # one scan keeps the test fast (~60-90s)
    })
    sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=body)


def _purge_sqs(sqs):
    """Drain the queue. Skips PurgeQueue (60-s cooldown) when the queue
    is already empty; otherwise tries PurgeQueue, falling back to a
    visibility-0 drain if the cooldown is active."""
    depth = int(
        sqs.get_queue_attributes(
            QueueUrl=QUEUE_URL,
            AttributeNames=["ApproximateNumberOfMessages"],
        )["Attributes"]["ApproximateNumberOfMessages"]
    )
    if depth == 0:
        return
    try:
        sqs.purge_queue(QueueUrl=QUEUE_URL)
        return
    except sqs.exceptions.PurgeQueueInProgress:
        pass
    # Fallback: receive + delete with a tiny visibility window. Tolerates
    # the purge cooldown without blocking the test for 60 seconds.
    for _ in range(50):
        msgs = sqs.receive_message(
            QueueUrl=QUEUE_URL, MaxNumberOfMessages=10, WaitTimeSeconds=0,
            VisibilityTimeout=1,
        ).get("Messages", [])
        if not msgs:
            return
        for m in msgs:
            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=m["ReceiptHandle"])


def _read_processed(ddb) -> int:
    resp = ddb.get_item(TableName=JOBS_TABLE, Key={"job_id": {"S": JOB_ID}})
    item = resp.get("Item")
    if not item:
        return 0
    return int(item.get("processed", {}).get("N", "0"))


def _read_failures(ddb) -> list:
    resp = ddb.query(
        TableName=FAILURES_TABLE,
        KeyConditionExpression="job_id = :j",
        ExpressionAttributeValues={":j": {"S": JOB_ID}},
    )
    return resp.get("Items", [])


def _run_worker(sqs, ddb) -> int:
    cfg = WorkerConfig(
        queue_url=QUEUE_URL,
        jobs_table=JOBS_TABLE,
        failures_table=FAILURES_TABLE,
        region=REGION,
        idle_threshold=2,
        poll_wait=5,
        work_dir=WORK_DIR,
    )
    return Worker(cfg, sqs=sqs, ddb=ddb).run()


def main() -> int:
    import logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    log = logging.getLogger("c2_integration")

    if not Path(GRANULE).exists():
        log.error("granule not found at %s", GRANULE)
        return 2

    sqs = _client("sqs")
    ddb = _client("dynamodb")

    log.info("test job_id=%s", JOB_ID)

    # ---------- Phase 0 — clean baseline ----------
    log.info("phase 0: purge SQS + DDB state for this job")
    _purge_sqs(sqs)

    # ---------- Phase 1 — first delivery ----------
    log.info("phase 1: enqueue ticket with 1 granule")
    _send_ticket(sqs, [GRANULE])

    log.info("phase 1: run worker (expect process + idle-exit)")
    t0 = time.time()
    rc = _run_worker(sqs, ddb)
    log.info("worker exit code=%d after %.1fs", rc, time.time() - t0)
    assert rc == 0, f"worker rc={rc}"

    processed = _read_processed(ddb)
    failures = _read_failures(ddb)
    log.info("phase 1 result: processed=%d failure_rows=%d", processed, len(failures))
    assert processed == 1, f"expected processed=1, got {processed}"
    assert len(failures) == 1, f"expected 1 failure-row, got {len(failures)}"
    assert failures[0]["state"]["S"] == "processed", f"expected state=processed, got {failures[0]}"

    # ---------- Phase 2 — redelivery (§C10 #3 idempotency) ----------
    log.info("phase 2: re-enqueue same ticket → counter must NOT increment")
    _send_ticket(sqs, [GRANULE])

    t0 = time.time()
    rc2 = _run_worker(sqs, ddb)
    log.info("worker exit code=%d after %.1fs", rc2, time.time() - t0)
    assert rc2 == 0

    processed2 = _read_processed(ddb)
    failures2 = _read_failures(ddb)
    log.info("phase 2 result: processed=%d failure_rows=%d", processed2, len(failures2))
    assert processed2 == 1, f"§C10 #3 idempotency broken: processed went {processed} → {processed2}"
    assert len(failures2) == 1, f"failures table grew: {len(failures)} → {len(failures2)}"

    # ---------- Phase 3 — teardown ----------
    log.info("phase 3: purge SQS (DDB rows isolated by per-run UUID — left in place)")
    try:
        _purge_sqs(sqs)
    except Exception as e:
        log.warning("teardown purge failed (non-fatal): %s", e)

    log.info("✓ integration test PASS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
