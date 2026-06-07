"""Completion-watcher Lambda — closes drained jobs (Path C C-5).

Fired every 30 s by the EventBridge rule ``starepods-completion-tick``. Scans
``StarePodsJobs`` for jobs still in ``state='running'``; for each job whose
per-granule counters have caught up to its size (``processed + failed ==
total_granules``) it performs the teardown the C-4 control plane left undone:

    1. Flip ``state`` → ``complete`` (if ``failed==0``) else ``failed``, stamp
       ``completed_at``.  CONDITIONAL on ``state='running'`` — the flip is the
       idempotency gate: only the invocation that wins the transition proceeds
       to steps 2-4, so ``active_jobs`` is never double-decremented even if two
       ticks overlap.
    2. Atomically ``ADD active_jobs -1`` on the ``JobsControl`` singleton,
       guarded so it never drops below zero.
    3. If ``active_jobs`` hit 0, scale the shared worker service to
       ``desiredCount=0`` (Decision 3 — shared workers; only the *last* job
       tears the fleet down).
    4. If the job carries a ``callback_url``, POST the terminal payload (3×
       exponential backoff); on exhaustion drop it on the callbacks DLQ.

Why a scan (not a query): ``StarePodsJobs`` has only PK ``job_id`` (no GSI on
``state``), so finding running jobs means a filtered ``Scan``. Fine at this
scale; add a GSI only if the table grows large.

The watcher runs OUTSIDE the VPC (§C10 #8) — it only talks to DynamoDB, ECS,
SQS, and the public-internet callback URL via their public IAM endpoints, so
it needs no NAT.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Config (env-driven; defaults match the CDK stack) ──────────────────────
JOBS_TABLE = os.environ.get("JOBS_TABLE_NAME", "StarePodsJobs")
ECS_CLUSTER = os.environ.get("ECS_CLUSTER", "starepods")
ECS_SERVICE = os.environ.get("ECS_SERVICE", "starepods-workers")
JOBS_CONTROL_ID = os.environ.get("JOBS_CONTROL_ID", "JobsControl")
CALLBACKS_DLQ_URL = os.environ.get("CALLBACKS_DLQ_URL", "")
CALLBACK_MAX_RETRIES = int(os.environ.get("CALLBACK_MAX_RETRIES", "3"))
CALLBACK_TIMEOUT_S = int(os.environ.get("CALLBACK_TIMEOUT_S", "5"))
TTL_DAYS = int(os.environ.get("TTL_DAYS", "30"))

ddb = boto3.client("dynamodb")
ecs = boto3.client("ecs")
sqs = boto3.client("sqs")


def _scan_running_jobs() -> list:
    """Return every ``StarePodsJobs`` item with ``state='running'``.

    ``state`` is a DynamoDB reserved word, so it goes through an
    ExpressionAttributeName. Pages through the whole table (small at this
    scale).
    """
    jobs = []
    kwargs = {
        "TableName": JOBS_TABLE,
        "FilterExpression": "#s = :running",
        "ExpressionAttributeNames": {"#s": "state"},
        "ExpressionAttributeValues": {":running": {"S": "running"}},
    }
    while True:
        r = ddb.scan(**kwargs)
        jobs.extend(r.get("Items", []))
        lek = r.get("LastEvaluatedKey")
        if not lek:
            break
        kwargs["ExclusiveStartKey"] = lek
    return jobs


def _n(item: dict, key: str, default: int = 0) -> int:
    v = item.get(key)
    return int(v["N"]) if v and "N" in v else default


def _try_finalize(job: dict) -> bool:
    """Flip a drained job to its terminal state.

    Returns True iff *this* invocation won the ``running → terminal``
    transition (the conditional update succeeded). A False return means the
    job either isn't drained yet or another tick already closed it — in both
    cases the caller must NOT decrement ``active_jobs``.
    """
    job_id = job["job_id"]["S"]
    total = _n(job, "total_granules")
    processed = _n(job, "processed")
    failed = _n(job, "failed")
    if processed + failed < total:
        return False  # not drained yet

    final_state = "complete" if failed == 0 else "failed"
    now = datetime.now(timezone.utc)
    expires_at = int((now + timedelta(days=TTL_DAYS)).timestamp())
    try:
        ddb.update_item(
            TableName=JOBS_TABLE,
            Key={"job_id": {"S": job_id}},
            UpdateExpression="SET #s = :final, completed_at = :ts, expires_at = :exp",
            ConditionExpression="#s = :running",  # idempotency gate
            ExpressionAttributeNames={"#s": "state"},
            ExpressionAttributeValues={
                ":final": {"S": final_state},
                ":ts": {"S": now.isoformat()},
                ":exp": {"N": str(expires_at)},
                ":running": {"S": "running"},
            },
        )
    except ddb.exceptions.ConditionalCheckFailedException:
        logger.info("job_id=%s already finalized by another tick; skipping", job_id)
        return False
    logger.info("finalized job_id=%s state=%s processed=%d failed=%d total=%d",
                job_id, final_state, processed, failed, total)
    return True


def _decrement_active_jobs() -> int:
    """Atomically ``ADD active_jobs -1``, never below 0. Returns the new value.

    The ``active_jobs > 0`` condition prevents the counter from going negative
    if state ever drifts; on a failed condition we treat the floor as 0.
    """
    try:
        r = ddb.update_item(
            TableName=JOBS_TABLE,
            Key={"job_id": {"S": JOBS_CONTROL_ID}},
            UpdateExpression="ADD active_jobs :neg",
            ConditionExpression="active_jobs > :zero",
            ExpressionAttributeValues={":neg": {"N": "-1"}, ":zero": {"N": "0"}},
            ReturnValues="UPDATED_NEW",
        )
        return int(r["Attributes"]["active_jobs"]["N"])
    except ddb.exceptions.ConditionalCheckFailedException:
        logger.warning("active_jobs already at floor; not decrementing below 0")
        return 0


def _scale_to_zero() -> None:
    try:
        ecs.update_service(cluster=ECS_CLUSTER, service=ECS_SERVICE, desiredCount=0)
        logger.info("scaled %s/%s to desiredCount=0 (last job drained)",
                    ECS_CLUSTER, ECS_SERVICE)
    except Exception:  # noqa: BLE001
        logger.exception("failed to scale service to 0")


def _post_callback(job_id: str, payload: dict) -> bool:
    """POST the terminal payload to the job's callback_url. Returns success.

    3× exponential backoff (1s, 2s, 4s). urllib avoids a Lambda dependency on
    requests.
    """
    url = payload["callback_url"]
    data = json.dumps(payload).encode()
    for attempt in range(1, CALLBACK_MAX_RETRIES + 1):
        try:
            req = urllib.request.Request(
                url, data=data, method="POST",
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=CALLBACK_TIMEOUT_S) as resp:
                if 200 <= resp.status < 300:
                    logger.info("callback delivered job_id=%s status=%d", job_id, resp.status)
                    return True
                logger.warning("callback job_id=%s attempt=%d non-2xx status=%d",
                               job_id, attempt, resp.status)
        except Exception as exc:  # noqa: BLE001 (urllib raises many error types)
            logger.warning("callback job_id=%s attempt=%d failed: %s", job_id, attempt, exc)
        if attempt < CALLBACK_MAX_RETRIES:
            time.sleep(2 ** (attempt - 1))
    return False


def _dlq_callback(job_id: str, payload: dict) -> None:
    if not CALLBACKS_DLQ_URL:
        logger.error("callback exhausted for job_id=%s but no DLQ configured", job_id)
        return
    try:
        sqs.send_message(QueueUrl=CALLBACKS_DLQ_URL, MessageBody=json.dumps(payload))
        logger.info("callback for job_id=%s sent to DLQ after exhausting retries", job_id)
    except Exception:  # noqa: BLE001
        logger.exception("failed to send exhausted callback to DLQ job_id=%s", job_id)


def _handle_callback(job: dict, final_state: str) -> None:
    cb = job.get("callback_url")
    if not cb:
        return
    payload = {
        "job_id": job["job_id"]["S"],
        "callback_url": cb["S"],
        "state": final_state,
        "total_granules": _n(job, "total_granules"),
        "processed": _n(job, "processed"),
        "failed": _n(job, "failed"),
    }
    if not _post_callback(payload["job_id"], payload):
        _dlq_callback(payload["job_id"], payload)


def handler(event, context):
    running = _scan_running_jobs()
    logger.info("watcher tick: %d running job(s)", len(running))
    closed = 0
    for job in running:
        if not _try_finalize(job):
            continue
        closed += 1
        final_state = "complete" if _n(job, "failed") == 0 else "failed"
        remaining = _decrement_active_jobs()
        if remaining == 0:
            _scale_to_zero()
        _handle_callback(job, final_state)
    return {"running": len(running), "closed": closed}
