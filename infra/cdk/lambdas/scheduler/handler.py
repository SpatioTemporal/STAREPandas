"""Scheduler Lambda — ``POST /ingest`` (Path C C-4 Part B).

Accepts an ingest job, validates it, splits the granule URIs into tickets,
enqueues them on the ``starepods-tickets`` SQS queue, bumps the shared-worker
``JobsControl.active_jobs`` counter, scales the ECS worker service up, and
returns ``202`` with the ``job_id``.

State machine (§C10 #4 — enqueue-then-running, crash-safe under the
shared-workers model):

    1. put_item(state='enqueued', total_granules=N, processed=0, failed=0)
    2. split granule URIs into tickets (§C2 sizing)
    3. send_message x M tickets (each tagged with job_id for routing)
    4. update_item(JobsControl, ADD active_jobs 1)        # atomic
    5. update_item(state='running')                        # only after 1-4
    6. update_service(desiredCount=max(W, currentDesired)) # never scale DOWN

If the Lambda dies before step 5, the job is left in 'enqueued' — the
completion watcher (C-5) ignores non-'running' jobs, so a half-enqueued job
never looks done. Re-submission is a fresh job_id; the worker's per-granule
conditional write (§C10 #3) + RDS ON CONFLICT (§C10 #1) absorb any duplicate
tickets idempotently.

Design notes:
* raw_collected_time is *validated* per granule here (fail-fast at submit
  time if a filename matches no known pattern) but NOT threaded into the
  ticket — the worker's io.granules.to_s3 remains the single source of truth
  for the stamped timestamp (§C10 #2).
* Large URI lists: API Gateway/Lambda cap the request body at ~6 MB. For
  bigger jobs the client uploads the URI list to S3 and passes a pointer
  (`granule_uris_s3`); the scheduler fetches + parses it.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone

import boto3

from common.ticket_sizing import split_into_tickets, DEFAULT_MAX_TICKET_SIZE
from common.timestamps import derive_timestamp_from_path, CannotDeriveTimestampError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Config (env-driven; defaults match the CDK stack) ──────────────────────
QUEUE_URL = os.environ["TICKETS_QUEUE_URL"]
JOBS_TABLE = os.environ.get("JOBS_TABLE_NAME", "StarePodsJobs")
ECS_CLUSTER = os.environ.get("ECS_CLUSTER", "starepods")
ECS_SERVICE = os.environ.get("ECS_SERVICE", "starepods-workers")
WORKER_CAP = int(os.environ.get("WORKER_CAP", "4"))           # §C9 v1 cost cap
MAX_TICKET_SIZE = int(os.environ.get("MAX_TICKET_SIZE", str(DEFAULT_MAX_TICKET_SIZE)))
TTL_DAYS = int(os.environ.get("TTL_DAYS", "30"))
JOBS_CONTROL_ID = os.environ.get("JOBS_CONTROL_ID", "JobsControl")
# Soft cap above which an inline URI list should instead be an S3 pointer.
INLINE_BODY_SOFT_LIMIT = int(os.environ.get("INLINE_BODY_SOFT_LIMIT", str(5 * 1024 * 1024)))

sqs = boto3.client("sqs")
ddb = boto3.client("dynamodb")
ecs = boto3.client("ecs")
s3 = boto3.client("s3")


class BadRequest(Exception):
    """400 — client error with a user-facing message."""


def _response(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


def _parse_s3_uri(uri: str):
    if not uri.startswith("s3://"):
        raise BadRequest(f"granule_uris_s3 must be an s3:// URI, got {uri!r}")
    rest = uri[len("s3://"):]
    bucket, _, key = rest.partition("/")
    if not bucket or not key:
        raise BadRequest(f"malformed s3 pointer {uri!r}")
    return bucket, key


def _load_granule_uris(payload: dict) -> list:
    """Return the granule URI list, fetching from S3 if a pointer was given."""
    if "granule_uris_s3" in payload:
        bucket, key = _parse_s3_uri(payload["granule_uris_s3"])
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            data = json.loads(obj["Body"].read())
        except Exception as exc:  # noqa: BLE001
            raise BadRequest(f"could not read granule list from pointer: {exc}")
        uris = data.get("granule_uris") if isinstance(data, dict) else data
    else:
        uris = payload.get("granule_uris")
    if not isinstance(uris, list) or not uris:
        raise BadRequest("granule_uris must be a non-empty list (or use granule_uris_s3)")
    if not all(isinstance(u, str) for u in uris):
        raise BadRequest("every granule URI must be a string")
    return uris


def _validate(payload: dict):
    """Validate the request and return (uris, instrument, s3_prefix, workers, options, callback_url)."""
    instrument = payload.get("instrument")
    if not instrument or not isinstance(instrument, str):
        raise BadRequest("instrument is required (e.g. 'GMI')")

    workers = payload.get("workers", WORKER_CAP)
    if not isinstance(workers, int) or workers < 1:
        raise BadRequest("workers must be a positive integer")
    if workers > WORKER_CAP:
        # §C9 cost cap — standard message (do not change wording; tests assert it).
        raise BadRequest(
            f"workers={workers} exceeds the v1 cap of {WORKER_CAP} — "
            f"please re-submit with workers={WORKER_CAP} (or omit the parameter)."
        )

    uris = _load_granule_uris(payload)

    # Fail-fast: every filename must yield a derivable collection timestamp,
    # else the worker would fail this granule one-by-one (§C10 #2).
    bad = []
    for u in uris:
        try:
            derive_timestamp_from_path(u)
        except CannotDeriveTimestampError:
            bad.append(u)
    if bad:
        raise BadRequest(
            f"{len(bad)} granule URI(s) have filenames with no recognised "
            f"timestamp pattern (e.g. {bad[0]}). Fix the names or add a pattern."
        )

    s3_prefix = payload.get("s3_prefix")  # optional; worker falls back to default
    options = payload.get("options") or {}
    if not isinstance(options, dict):
        raise BadRequest("options must be an object")
    callback_url = payload.get("callback_url")  # optional; used by C-5 watcher
    return uris, instrument, s3_prefix, workers, options, callback_url


def handler(event, context):
    try:
        raw = event.get("body") or "{}"
        if event.get("isBase64Encoded"):
            import base64
            raw = base64.b64decode(raw).decode()
        if len(raw) > INLINE_BODY_SOFT_LIMIT and "granule_uris_s3" not in (json.loads(raw) or {}):
            raise BadRequest(
                "request body exceeds the inline limit; upload the URI list to "
                "S3 and pass 'granule_uris_s3' instead of an inline 'granule_uris'."
            )
        payload = json.loads(raw)
        if not isinstance(payload, dict):
            raise BadRequest("request body must be a JSON object")

        uris, instrument, s3_prefix, workers, options, callback_url = _validate(payload)

        job_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        expires_at = int((now + timedelta(days=TTL_DAYS)).timestamp())
        tickets = split_into_tickets(uris, workers, MAX_TICKET_SIZE)

        # 1) Persist 'enqueued' before touching SQS — crash-safe.
        item = {
            "job_id": {"S": job_id},
            "state": {"S": "enqueued"},
            "total_granules": {"N": str(len(uris))},
            "ticket_count": {"N": str(len(tickets))},
            "processed": {"N": "0"},
            "failed": {"N": "0"},
            "workers": {"N": str(workers)},
            "instrument": {"S": instrument},
            "created_at": {"S": now.isoformat()},
            "expires_at": {"N": str(expires_at)},
        }
        if s3_prefix:
            item["s3_prefix"] = {"S": s3_prefix}
        if callback_url:
            item["callback_url"] = {"S": callback_url}
        ddb.put_item(TableName=JOBS_TABLE, Item=item)

        # 2-3) Enqueue tickets, each routable by job_id.
        for tk in tickets:
            body = {"job_id": job_id, "instrument": instrument, "granule_uris": tk}
            if s3_prefix:
                body["s3_prefix"] = s3_prefix
            if options:
                body["options"] = options
            sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(body))

        # 4) Atomic increment of the shared-worker active-job counter.
        ddb.update_item(
            TableName=JOBS_TABLE,
            Key={"job_id": {"S": JOBS_CONTROL_ID}},
            UpdateExpression="ADD active_jobs :one",
            ExpressionAttributeValues={":one": {"N": "1"}},
        )

        # 5) Flip to 'running' only after tickets are on the queue + counter bumped.
        ddb.update_item(
            TableName=JOBS_TABLE,
            Key={"job_id": {"S": job_id}},
            UpdateExpression="SET #s = :running",
            ExpressionAttributeNames={"#s": "state"},
            ExpressionAttributeValues={":running": {"S": "running"}},
        )

        # 6) Scale workers UP only (another job may already want more).
        try:
            cur = ecs.describe_services(cluster=ECS_CLUSTER, services=[ECS_SERVICE])
            current_desired = cur["services"][0]["desiredCount"] if cur["services"] else 0
        except Exception:  # noqa: BLE001
            current_desired = 0
        target = max(workers, current_desired)
        if target > current_desired:
            ecs.update_service(cluster=ECS_CLUSTER, service=ECS_SERVICE, desiredCount=target)

        logger.info("scheduled job_id=%s granules=%d tickets=%d workers=%d desired=%d",
                    job_id, len(uris), len(tickets), workers, target)
        return _response(202, {
            "job_id": job_id,
            "state": "running",
            "total_granules": len(uris),
            "ticket_count": len(tickets),
            "workers": workers,
        })

    except BadRequest as e:
        logger.warning("bad request: %s", e)
        return _response(400, {"error": str(e)})
    except Exception as e:  # noqa: BLE001
        logger.exception("scheduler error")
        return _response(500, {"error": f"internal error: {e}"})
