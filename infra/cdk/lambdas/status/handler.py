"""Status Lambda — ``GET /jobs/{id}``, ``GET /jobs/{id}/failures``,
``DELETE /jobs/{id}`` (Path C C-4 Part B).

* ``GET /jobs/{id}`` — read the job item from ``StarePodsJobs`` and return
  its current state + counters.
* ``GET /jobs/{id}/failures`` — query ``StarePodsFailures`` by ``job_id`` and
  return the per-granule outcome rows (paginated via ``?next=`` token).
* ``DELETE /jobs/{id}`` — 501 Not Implemented (cancellation deferred per
  §C11 Decision 4 / Decision 4 "no cancel in v1").

Routing is by the API Gateway proxy ``resource`` template + ``httpMethod``,
so one Lambda backs all three methods.
"""

from __future__ import annotations

import json
import logging
import os

import boto3
from boto3.dynamodb.types import TypeDeserializer

logger = logging.getLogger()
logger.setLevel(logging.INFO)

JOBS_TABLE = os.environ.get("JOBS_TABLE_NAME", "StarePodsJobs")
FAILURES_TABLE = os.environ.get("FAILURES_TABLE_NAME", "StarePodsFailures")
FAILURES_PAGE_SIZE = int(os.environ.get("FAILURES_PAGE_SIZE", "100"))

ddb = boto3.client("dynamodb")
_deser = TypeDeserializer()


def _response(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body, default=str),
    }


def _deserialize(item: dict) -> dict:
    return {k: _deser.deserialize(v) for k, v in item.items()}


def _get_job(job_id: str) -> dict:
    r = ddb.get_item(TableName=JOBS_TABLE, Key={"job_id": {"S": job_id}})
    item = r.get("Item")
    if not item:
        return _response(404, {"error": f"job {job_id} not found"})
    job = _deserialize(item)
    # Normalise the numeric fields (Decimal -> int) for a clean JSON shape.
    for k in ("total_granules", "ticket_count", "processed", "failed", "workers", "expires_at"):
        if k in job:
            job[k] = int(job[k])
    return _response(200, job)


def _get_failures(job_id: str, next_token: str | None) -> dict:
    kwargs = {
        "TableName": FAILURES_TABLE,
        "KeyConditionExpression": "job_id = :j",
        "ExpressionAttributeValues": {":j": {"S": job_id}},
        "Limit": FAILURES_PAGE_SIZE,
    }
    if next_token:
        try:
            kwargs["ExclusiveStartKey"] = json.loads(next_token)
        except Exception:  # noqa: BLE001
            return _response(400, {"error": "invalid 'next' pagination token"})
    r = ddb.query(**kwargs)
    rows = [_deserialize(it) for it in r.get("Items", [])]
    body = {"job_id": job_id, "count": len(rows), "failures": rows}
    if "LastEvaluatedKey" in r:
        body["next"] = json.dumps(r["LastEvaluatedKey"])
    return _response(200, body)


def handler(event, context):
    method = (event.get("httpMethod") or "").upper()
    resource = event.get("resource") or event.get("path") or ""
    params = event.get("pathParameters") or {}
    job_id = params.get("id")

    try:
        if method == "DELETE":
            # §C11 Decision 4 — cancellation deferred to v2.
            return _response(501, {
                "error": "job cancellation is not implemented in v1",
                "job_id": job_id,
            })

        if method != "GET":
            return _response(405, {"error": f"method {method} not allowed"})

        if not job_id:
            return _response(400, {"error": "missing job id in path"})

        if resource.endswith("/failures"):
            qs = event.get("queryStringParameters") or {}
            return _get_failures(job_id, qs.get("next"))

        return _get_job(job_id)

    except Exception as e:  # noqa: BLE001
        logger.exception("status error")
        return _response(500, {"error": f"internal error: {e}"})
