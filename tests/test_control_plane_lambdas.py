"""Unit tests for the C-4 Part B control-plane Lambdas (scheduler + status).

These exercise the handler logic offline with mocked boto3 clients — no AWS.
They complement the live DoD (curl POST /ingest etc.) by locking in:

* parity between the vendored helpers and the library source of truth,
* request validation + the §C9 cost cap (workers <= 4),
* the §C10 #4 enqueue-then-running state-machine call order,
* status routing (GET / DELETE-501 / failures).

The Lambda handlers build boto3 clients at import time (a common Lambda
pattern), so we set a region + the required env vars *before* importing,
then patch the module-level client objects for the handler() tests.
"""

import importlib
import json
import os
import sys
from unittest import mock

import pytest

# Make the Lambda source importable (infra/cdk/lambdas).
_LAMBDAS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "infra", "cdk", "lambdas",
)
if _LAMBDAS_DIR not in sys.path:
    sys.path.insert(0, _LAMBDAS_DIR)

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")
os.environ.setdefault("TICKETS_QUEUE_URL", "https://sqs.us-west-2.amazonaws.com/0/starepods-tickets")
os.environ.setdefault("JOBS_TABLE_NAME", "StarePodsJobs")
os.environ.setdefault("FAILURES_TABLE_NAME", "StarePodsFailures")
os.environ.setdefault("ECS_CLUSTER", "starepods")
os.environ.setdefault("ECS_SERVICE", "starepods-workers")
os.environ.setdefault("WORKER_CAP", "4")

scheduler = importlib.import_module("scheduler.handler")
status = importlib.import_module("status.handler")

GMI = ("s3://zarrpods/x/"
       "1C.GPM.GMI.XCAL2016-C.20250101-S034347-E051659.061567.V07B.HDF5")


# ── Vendored-helper parity ────────────────────────────────────────────────

def test_ticket_sizing_parity():
    from starepandas.cloud.ticket_sizing import split_into_tickets as lib
    from common.ticket_sizing import split_into_tickets as vend
    for n in (0, 1, 40, 41, 100, 160, 161, 500):
        uris = [f"u{i}" for i in range(n)]
        for w in (1, 4):
            assert vend(uris, w, 40) == lib(uris, w, 40), (n, w)


def test_timestamp_parity():
    from starepandas.io.granules._timestamps import derive_timestamp_from_path as lib
    from common.timestamps import derive_timestamp_from_path as vend
    assert vend(GMI) == lib(GMI)


# ── Scheduler validation ──────────────────────────────────────────────────

def test_cost_cap_rejects_too_many_workers():
    with pytest.raises(scheduler.BadRequest) as e:
        scheduler._validate({"instrument": "GMI", "workers": 8, "granule_uris": [GMI]})
    msg = str(e.value)
    assert "exceeds the v1 cap of 4" in msg and "workers=4" in msg


def test_missing_instrument_rejected():
    with pytest.raises(scheduler.BadRequest):
        scheduler._validate({"granule_uris": [GMI]})


def test_empty_granule_list_rejected():
    with pytest.raises(scheduler.BadRequest):
        scheduler._validate({"instrument": "GMI", "granule_uris": []})


def test_undatable_filename_rejected():
    with pytest.raises(scheduler.BadRequest) as e:
        scheduler._validate({"instrument": "GMI", "granule_uris": ["s3://b/not-a-granule.bin"]})
    assert "timestamp pattern" in str(e.value)


def test_valid_request_defaults_workers_to_cap():
    uris, instr, prefix, workers, options, cb = scheduler._validate(
        {"instrument": "GMI", "granule_uris": [GMI], "s3_prefix": "s3://zarrpods/storage"}
    )
    assert instr == "GMI" and workers == 4 and uris == [GMI]
    assert prefix == "s3://zarrpods/storage"


# ── Scheduler handler() — state machine + responses ───────────────────────

def _api_event(body: dict) -> dict:
    return {"body": json.dumps(body), "httpMethod": "POST", "resource": "/ingest"}


def test_handler_happy_path_runs_state_machine_in_order():
    calls = []
    with mock.patch.object(scheduler, "ddb") as ddb, \
         mock.patch.object(scheduler, "sqs") as sqs, \
         mock.patch.object(scheduler, "ecs") as ecs:
        ddb.put_item.side_effect = lambda **k: calls.append("put")
        sqs.send_message.side_effect = lambda **k: calls.append("send")
        ddb.update_item.side_effect = lambda **k: calls.append(
            "active_jobs" if k["Key"]["job_id"]["S"] == "JobsControl" else "running"
        )
        ecs.describe_services.return_value = {"services": [{"desiredCount": 0}]}
        ecs.update_service.side_effect = lambda **k: calls.append("scale")

        resp = scheduler.handler(_api_event(
            {"instrument": "GMI", "granule_uris": [GMI], "s3_prefix": "s3://zarrpods/storage"}
        ), None)

    assert resp["statusCode"] == 202
    body = json.loads(resp["body"])
    assert body["state"] == "running" and body["ticket_count"] == 1 and body["workers"] == 4
    # §C10 #4 order: enqueued put -> tickets -> active_jobs bump -> running -> scale
    assert calls == ["put", "send", "active_jobs", "running", "scale"]


def test_handler_cost_cap_returns_400_without_touching_aws():
    with mock.patch.object(scheduler, "ddb") as ddb, \
         mock.patch.object(scheduler, "sqs") as sqs, \
         mock.patch.object(scheduler, "ecs") as ecs:
        resp = scheduler.handler(_api_event(
            {"instrument": "GMI", "workers": 8, "granule_uris": [GMI]}
        ), None)
        assert resp["statusCode"] == 400
        ddb.put_item.assert_not_called()
        sqs.send_message.assert_not_called()
        ecs.update_service.assert_not_called()


def test_handler_does_not_scale_down():
    """If the service already wants more workers than this job, don't shrink it."""
    with mock.patch.object(scheduler, "ddb"), \
         mock.patch.object(scheduler, "sqs"), \
         mock.patch.object(scheduler, "ecs") as ecs:
        ecs.describe_services.return_value = {"services": [{"desiredCount": 4}]}
        scheduler.handler(_api_event(
            {"instrument": "GMI", "workers": 2, "granule_uris": [GMI]}
        ), None)
        ecs.update_service.assert_not_called()  # max(2, 4) == 4 == current → no change


# ── Status handler routing ────────────────────────────────────────────────

def test_status_delete_returns_501():
    resp = status.handler(
        {"httpMethod": "DELETE", "resource": "/jobs/{id}", "pathParameters": {"id": "j1"}}, None
    )
    assert resp["statusCode"] == 501


def test_status_get_missing_job_returns_404():
    with mock.patch.object(status, "ddb") as ddb:
        ddb.get_item.return_value = {}  # no Item
        resp = status.handler(
            {"httpMethod": "GET", "resource": "/jobs/{id}", "pathParameters": {"id": "nope"}}, None
        )
    assert resp["statusCode"] == 404


def test_status_get_found_returns_200_with_int_counters():
    item = {
        "job_id": {"S": "j1"}, "state": {"S": "running"},
        "total_granules": {"N": "10"}, "processed": {"N": "3"}, "failed": {"N": "0"},
        "workers": {"N": "4"},
    }
    with mock.patch.object(status, "ddb") as ddb:
        ddb.get_item.return_value = {"Item": item}
        resp = status.handler(
            {"httpMethod": "GET", "resource": "/jobs/{id}", "pathParameters": {"id": "j1"}}, None
        )
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["state"] == "running" and body["processed"] == 3 and body["total_granules"] == 10


def test_status_failures_query_returns_rows():
    with mock.patch.object(status, "ddb") as ddb:
        ddb.query.return_value = {
            "Items": [
                {"job_id": {"S": "j1"}, "granule_uri": {"S": "s3://b/g1"},
                 "state": {"S": "failed"}, "error": {"S": "boom"}},
            ]
        }
        resp = status.handler(
            {"httpMethod": "GET", "resource": "/jobs/{id}/failures",
             "pathParameters": {"id": "j1"}, "queryStringParameters": None}, None
        )
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["count"] == 1 and body["failures"][0]["error"] == "boom"
