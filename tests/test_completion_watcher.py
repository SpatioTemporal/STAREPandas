"""Unit tests for the C-5 completion-watcher Lambda.

Exercise the handler offline with mocked boto3 clients — no AWS. They lock in
the teardown logic the live DoD also checks (running→complete flip, active_jobs
decrement, scale-to-0 only when the last job drains, callback retry→DLQ) plus
the idempotency gate that keeps overlapping ticks from double-decrementing.

Like the control-plane tests, the handler builds boto3 clients at import time,
so we set a region + env vars *before* importing, then patch the module-level
client objects per test.
"""

import importlib
import os
import sys
from unittest import mock

import pytest

_LAMBDAS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "infra", "cdk", "lambdas",
)
if _LAMBDAS_DIR not in sys.path:
    sys.path.insert(0, _LAMBDAS_DIR)

os.environ.setdefault("AWS_DEFAULT_REGION", "us-west-2")
os.environ.setdefault("JOBS_TABLE_NAME", "StarePodsJobs")
os.environ.setdefault("ECS_CLUSTER", "starepods")
os.environ.setdefault("ECS_SERVICE", "starepods-workers")
os.environ.setdefault("CALLBACKS_DLQ_URL", "https://sqs.us-west-2.amazonaws.com/0/starepods-callbacks-dlq")

watcher = importlib.import_module("watcher.handler")


class _CondFail(Exception):
    """Stand-in for botocore's ConditionalCheckFailedException."""


def _wire_ddb(ddb):
    """Give the mocked ddb client a real exception class so `except` works."""
    ddb.exceptions.ConditionalCheckFailedException = _CondFail
    return ddb


def _job(job_id="j1", state="running", total=2, processed=2, failed=0, callback_url=None):
    item = {
        "job_id": {"S": job_id},
        "state": {"S": state},
        "total_granules": {"N": str(total)},
        "processed": {"N": str(processed)},
        "failed": {"N": str(failed)},
    }
    if callback_url:
        item["callback_url"] = {"S": callback_url}
    return item


# ── _scan_running_jobs ─────────────────────────────────────────────────────

def test_scan_paginates():
    with mock.patch.object(watcher, "ddb") as ddb:
        ddb.scan.side_effect = [
            {"Items": [_job("a")], "LastEvaluatedKey": {"job_id": {"S": "a"}}},
            {"Items": [_job("b")]},
        ]
        jobs = watcher._scan_running_jobs()
    assert [j["job_id"]["S"] for j in jobs] == ["a", "b"]
    assert ddb.scan.call_count == 2
    # state is a reserved word → must go through an ExpressionAttributeName.
    _, kwargs = ddb.scan.call_args_list[0]
    assert kwargs["ExpressionAttributeNames"] == {"#s": "state"}


# ── _try_finalize ──────────────────────────────────────────────────────────

def test_finalize_skips_undrained_job():
    with mock.patch.object(watcher, "ddb") as ddb:
        assert watcher._try_finalize(_job(total=5, processed=2, failed=0)) is False
        ddb.update_item.assert_not_called()


def test_finalize_complete_when_no_failures():
    with mock.patch.object(watcher, "ddb") as ddb:
        _wire_ddb(ddb)
        assert watcher._try_finalize(_job(total=2, processed=2, failed=0)) is True
        _, kwargs = ddb.update_item.call_args
        assert kwargs["ExpressionAttributeValues"][":final"]["S"] == "complete"
        # Conditional on still-running → idempotency gate.
        assert kwargs["ConditionExpression"] == "#s = :running"
        # Terminal write stamps the TTL gap shut.
        assert ":exp" in kwargs["ExpressionAttributeValues"]


def test_finalize_failed_when_any_failure():
    with mock.patch.object(watcher, "ddb") as ddb:
        _wire_ddb(ddb)
        assert watcher._try_finalize(_job(total=3, processed=2, failed=1)) is True
        _, kwargs = ddb.update_item.call_args
        assert kwargs["ExpressionAttributeValues"][":final"]["S"] == "failed"


def test_finalize_returns_false_when_already_closed():
    """Overlapping tick: the conditional update fails → don't proceed."""
    with mock.patch.object(watcher, "ddb") as ddb:
        _wire_ddb(ddb)
        ddb.update_item.side_effect = _CondFail()
        assert watcher._try_finalize(_job(total=2, processed=2, failed=0)) is False


# ── _decrement_active_jobs ─────────────────────────────────────────────────

def test_decrement_returns_new_value():
    with mock.patch.object(watcher, "ddb") as ddb:
        _wire_ddb(ddb)
        ddb.update_item.return_value = {"Attributes": {"active_jobs": {"N": "2"}}}
        assert watcher._decrement_active_jobs() == 2


def test_decrement_floors_at_zero():
    with mock.patch.object(watcher, "ddb") as ddb:
        _wire_ddb(ddb)
        ddb.update_item.side_effect = _CondFail()
        assert watcher._decrement_active_jobs() == 0


# ── handler() end-to-end ───────────────────────────────────────────────────

def test_handler_scales_to_zero_on_last_job():
    with mock.patch.object(watcher, "ddb") as ddb, \
         mock.patch.object(watcher, "ecs") as ecs:
        _wire_ddb(ddb)
        ddb.scan.return_value = {"Items": [_job(total=1, processed=1, failed=0)]}
        ddb.update_item.return_value = {"Attributes": {"active_jobs": {"N": "0"}}}
        out = watcher.handler({}, None)
    assert out == {"running": 1, "closed": 1}
    ecs.update_service.assert_called_once()
    assert ecs.update_service.call_args.kwargs["desiredCount"] == 0


def test_handler_does_not_scale_when_jobs_remain():
    with mock.patch.object(watcher, "ddb") as ddb, \
         mock.patch.object(watcher, "ecs") as ecs:
        _wire_ddb(ddb)
        ddb.scan.return_value = {"Items": [_job(total=1, processed=1, failed=0)]}
        ddb.update_item.return_value = {"Attributes": {"active_jobs": {"N": "1"}}}
        watcher.handler({}, None)
    ecs.update_service.assert_not_called()


def test_handler_skips_undrained_without_decrementing():
    with mock.patch.object(watcher, "ddb") as ddb, \
         mock.patch.object(watcher, "ecs") as ecs:
        _wire_ddb(ddb)
        ddb.scan.return_value = {"Items": [_job(total=5, processed=2, failed=0)]}
        out = watcher.handler({}, None)
    assert out == {"running": 1, "closed": 0}
    ddb.update_item.assert_not_called()  # neither finalize nor decrement
    ecs.update_service.assert_not_called()


# ── callbacks ──────────────────────────────────────────────────────────────

def test_callback_success_no_dlq():
    job = _job(total=1, processed=1, failed=0, callback_url="https://example.com/cb")
    with mock.patch.object(watcher, "ddb") as ddb, \
         mock.patch.object(watcher, "ecs"), \
         mock.patch.object(watcher, "sqs") as sqs, \
         mock.patch.object(watcher, "_post_callback", return_value=True) as post:
        _wire_ddb(ddb)
        ddb.scan.return_value = {"Items": [job]}
        ddb.update_item.return_value = {"Attributes": {"active_jobs": {"N": "0"}}}
        watcher.handler({}, None)
    post.assert_called_once()
    sqs.send_message.assert_not_called()


def test_callback_exhausted_goes_to_dlq():
    job = _job(total=1, processed=1, failed=0, callback_url="https://example.com/cb")
    with mock.patch.object(watcher, "ddb") as ddb, \
         mock.patch.object(watcher, "ecs"), \
         mock.patch.object(watcher, "sqs") as sqs, \
         mock.patch.object(watcher, "_post_callback", return_value=False):
        _wire_ddb(ddb)
        ddb.scan.return_value = {"Items": [job]}
        ddb.update_item.return_value = {"Attributes": {"active_jobs": {"N": "0"}}}
        watcher.handler({}, None)
    sqs.send_message.assert_called_once()
    assert sqs.send_message.call_args.kwargs["QueueUrl"].endswith("starepods-callbacks-dlq")


def test_no_callback_when_url_absent():
    with mock.patch.object(watcher, "ddb") as ddb, \
         mock.patch.object(watcher, "ecs"), \
         mock.patch.object(watcher, "sqs") as sqs, \
         mock.patch.object(watcher, "_post_callback") as post:
        _wire_ddb(ddb)
        ddb.scan.return_value = {"Items": [_job(total=1, processed=1, failed=0)]}
        ddb.update_item.return_value = {"Attributes": {"active_jobs": {"N": "0"}}}
        watcher.handler({}, None)
    post.assert_not_called()
    sqs.send_message.assert_not_called()
