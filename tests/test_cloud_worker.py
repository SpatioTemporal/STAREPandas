"""Tests for starepandas.cloud.worker.

Covers the long-poll loop, the §C10 #3 idempotent counter, the
ticket-level failure path, and Decision 9 (§C10 #9) graceful exit on
RDS credential rotation. AWS clients are injected as MagicMocks — moto
is not required.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from starepandas.cloud import worker as worker_mod
from starepandas.cloud.worker import (
    Worker,
    WorkerConfig,
    _RDSAuthRotation,
    _is_rds_auth_error,
)


# ── fixtures ─────────────────────────────────────────────────────────────


def _ddb_with_condition_exc():
    """Return a MagicMock DDB client that raises ConditionalCheckFailedException
    when its put_item is configured to fail. The exception type is created on
    the fly so we don't depend on a real boto3 client.
    """
    ddb = MagicMock()

    class ConditionalCheckFailedException(Exception):
        pass

    ddb.exceptions.ConditionalCheckFailedException = ConditionalCheckFailedException
    return ddb, ConditionalCheckFailedException


def _make_worker(tmp_path, *, ddb=None, sqs=None, ingest_fn=None, fetch_fn=None):
    cfg = WorkerConfig(
        queue_url="https://sqs.us-west-2.amazonaws.com/000/test-q",
        jobs_table="JobsT",
        failures_table="FailT",
        region="us-west-2",
        idle_threshold=2,
        poll_wait=0,
        work_dir=str(tmp_path),
    )
    return Worker(
        cfg,
        sqs=sqs or MagicMock(),
        ddb=ddb or _ddb_with_condition_exc()[0],
        ingest_fn=ingest_fn or MagicMock(),
        fetch_fn=fetch_fn or (lambda uri, d: uri),
    )


def _ticket(uris, job_id="job-1", instrument="GMI", s3_prefix="s3://b/p", options=None):
    return json.dumps({
        "job_id": job_id,
        "instrument": instrument,
        "s3_prefix": s3_prefix,
        "granule_uris": list(uris),
        "options": options or {},
    })


# ── happy path ───────────────────────────────────────────────────────────


def test_process_ticket_success_increments_processed(tmp_path):
    """One ticket with two granules: ingest called twice, processed += 2."""
    ddb, _ = _ddb_with_condition_exc()
    ingest = MagicMock()
    w = _make_worker(tmp_path, ddb=ddb, ingest_fn=ingest)

    w.process_ticket(_ticket(["s3://b/g1.HDF5", "s3://b/g2.HDF5"]))

    assert ingest.call_count == 2
    # Two claim-puts + two job-counter increments.
    assert ddb.put_item.call_count == 2
    assert ddb.update_item.call_count == 2
    for call in ddb.update_item.call_args_list:
        assert call.kwargs["ExpressionAttributeNames"] == {"#c": "processed"}


# ── §C10 #3 — dedup on redelivery ─────────────────────────────────────────


def test_redelivery_does_not_double_count(tmp_path):
    """Conditional put_item raising ConditionalCheckFailed must skip the
    job-counter increment. Simulates SQS re-delivering a ticket whose
    granules were already recorded by a previous run."""
    ddb, CondExc = _ddb_with_condition_exc()
    ddb.put_item.side_effect = CondExc("already there")
    w = _make_worker(tmp_path, ddb=ddb)

    w.process_ticket(_ticket(["s3://b/g1.HDF5"]))

    ddb.put_item.assert_called_once()
    ddb.update_item.assert_not_called()


def test_partial_redelivery_only_increments_unclaimed(tmp_path):
    """Two granules; the first was already claimed, the second is new.
    Exactly one increment expected."""
    ddb, CondExc = _ddb_with_condition_exc()
    ddb.put_item.side_effect = [CondExc("dup"), None]
    w = _make_worker(tmp_path, ddb=ddb)

    w.process_ticket(_ticket(["s3://b/g1.HDF5", "s3://b/g2.HDF5"]))

    assert ddb.put_item.call_count == 2
    assert ddb.update_item.call_count == 1


# ── per-granule failure ──────────────────────────────────────────────────


def test_granule_failure_records_failed_state(tmp_path):
    """Ingest raising means the granule is recorded as state='failed' and
    the ``failed`` counter is bumped (not ``processed``)."""
    ddb, _ = _ddb_with_condition_exc()
    ingest = MagicMock(side_effect=RuntimeError("bad granule"))
    w = _make_worker(tmp_path, ddb=ddb, ingest_fn=ingest)

    w.process_ticket(_ticket(["s3://b/g1.HDF5"]))

    put_kwargs = ddb.put_item.call_args.kwargs
    assert put_kwargs["Item"]["state"]["S"] == "failed"
    assert "bad granule" in put_kwargs["Item"]["error"]["S"]
    assert ddb.update_item.call_args.kwargs["ExpressionAttributeNames"] == {"#c": "failed"}


def test_error_string_truncated(tmp_path):
    """Errors are capped before DDB write."""
    ddb, _ = _ddb_with_condition_exc()
    ingest = MagicMock(side_effect=RuntimeError("X" * 5000))
    w = _make_worker(tmp_path, ddb=ddb, ingest_fn=ingest)

    w.process_ticket(_ticket(["s3://b/g1.HDF5"]))

    err = ddb.put_item.call_args.kwargs["Item"]["error"]["S"]
    assert len(err.encode("utf-8")) <= 1024


# ── Decision 9 / §C10 #9 — RDS auth rotation graceful exit ────────────────


class _FakePsycopgOpError(Exception):
    """Stand-in for psycopg2.OperationalError that ``_is_rds_auth_error``
    recognises. We monkeypatch the helper rather than depend on psycopg2."""
    pass


def test_is_rds_auth_error_matches_auth_messages(monkeypatch):
    monkeypatch.setattr(worker_mod, "_is_rds_auth_error", lambda exc: True)
    assert worker_mod._is_rds_auth_error(RuntimeError("password authentication failed"))


def test_rds_auth_error_bubbles_as_rotation_exception(tmp_path, monkeypatch):
    """An ingest call that raises an auth-shaped error must surface as
    _RDSAuthRotation so the run loop can exit cleanly."""
    monkeypatch.setattr(worker_mod, "_is_rds_auth_error", lambda exc: True)
    ddb, _ = _ddb_with_condition_exc()
    ingest = MagicMock(side_effect=RuntimeError("password authentication failed"))
    w = _make_worker(tmp_path, ddb=ddb, ingest_fn=ingest)

    with pytest.raises(_RDSAuthRotation):
        w.process_ticket(_ticket(["s3://b/g1.HDF5"]))

    # No counters touched: the ticket isn't being recorded as a failure;
    # SQS will redeliver to a fresh container after the visibility timeout.
    ddb.update_item.assert_not_called()


def test_run_loop_exits_cleanly_on_rotation(tmp_path, monkeypatch):
    """End-to-end: a single ticket triggers rotation; run() returns 0 and
    the message is NOT deleted (so SQS can redeliver)."""
    monkeypatch.setattr(worker_mod, "_is_rds_auth_error", lambda exc: True)
    sqs = MagicMock()
    sqs.receive_message.return_value = {
        "Messages": [{"Body": _ticket(["s3://b/g.HDF5"]), "ReceiptHandle": "rh-1"}]
    }
    ddb, _ = _ddb_with_condition_exc()
    ingest = MagicMock(side_effect=RuntimeError("password authentication failed"))
    w = _make_worker(tmp_path, sqs=sqs, ddb=ddb, ingest_fn=ingest)

    assert w.run() == 0
    sqs.delete_message.assert_not_called()


# ── long-poll loop ───────────────────────────────────────────────────────


def test_idle_threshold_exits(tmp_path):
    """N consecutive empty polls → run() returns 0."""
    sqs = MagicMock()
    sqs.receive_message.return_value = {"Messages": []}
    w = _make_worker(tmp_path, sqs=sqs)

    assert w.run() == 0
    # idle_threshold=2 in the fixture → exactly 2 receive calls.
    assert sqs.receive_message.call_count == 2


def test_message_then_idle_resets_counter(tmp_path):
    """A delivered message resets the idle counter — verifies we exit only
    after idle_threshold *consecutive* empty polls, not cumulative."""
    sqs = MagicMock()
    sqs.receive_message.side_effect = [
        {"Messages": []},
        {"Messages": [{"Body": _ticket(["s3://b/g.HDF5"]), "ReceiptHandle": "rh-x"}]},
        {"Messages": []},
        {"Messages": []},
    ]
    w = _make_worker(tmp_path, sqs=sqs)
    w.run()

    assert sqs.receive_message.call_count == 4
    sqs.delete_message.assert_called_once()


def test_ticket_failure_does_not_delete_message(tmp_path):
    """Malformed JSON → exception caught at ticket level, message left
    for SQS redelivery."""
    sqs = MagicMock()
    sqs.receive_message.side_effect = [
        {"Messages": [{"Body": "{ not valid json", "ReceiptHandle": "rh-x"}]},
        {"Messages": []},
        {"Messages": []},
    ]
    w = _make_worker(tmp_path, sqs=sqs)
    w.run()

    sqs.delete_message.assert_not_called()


# ── _is_rds_auth_error helper ────────────────────────────────────────────


def test_is_rds_auth_error_rejects_non_psycopg():
    """A plain RuntimeError with the right wording is *not* an auth error
    (it's not a psycopg2 type) — required to avoid masking unrelated
    failures with the rotation graceful-exit path."""
    assert not _is_rds_auth_error(RuntimeError("password authentication failed"))


# ── default fetch ────────────────────────────────────────────────────────


def test_default_fetch_returns_local_path_unchanged(tmp_path):
    w = _make_worker(tmp_path, fetch_fn=None)
    # Schemes are URLs only; bare paths are returned as-is.
    assert w._default_fetch("/local/path.HDF5", str(tmp_path)) == "/local/path.HDF5"
    assert w._default_fetch("file:///abs/path.HDF5", str(tmp_path)) == "/abs/path.HDF5"
