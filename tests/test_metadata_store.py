"""Tests for starepandas.metadata.

Pure-Python tests for ``PartitionRow`` and the ``MetadataStore`` protocol
shape; the live ``RDSMetadataStore`` behaviour is exercised by
``/stare-pods-verification`` (task 9), not unit tests.
"""

import datetime

import pytest
import json

from starepandas.metadata import (
    MetadataStore,
    PartitionRow,
    RDSMetadataStore,
)


def test_partition_row_roundtrips_to_insert_tuple():
    row = PartitionRow(
        dataset="GMI_S1",
        raw_collected_time=datetime.datetime(2024, 1, 15, 0, 0, 0),
        grouped_id=1234567890123,
        s3_bucket="zarrpods",
        resolution_level=10,
        metadata_json={"group_path": "s3://zarrpods/foo/bar.parquet", "num_rows": 250000},
        data_level="L1C",
    )

    tup = row.as_insert_tuple()
    assert tup[0] == "GMI_S1"                       # Dataset
    assert tup[1] == "L1C"                          # DataLevel
    assert tup[2] == datetime.datetime(2024, 1, 15) # RawData Collected Time
    assert tup[3] == 1234567890123                  # grouped_id
    assert tup[4] == "zarrpods"                     # S3 bucket
    assert tup[5] == 10                             # Resolution level
    assert json.loads(tup[6]) == row.metadata_json  # MetadataJson


def test_partition_row_data_level_defaults_to_none():
    row = PartitionRow(
        dataset="AMSR2",
        raw_collected_time=datetime.datetime(2024, 6, 1),
        grouped_id=42,
        s3_bucket="zarrpods",
        resolution_level=10,
        metadata_json={},
    )
    assert row.data_level is None
    assert row.as_insert_tuple()[1] is None


def test_rds_store_satisfies_protocol():
    """``RDSMetadataStore`` must structurally satisfy ``MetadataStore``."""
    store = RDSMetadataStore()                       # no conn → lazy
    assert isinstance(store, MetadataStore)
    # Don't touch the network: just confirm the methods exist.
    assert callable(store.write_partitions)
    assert callable(store.find)
    assert callable(store.delete_by_prefix)


def test_rds_store_write_partitions_empty_input_short_circuits():
    """Empty input must return 0 without opening a connection."""
    store = RDSMetadataStore()
    # If this tried to connect it would fail in a test environment.
    assert store.write_partitions([]) == 0


# ----- Tenacity retry behaviour -------------------------------------------


def _row():
    return PartitionRow(
        dataset="GMI_S1",
        raw_collected_time=datetime.datetime(2024, 1, 15),
        grouped_id=1,
        s3_bucket="zarrpods",
        resolution_level=10,
        metadata_json={"group_path": "s3://x/y.parquet"},
    )


def _install_fast_batch_insert(monkeypatch, inner):
    """Replace RDSMetadataStore._batch_insert_with_retry with a freshly
    decorated wrapper that uses ``wait_fixed(0)`` instead of exponential
    backoff. Tenacity decorators bake in their wait policy at decoration
    time, so monkey-patching .wait on the live decorator is fiddly —
    redecorating ``inner`` is cleaner."""
    from tenacity import retry, retry_if_exception, stop_after_attempt, wait_fixed
    from starepandas.metadata import _is_transient_db_error

    fast = retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(0),
        retry=retry_if_exception(_is_transient_db_error),
        reraise=True,
    )(inner)
    monkeypatch.setattr(RDSMetadataStore, "_batch_insert_with_retry", fast)


def test_transient_error_triggers_retry_then_fallback(monkeypatch):
    """An OperationalError on every batch attempt exhausts retries,
    then write_partitions falls back to _write_one_by_one."""
    import psycopg2

    store = RDSMetadataStore()
    store._conn = object()  # bypass _get_conn — never actually used

    attempts = []
    def raising_batch(self, tuples):
        attempts.append(1)
        raise psycopg2.OperationalError("connection reset by peer")

    fallback_calls = []
    def fake_fallback(self, tuples):
        fallback_calls.append(len(tuples))
        return len(tuples)

    _install_fast_batch_insert(monkeypatch, raising_batch)
    monkeypatch.setattr(RDSMetadataStore, "_write_one_by_one", fake_fallback)

    before = RDSMetadataStore.rds_write_failures
    result = store.write_partitions([_row()])

    assert len(attempts) == 3, f"expected 3 retry attempts, got {len(attempts)}"
    assert fallback_calls == [1]
    assert result == 1
    assert RDSMetadataStore.rds_write_failures == before + 1


def test_non_transient_error_skips_retry_then_fallback(monkeypatch):
    """An IntegrityError (UNIQUE violation etc.) is not retried — it
    falls through to row-by-row on the first attempt."""
    import psycopg2

    store = RDSMetadataStore()
    store._conn = object()

    attempts = []
    def raising_batch(self, tuples):
        attempts.append(1)
        raise psycopg2.IntegrityError("duplicate key value violates unique constraint")

    fallback_calls = []
    def fake_fallback(self, tuples):
        fallback_calls.append(len(tuples))
        return len(tuples)

    _install_fast_batch_insert(monkeypatch, raising_batch)
    monkeypatch.setattr(RDSMetadataStore, "_write_one_by_one", fake_fallback)

    before = RDSMetadataStore.rds_write_failures
    result = store.write_partitions([_row()])

    assert len(attempts) == 1, f"expected single attempt for non-transient error, got {len(attempts)}"
    assert fallback_calls == [1]
    assert result == 1
    assert RDSMetadataStore.rds_write_failures == before + 1


def test_retry_config_matches_spec():
    """Sanity-check the @retry decorator parameters on write_partitions's
    batch helper. Catches accidental changes to attempt count / predicate."""
    deco = RDSMetadataStore._batch_insert_with_retry.retry
    # tenacity exposes .stop, .wait, .retry on the decorated function.
    assert hasattr(deco, 'stop')
    assert hasattr(deco, 'retry')
    # stop_after_attempt(3) → max_attempt_number = 3
    assert getattr(deco.stop, 'max_attempt_number', None) == 3, \
        f"expected 3 attempts, got {deco.stop}"


# ----- ON CONFLICT DO UPDATE (§C10 #1) ------------------------------------


def test_insert_sql_uses_on_conflict_do_update():
    """The batch INSERT must end with ON CONFLICT DO UPDATE so retries are
    idempotent against the pods_unique constraint (§C10 #1)."""
    from starepandas.metadata import _INSERT_SQL
    assert 'ON CONFLICT' in _INSERT_SQL
    assert '"Dataset", "RawData Collected Time", grouped_id' in _INSERT_SQL
    assert 'DO UPDATE' in _INSERT_SQL
    assert 'EXCLUDED."MetadataJson"' in _INSERT_SQL


def test_insert_one_sql_also_uses_on_conflict():
    """The row-by-row fallback must also be idempotent — it's reached when
    the batch dies mid-transaction, so partial-success retries hit the
    same UNIQUE constraint."""
    from starepandas.metadata import _INSERT_ONE_SQL
    assert 'ON CONFLICT' in _INSERT_ONE_SQL
    assert 'DO UPDATE' in _INSERT_ONE_SQL


def test_on_conflict_targets_constraint_columns_in_order():
    """The ON CONFLICT target must list exactly the columns of the
    pods_unique constraint in the same order, otherwise PostgreSQL won't
    recognise the inference and will raise."""
    from starepandas.metadata import _ON_CONFLICT_CLAUSE
    assert '("Dataset", "RawData Collected Time", grouped_id)' in _ON_CONFLICT_CLAUSE


# ----- temporal range + pod code (temporal-stare-pods issue 01) -------------


def test_partition_row_carries_temporal_range_and_podcode():
    """The widened row appends t_start, t_end, podcode to the insert tuple,
    in the same order as the INSERT column list."""
    row = PartitionRow(
        dataset="GMI_S1",
        raw_collected_time=datetime.datetime(2024, 1, 15),
        grouped_id=1234567890123,
        s3_bucket="zarrpods",
        resolution_level=4,
        metadata_json={},
        data_level="L1C",
        t_start=datetime.datetime(2024, 1, 15, 0, 1, 30),
        t_end=datetime.datetime(2024, 1, 15, 0, 3, 45),
        podcode="q132110",
    )
    tup = row.as_insert_tuple()
    assert len(tup) == 10
    assert tup[7] == datetime.datetime(2024, 1, 15, 0, 1, 30)  # t_start
    assert tup[8] == datetime.datetime(2024, 1, 15, 0, 3, 45)  # t_end
    assert tup[9] == "q132110"                                  # podcode
    assert tup[7] <= tup[8]


def test_partition_row_temporal_fields_default_to_null():
    """A chunk with no usable timestamps writes with an empty range."""
    row = PartitionRow(
        dataset="GMI_S1",
        raw_collected_time=datetime.datetime(2024, 1, 15),
        grouped_id=1,
        s3_bucket="zarrpods",
        resolution_level=4,
        metadata_json={},
    )
    tup = row.as_insert_tuple()
    assert len(tup) == 10
    assert tup[7] is None and tup[8] is None and tup[9] is None


def test_insert_sql_carries_temporal_columns():
    from starepandas.metadata import _INSERT_ONE_SQL, _INSERT_SQL
    for sql in (_INSERT_SQL, _INSERT_ONE_SQL):
        assert 't_start, t_end, podcode' in sql


def test_on_conflict_refreshes_temporal_range_and_podcode():
    """Re-ingest must land the recomputed range and pod code on the
    existing row (PRD: 'the on-conflict path refreshes all three')."""
    from starepandas.metadata import _ON_CONFLICT_CLAUSE
    for col in ('t_start', 't_end', 'podcode'):
        assert f'{col} = EXCLUDED.{col}' in _ON_CONFLICT_CLAUSE


# ----- 2026-09-07: a row-by-row shortfall is an error, not a log line -------


def test_row_by_row_shortfall_raises_metadata_write_error(monkeypatch):
    """Batch fails on a dead connection, every row of the fallback fails
    too → MetadataWriteError (was: return 0 and let the caller print
    'Inserted 0 metadata rows' — the orphan-chunk bug of the Q1 bulk run)."""
    import psycopg2
    from starepandas.metadata import MetadataWriteError

    store = RDSMetadataStore()
    store._conn = object()

    def raising_batch(self, tuples):
        raise psycopg2.OperationalError("server closed the connection unexpectedly")

    def partial_fallback(self, tuples):
        self.last_write_error = psycopg2.InterfaceError("connection already closed")
        return 0

    _install_fast_batch_insert(monkeypatch, raising_batch)
    monkeypatch.setattr(RDSMetadataStore, "_write_one_by_one", partial_fallback)

    with pytest.raises(MetadataWriteError) as info:
        store.write_partitions([_row(), _row()])
    err = info.value
    assert (err.requested, err.inserted) == (2, 0)
    assert isinstance(err.last_error, psycopg2.InterfaceError)
    assert "0 of 2" in str(err) and "connection already closed" in str(err)
    assert isinstance(err.__cause__, psycopg2.OperationalError)


def test_write_one_by_one_tracks_last_error():
    """The fallback records the last per-row exception (and clears it on a
    clean pass) so the shortfall error can name the cause."""
    class _Cur:
        def __init__(self, fail): self.fail = fail
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, sql, tup):
            if self.fail:
                raise RuntimeError("row rejected")

    class _Conn:
        def __init__(self, fail_every_other):
            self.calls = 0
            self.fail_every_other = fail_every_other
        def cursor(self):
            self.calls += 1
            return _Cur(self.fail_every_other and self.calls % 2 == 0)
        def commit(self): pass
        def rollback(self): pass

    store = RDSMetadataStore(conn=_Conn(fail_every_other=True))
    tuples = [_row().as_insert_tuple() for _ in range(4)]
    assert store._write_one_by_one(tuples) == 2
    assert isinstance(store.last_write_error, RuntimeError)

    store = RDSMetadataStore(conn=_Conn(fail_every_other=False))
    assert store._write_one_by_one(tuples) == 4
    assert store.last_write_error is None
