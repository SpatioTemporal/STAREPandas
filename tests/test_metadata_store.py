"""Tests for starepandas.metadata.

Pure-Python tests for ``PartitionRow`` and the ``MetadataStore`` protocol
shape; the live ``RDSMetadataStore`` behaviour is exercised by
``/stare-pods-verification`` (task 9), not unit tests.
"""

import datetime
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
