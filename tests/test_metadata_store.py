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
