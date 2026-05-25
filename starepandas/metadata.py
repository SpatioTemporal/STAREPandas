"""Partition-metadata store abstraction.

§C9 M4 hedge: today's RDS catalogue is the only backend, but every write,
read, and delete that touches ``PodsMetadata`` goes through this interface
so a future swap to DynamoDB (§3 M3) is a localised change.

The protocol exposes three operations:

* ``write_partitions(rows)`` — bulk INSERT of partition rows
  (used by ``STAREDataFrame.to_s3``)
* ``find(**filters)`` — query partition metadata
  (used by ``starepandas.io.granules.load_s3_metadata`` and downstream
  ``find_intersecting_data``)
* ``delete_by_prefix(s3_prefix)`` — remove rows whose ``group_path`` is
  under a prefix (used by ``StarePodsDemo.clean_s3_prefix``)

For C-1 the implementation wraps the existing behaviour verbatim — no
schema or query changes. Subsequent tasks layer tenacity retries
(task 4), per-granule timestamps (task 5), and ``ON CONFLICT DO UPDATE``
(task 6) on top.
"""

from __future__ import annotations

import datetime
import json
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Protocol, runtime_checkable

import pandas as pd


@dataclass(frozen=True)
class PartitionRow:
    """One row of ``PodsMetadata``.

    Mirrors the column layout the existing INSERT in
    ``staredataframe.py:1847-1887`` produces; the conversion to the tuple
    psycopg2 expects lives in :meth:`as_insert_tuple` so the SQL stays in
    one place.
    """

    dataset: str
    raw_collected_time: datetime.datetime
    grouped_id: int
    s3_bucket: str
    resolution_level: int
    metadata_json: dict
    data_level: Optional[str] = None

    def as_insert_tuple(self) -> tuple:
        return (
            self.dataset,
            self.data_level,
            self.raw_collected_time,
            self.grouped_id,
            self.s3_bucket,
            self.resolution_level,
            json.dumps(self.metadata_json),
        )


@runtime_checkable
class MetadataStore(Protocol):
    """Catalog of partition metadata.

    Implementations: :class:`RDSMetadataStore` (today). A
    ``DynamoDBMetadataStore`` would slot in here unchanged when the §C9
    triggers fire (sustained write QPS > 5k, RDS dominates cost, or a
    single-database operational surface becomes a goal).
    """

    def write_partitions(self, rows: Iterable[PartitionRow]) -> int:
        """Insert partition rows. Returns count inserted."""
        ...

    def find(self, **filters: Any) -> pd.DataFrame:
        """Query partition metadata with optional filters.

        Supported keyword filters: ``dataset``, ``dataset_prefix``,
        ``data_level``, ``s3_bucket``, ``resolution_level``, ``start_date``,
        ``end_date``, ``grouped_id``, ``limit``, ``order_by``.
        """
        ...

    def delete_by_prefix(self, s3_prefix: str) -> int:
        """Delete rows whose ``MetadataJson.group_path`` starts with the
        prefix. Returns count deleted.
        """
        ...


_INSERT_SQL = (
    'INSERT INTO "PodsMetadata" '
    '("Dataset", "DataLevel", "RawData Collected Time", grouped_id, '
    '"S3 bucket", "Resolution level", "MetadataJson") '
    'VALUES %s'
)
_INSERT_ONE_SQL = (
    'INSERT INTO "PodsMetadata" '
    '("Dataset", "DataLevel", "RawData Collected Time", grouped_id, '
    '"S3 bucket", "Resolution level", "MetadataJson") '
    'VALUES (%s, %s, %s, %s, %s, %s, %s)'
)


class RDSMetadataStore:
    """PostgreSQL/RDS implementation backing ``PodsMetadata``."""

    def __init__(self, conn=None, db_name: str = "StarePodsMetadata"):
        """If ``conn`` is given, the caller owns its lifecycle. Otherwise
        the store opens one lazily and closes it in :meth:`close`."""
        self._conn = conn
        self._db_name = db_name
        self._owns_conn = conn is None

    def _get_conn(self):
        if self._conn is None:
            from starepandas.staredataframe import _ensure_rds_db_and_table
            self._conn = _ensure_rds_db_and_table(self._db_name)
        return self._conn

    def close(self) -> None:
        if self._owns_conn and self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def write_partitions(self, rows: Iterable[PartitionRow]) -> int:
        tuples = [r.as_insert_tuple() for r in rows]
        if not tuples:
            return 0

        conn = self._get_conn()
        try:
            from psycopg2.extras import execute_values
            with conn.cursor() as cur:
                execute_values(cur, _INSERT_SQL, tuples)
            conn.commit()
            return len(tuples)
        except Exception as e:
            print(f"Warning: Batch insert failed ({e}), falling back to row-by-row insert...")
            try:
                conn.rollback()
            except Exception:
                pass
            return self._write_one_by_one(tuples)

    def _write_one_by_one(self, tuples: list[tuple]) -> int:
        """Fallback for batch failure — preserves current behaviour."""
        conn = self._get_conn()
        inserted = 0
        for tup in tuples:
            try:
                with conn.cursor() as cur:
                    cur.execute(_INSERT_ONE_SQL, tup)
                conn.commit()
                inserted += 1
            except Exception:
                try:
                    conn.rollback()
                except Exception:
                    pass
        return inserted

    def find(self, **filters: Any) -> pd.DataFrame:
        # Defer to the existing query builder so the SQL stays in one place.
        from starepandas.io.granules import load_s3_metadata
        return load_s3_metadata(**filters)

    def delete_by_prefix(self, s3_prefix: str) -> int:
        normalized = s3_prefix.rstrip('/')
        like_pattern = f"{normalized}/%"
        conn = self._get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    'DELETE FROM "PodsMetadata" '
                    'WHERE "MetadataJson"->>%s LIKE %s',
                    ('group_path', like_pattern),
                )
                deleted = cur.rowcount or 0
            conn.commit()
            return deleted
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
