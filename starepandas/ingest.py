#!/usr/bin/env python3
"""STARE-PODS ingestion API (task 7 extraction from ``demo_lib``).

Module-level functions for ingesting granule files into Parquet partitions
either on the local filesystem (``ingest_granules_local``) or on S3 with an
RDS catalog (``ingest_granules_s3``). Also exposes ``clean_s3_prefix`` so
the same S3+RDS cleanup logic is reachable without instantiating a demo class.

These are the entry points the future Path C cloud worker imports — see
``stare_pods_aws_parallel_plan.html`` §C0 (the shared Layer-2 entrypoint
between local-S3 mode and cloud-service mode). ``StarePodsDemo`` and
``LocalStarePodsDemo`` in ``demo_lib.py`` are now thin shims that delegate
here so existing notebooks keep working unchanged.
"""

from __future__ import annotations

import glob as _glob
import logging
import os
from typing import Any, Dict, List, Optional

import starepandas

logger = logging.getLogger(__name__)


def _glob_granules(data_path: str) -> List[str]:
    """Resolve ``data_path`` to a concrete list of granule file paths.

    Supports: a single file, a directory (recursively globs ``**/*.HDF5``),
    or a glob pattern. Returns an empty list when nothing matches.
    """
    if os.path.isdir(data_path):
        return _glob.glob(f"{data_path}/**/*.HDF5", recursive=True)
    if '*' in data_path or '?' in data_path:
        return _glob.glob(data_path)
    return [data_path] if os.path.exists(data_path) else []


# ── S3 prefix cleanup ────────────────────────────────────────────────────────


def clean_s3_prefix(s3_prefix: str) -> Dict[str, int]:
    """Remove every S3 object under ``s3_prefix`` and every RDS
    ``PodsMetadata`` row whose ``MetadataJson.group_path`` starts with it.

    Mirrors the local pipeline's ``CLEAN_BEFORE_RUN`` behaviour for the
    S3+RDS path. Idempotent (§C10 #1) ingest makes this unnecessary in
    normal operation, but it's useful when wiping a known-bad batch
    before re-running.

    Parameters
    ----------
    s3_prefix : str
        S3 URI prefix to clean (e.g. ``"s3://zarrpods/storage"``).
        Matched as a string prefix against ``group_path`` values, so
        every subkey under it is also removed.

    Returns
    -------
    dict
        ``{'rds_rows_deleted': int, 's3_objects_deleted': int}``
    """
    import s3fs
    from starepandas.staredataframe import (
        _AWS_S3_STORAGE_OPTIONS, _load_config_from_default_locations,
    )
    from starepandas.metadata import RDSMetadataStore

    merged_opts = dict(_AWS_S3_STORAGE_OPTIONS)
    if not merged_opts:
        _load_config_from_default_locations()
        merged_opts = dict(starepandas.staredataframe._AWS_S3_STORAGE_OPTIONS)

    normalized_prefix = s3_prefix.rstrip('/')

    # 1) RDS cleanup — routed through MetadataStore so the SQL stays in
    #    one place (§C9 M4 hedge).
    store = RDSMetadataStore()
    try:
        rds_deleted = store.delete_by_prefix(normalized_prefix)
    finally:
        store.close()

    # 2) S3 cleanup — recursively remove objects under the prefix.
    s3_deleted = 0
    try:
        fs = s3fs.S3FileSystem(**merged_opts) if merged_opts else s3fs.S3FileSystem()
        bare = normalized_prefix[len('s3://'):] if normalized_prefix.startswith('s3://') else normalized_prefix
        if fs.exists(bare):
            try:
                s3_deleted = sum(1 for _ in fs.find(bare))
            except Exception:
                s3_deleted = -1
            fs.rm(bare, recursive=True)
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.warning(f"S3 cleanup of {s3_prefix} encountered: {e}")

    logger.info(
        f"clean_s3_prefix({s3_prefix}): "
        f"deleted {rds_deleted} RDS row(s), {s3_deleted} S3 object(s)"
    )
    return {'rds_rows_deleted': rds_deleted, 's3_objects_deleted': s3_deleted}


# ── Ingest into S3 + RDS ─────────────────────────────────────────────────────


def ingest_granules_s3(
    data_path: str,
    instrument: str,
    s3_prefix: Optional[str] = None,
    scan: Optional[str] = None,
    level: int = 10,
    clean_before_run: bool = False,
    **kwargs: Any,
) -> List[str]:
    """Partition granules into Parquet files on S3 and record metadata in RDS.

    Final S3 layout — flat quaternary pod-code keys (the local pipeline uses
    the hierarchical variant; see :func:`ingest_granules_local`)::

        <s3_prefix>/<podcode>-<granule_basename>-<dataset>.parquet

    Parameters
    ----------
    data_path : str
        Path to a single granule file, a directory, or a glob pattern.
    instrument : str
        Instrument / dataset name (e.g. ``"GMI"``, ``"AMSR2"``, ``"SSMIS"``,
        ``"ATMS"``).
    s3_prefix : str, optional
        S3 root for storage (e.g. ``"s3://zarrpods/storage"``). When omitted,
        falls back to ``default_s3_prefix`` from ``.config`` (task 12).
    scan : str, optional
        Specific scan to process (``"S1"``, ``"S2"``, etc.). ``None``
        processes all scans the granule reader exposes.
    level : int, optional
        STARE partitioning level (default 10). Actual partition level is
        capped to ``MAX_PARTITION_LEVEL`` inside ``to_s3``.
    clean_before_run : bool, optional
        If True, call :func:`clean_s3_prefix` on ``s3_prefix`` first.
        Requires ``s3_prefix`` to be set explicitly (refuses to wipe the
        default prefix).
    **kwargs
        Forwarded to :func:`starepandas.io.granules.to_s3`.

    Returns
    -------
    list of str
        S3 paths returned by each per-granule ``to_s3`` call.
    """
    if clean_before_run:
        if s3_prefix is None:
            raise ValueError(
                "clean_before_run=True requires s3_prefix to be set "
                "explicitly (refusing to wipe the entire default_s3_prefix)."
            )
        logger.info(f"clean_before_run=True → wiping {s3_prefix} on S3 + RDS first")
        clean_s3_prefix(s3_prefix)

    logger.info(f"Ingesting {instrument} granules from {data_path}")
    granule_files = _glob_granules(data_path)
    if not granule_files:
        logger.warning(f"No granule files found in {data_path}")
        return []
    logger.info(f"Found {len(granule_files)} {instrument} file(s)")

    s3_paths: List[str] = []
    for granule_file in granule_files:
        try:
            logger.info(f"Processing {os.path.basename(granule_file)}")
            kwargs.setdefault('read_timestamp', True)
            s3_result = starepandas.io.granules.to_s3(
                file_path=granule_file,
                s3_path=s3_prefix,   # None → default_s3_prefix from .config
                level=level,
                dataset=instrument,
                scan=scan,
                **kwargs,
            )
            if isinstance(s3_result, list):
                s3_paths.extend(s3_result)
            else:
                s3_paths.append(s3_result)
            logger.info(f"✓ Stored {os.path.basename(granule_file)} → {s3_result}")
        except Exception as e:
            logger.error(f"✗ Failed to process {granule_file}: {e}")
            continue

    logger.info(f"Ingested {len(s3_paths)} Parquet dataset(s)")
    return s3_paths


# ── Ingest into local filesystem + SQLite ────────────────────────────────────


def ingest_granules_local(
    data_path: str,
    instrument: str,
    local_root: str = "/tmp/stare_pods_local",
    scan: Optional[str] = None,
    level: int = 10,
    db_path: Optional[str] = None,
    **kwargs: Any,
) -> List[str]:
    """Partition granules into Parquet files on the local filesystem and
    record metadata in a SQLite database. No AWS, no RDS.

    Layout — hierarchical quaternary pod-code dir tree with a self-describing
    leaf (S3 uses the flat variant; see :func:`ingest_granules_s3`)::

        <local_root>/q13/q132/q1321/q13211/q132110/q132110-<granule_basename>-<dataset>.parquet

    Parameters
    ----------
    data_path : str
        Path to a single granule file, a directory, or a glob pattern.
    instrument : str
        Instrument / dataset name (e.g. ``"GMI"``).
    local_root : str, optional
        Root directory for Parquet storage. Created if absent. Default
        ``"/tmp/stare_pods_local"``.
    scan : str, optional
        Specific scan to process (``"S1"``, etc.). ``None`` processes all.
    level : int, optional
        STARE partitioning level (default 10).
    db_path : str, optional
        Path to the SQLite metadata DB. Default
        ``<local_root>/metadata.db``.
    **kwargs
        Forwarded to :func:`starepandas.io.granules.to_local`.

    Returns
    -------
    list of str
        Local paths written for each processed granule.
    """
    local_root = os.path.abspath(local_root)
    os.makedirs(local_root, exist_ok=True)
    if db_path is None:
        db_path = os.path.join(local_root, "metadata.db")

    granule_files = _glob_granules(data_path)
    if not granule_files:
        logger.warning(f"No granule files found in {data_path}")
        return []
    logger.info(f"Found {len(granule_files)} {instrument} file(s)")

    local_paths: List[str] = []
    for granule_file in granule_files:
        try:
            logger.info(f"Processing {os.path.basename(granule_file)}")
            base_name = os.path.splitext(os.path.basename(granule_file))[0]
            kwargs.setdefault('read_timestamp', True)
            result = starepandas.io.granules.to_local(
                file_path=granule_file,
                local_path=local_root,
                level=level,
                db_path=db_path,
                dataset=instrument,
                scan=scan,
                granule_name=base_name,
                **kwargs,
            )
            if isinstance(result, list):
                local_paths.extend(result)
            else:
                local_paths.append(result)
            logger.info(
                f"✓ Stored {os.path.basename(granule_file)} (granule={base_name}) → {local_root}"
            )
        except Exception as e:
            logger.error(f"✗ Failed to process {granule_file}: {e}")
            continue

    logger.info(f"Ingested {len(local_paths)} Parquet dataset(s)")
    return local_paths


__all__ = [
    "ingest_granules_s3",
    "ingest_granules_local",
    "clean_s3_prefix",
]
