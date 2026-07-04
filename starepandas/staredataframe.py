import bz2
import geopandas.plotting
import pystare
import pandas
import numpy as np
import starepandas
import netCDF4
import starepandas.tools.trixel_conversions
import starepandas.tools.temporal_conversions
import starepandas.io.pod
import multiprocessing
import pickle
import s3fs
import os
import json
import datetime

import logging
import time
import copy
import re
import warnings

from pathlib import Path

# MetadataStore abstraction (§C9 M4) — RDSMetadataStore wraps the
# psycopg2 INSERT/SELECT/DELETE paths so a future DynamoDB swap is a
# localised change. ``metadata.py`` lazy-imports ``_ensure_rds_db_and_table``
# from this module so no circular import.
from starepandas.metadata import PartitionRow, RDSMetadataStore

_AWS_S3_STORAGE_OPTIONS = {}
_AWS_RDS_OPTIONS = {}

# Task 12: default S3 prefix for ingest pipelines. Loaded from
# .config's optional ``default_s3_prefix=...`` line. Empty string means
# "not configured" — callers that omit s3_path will then have to fail
# loudly rather than silently writing to an unexpected location.
_DEFAULT_S3_PREFIX = ""

# C-6: cloud client SDK config. The REST API base URL (``endpoint``) and the
# API-Gateway key (``api_key``) are read from the same ``.config`` path as the
# S3/RDS settings so cloud callers reuse one config mechanism. Empty string
# means "not configured"; ``starepandas.cloud.config.get_cloud_config`` raises
# a clear error in that case. These are consumed by ``_apply_config_data`` and
# routed to these constants rather than leaking into s3fs storage-options.
_CLOUD_ENDPOINT = ""
_CLOUD_API_KEY = ""

# Maximum STARE level used for spatial partitioning when writing to S3.
# Each level multiplies partition count by 4. Level 4 caps at ~256 partitions
# per granule (vs ~4096 at level 6), keeping each Parquet partition file in
# the multi-MB range — the regime PyArrow / S3 are optimised for.
MAX_PARTITION_LEVEL = 4


# ─────────────────────────────────────────────────────────────────────────────
# Quaternary pod-code codec (docs/quaternary_storage_plan.md §2)
#
# A pod code is a compact base-4 string for a level-N trixel:
#
#     "q" + octant-digit(0-7) + one quaternary-digit(0-3) per refinement level
#
# Its length is *dynamic* — it follows the trixel's actual STARE level (a
# level-2 trixel → ``q132``; a level-4 trixel → ``q13211``). It encodes the
# same address as the old ``Q00_1/Q01_3/Q02_2/Q03_1/Q04_1`` directory chain.
#
# Bit layout of a STARE SID (same as the old generate_partition_path decode):
#   bits 0-4   : number of levels − 1  (so num_levels = (sid & 0x1F) + 1)
#   bits 59-61 : octant value          (level 0, 3 bits, 0-7)
#   bits 57-58 : level-1 quaternary value, 55-56 level-2, … (2 bits each)
#
# Chunk filenames are self-describing (``<podcode>-<granule>-<dataset>.parquet``)
# so a flat S3 listing is fully traceable without any directory context.
# ─────────────────────────────────────────────────────────────────────────────

CHUNK_SUFFIX = '.parquet'


def sid_to_podcode(sid: int) -> str:
    """Encode a STARE SID as a compact, dynamic-length pod code.

    The pod code is ``"q"`` + the octant digit + one quaternary digit per
    refinement level; its length follows the SID's actual level.

    Examples
    --------
    >>> sid_to_podcode(podcode_to_sid("q13211"))
    'q13211'
    """
    sid = int(sid) & 0xFFFFFFFFFFFFFFFF
    num_levels = (sid & 0x1F) + 1          # includes the octant level (level 0)
    octant = (sid >> 59) & 0x7
    digits = []
    for level in range(1, num_levels):
        if level <= 27:
            bit_start = 59 - 2 * level
            digits.append((sid >> bit_start) & 0x3)
        else:                              # no more bits beyond level 27
            digits.append(0)
    return 'q' + str(octant) + ''.join(str(d) for d in digits)


def podcode_to_sid(podcode: str) -> int:
    """Decode a pod code back to a STARE SID at the code's own level.

    Inverse of :func:`sid_to_podcode`. The reconstructed SID's level (bits
    0-4) is set from the number of quaternary digits in the code.

    Examples
    --------
    >>> podcode_to_sid("q13211") == podcode_to_sid("q13211")
    True
    """
    if not isinstance(podcode, str) or not podcode.startswith('q'):
        raise ValueError(f"Invalid pod code {podcode!r}: must start with 'q'")
    body = podcode[1:]
    if not body:
        raise ValueError(f"Invalid pod code {podcode!r}: missing octant digit")
    try:
        octant = int(body[0])
    except ValueError:
        raise ValueError(f"Invalid octant digit in pod code {podcode!r}")
    if not (0 <= octant <= 7):
        raise ValueError(f"Octant {octant} out of range (0-7) in {podcode!r}")
    digits = []
    for ch in body[1:]:
        try:
            d = int(ch)
        except ValueError:
            raise ValueError(f"Invalid quaternary digit {ch!r} in pod code {podcode!r}")
        if not (0 <= d <= 3):
            raise ValueError(f"Quaternary digit {d} out of range (0-3) in {podcode!r}")
        digits.append(d)

    num_levels = 1 + len(digits)           # octant level + quaternary levels
    if num_levels > 28:
        raise ValueError(f"Pod code {podcode!r} encodes too many levels ({num_levels})")
    sid = (num_levels - 1) & 0x1F          # bits 0-4
    sid |= (octant & 0x7) << 59            # bits 59-61
    for i, d in enumerate(digits, start=1):
        bit_start = 59 - 2 * i
        sid |= (d & 0x3) << bit_start
    return sid


def podcode_to_local_dirs(podcode: str) -> list:
    """Cumulative pod-code directory chain for the local (hierarchical) layout.

    The leaf directory equals the full pod code; its depth follows the level.

    Examples
    --------
    >>> podcode_to_local_dirs("q13211")
    ['q13', 'q132', 'q1321', 'q13211']
    >>> podcode_to_local_dirs("q132")
    ['q13', 'q132']
    """
    body = podcode[1:]
    if len(body) <= 1:                     # octant-only (level-0) trixel
        return [podcode]
    return ['q' + body[:n] for n in range(2, len(body) + 1)]


def chunk_filename(podcode: str, granule_basename: str, dataset: str) -> str:
    """Build a self-describing chunk filename per the §2 grammar.

    ``<podcode>-<granule_basename>-<dataset>.parquet``. The grammar relies on
    datasets never containing ``-`` (they use ``_``: ``SSMIS_S1``, ``GMI_S1``).
    ``granule_basename`` *may* contain ``-`` — it is recovered as the middle
    span between the first and last ``-`` (see :func:`parse_chunk_filename`).
    """
    if '-' in dataset:
        raise ValueError(
            f"dataset name must not contain '-' (the filename grammar reserves "
            f"'-' as a separator; datasets use '_'): {dataset!r}"
        )
    if '-' in podcode:
        raise ValueError(f"pod code must not contain '-': {podcode!r}")
    return f"{podcode}-{granule_basename}-{dataset}{CHUNK_SUFFIX}"


def parse_chunk_filename(name: str):
    """Parse a chunk filename / key into ``(podcode, granule_basename, dataset)``.

    Inverse of :func:`chunk_filename`. Accepts a bare filename or a full path /
    S3 key (only the basename is parsed). Pod code = before the first ``-``;
    dataset = after the last ``-``; granule basename = everything between.
    """
    base = os.path.basename(name)
    if base.endswith(CHUNK_SUFFIX):
        base = base[:-len(CHUNK_SUFFIX)]
    first = base.find('-')
    last = base.rfind('-')
    if first == -1 or first == last:
        raise ValueError(
            f"Not a pod-code chunk filename (need >=2 '-' separators): {name!r}"
        )
    podcode = base[:first]
    dataset = base[last + 1:]
    granule_basename = base[first + 1:last]
    return podcode, granule_basename, dataset

def aws_configure(key=None, secret=None, token=None, region_name=None, endpoint_url=None, client_kwargs=None,
                  rds=None, db_host=None, db_port=None, db_username=None, db_password=None, db_database=None,
                  **s3fs_kwargs):
    """
    Configure default AWS/S3 options for S3 Parquet helpers and optional RDS Postgres metadata store.

    Parameters
    - key: AWS access key id
    - secret: AWS secret access key
    - token: AWS session token (optional)
    - region_name: AWS region (e.g., "us-west-2")
    - endpoint_url: Custom S3-compatible endpoint (optional)
    - client_kwargs: dict to merge into s3fs client_kwargs
    - rds: dict with keys {host, port, username, password, database} for RDS connection
    - db_host/db_port/db_username/db_password/db_database: overrides for rds dict
    - **s3fs_kwargs: any additional s3fs.S3FileSystem kwargs

    Notes
    - These options are used by to_s3/from_s3 when storage_options is not provided.
    - You can pass a ready-made 'client_kwargs' dict or individual fields like 'region_name'/'endpoint_url'.
    """
    global _AWS_S3_STORAGE_OPTIONS, _AWS_RDS_OPTIONS
    options = dict(s3fs_kwargs) if s3fs_kwargs else {}
    if key is not None:
        options['key'] = key
    if secret is not None:
        options['secret'] = secret
    if token is not None:
        options['token'] = token

    ck = dict(client_kwargs) if client_kwargs else {}
    if region_name is not None:
        ck['region_name'] = region_name
    if endpoint_url is not None:
        ck['endpoint_url'] = endpoint_url
    if ck:
        options['client_kwargs'] = ck

    _AWS_S3_STORAGE_OPTIONS = options

    # Configure RDS/PostgreSQL connection options
    rds_opts = {}
    if isinstance(rds, dict):
        rds_opts.update(rds)
    if db_host is not None:
        rds_opts['host'] = db_host
    if db_port is not None:
        rds_opts['port'] = int(db_port)
    if db_username is not None:
        rds_opts['username'] = db_username
    if db_password is not None:
        rds_opts['password'] = db_password
    if db_database is not None:
        rds_opts['database'] = db_database

    if rds_opts:
        _AWS_RDS_OPTIONS = rds_opts
    return _AWS_S3_STORAGE_OPTIONS

# Env var carrying the worker config as a JSON string. ECS injects the
# Secrets-Manager secret this way (see infra/cdk task definition), so the
# cloud worker needs no /etc/starepods/.config file mounted — the env-var
# branch in _load_config_from_default_locations parses it directly.
WORKER_SECRET_ENV_VAR = 'STAREPANDAS_WORKER_SECRET'

# Keys consumed explicitly by _apply_config_data — these must NOT pass
# through as s3fs.S3FileSystem kwargs (notably default_s3_prefix, which is
# not a valid S3FileSystem argument and would raise at construction time).
_RESERVED_CONFIG_KEYS = {
    'key', 'secret', 'token', 'client_kwargs',
    'aws_access_key_id', 'aws_secret_access_key', 'aws_session_token',
    'region', 'region_name', 'endpoint_url', 'rds',
    'host', 'port', 'username', 'password', 'database',
    'default_s3_prefix',
    'endpoint', 'api_key',
}


def _apply_config_data(data):
    """Apply a parsed config dict (JSON-shaped) to the module-level AWS/S3
    and RDS defaults.

    Shared by ``load_aws_configure`` (file path) and the
    ``STAREPANDAS_WORKER_SECRET`` env-var path so both honour the same
    schema. Crucially, ``default_s3_prefix`` is consumed here and routed to
    the module constant rather than leaking into the s3fs kwargs.
    """
    key = data.get('key') or data.get('aws_access_key_id')
    secret = data.get('secret') or data.get('aws_secret_access_key')
    token = data.get('token') or data.get('aws_session_token')

    client_kwargs = data.get('client_kwargs', {})
    region_name = data.get('region_name') or data.get('region')
    endpoint_url = data.get('endpoint_url')

    rds_block = dict(data.get('rds') or {})
    for k in ['host', 'port', 'username', 'password', 'database']:
        if k in data and k not in rds_block:
            rds_block[k] = data[k]

    dp = data.get('default_s3_prefix')
    if dp:
        global _DEFAULT_S3_PREFIX
        _DEFAULT_S3_PREFIX = str(dp).rstrip('/')

    # C-6: cloud SDK endpoint + API key. Routed to the module constants so the
    # client SDK can read them without re-parsing the config file.
    ep = data.get('endpoint')
    if ep:
        global _CLOUD_ENDPOINT
        _CLOUD_ENDPOINT = str(ep).rstrip('/')
    ak = data.get('api_key')
    if ak:
        global _CLOUD_API_KEY
        _CLOUD_API_KEY = str(ak)

    return aws_configure(
        key=key,
        secret=secret,
        token=token,
        region_name=region_name,
        endpoint_url=endpoint_url,
        client_kwargs=client_kwargs,
        rds=rds_block,
        **{k: v for k, v in data.items() if k not in _RESERVED_CONFIG_KEYS}
    )


def load_aws_configure(config_path):
    """
    Load AWS/S3 configuration from a JSON file and set defaults for S3 Parquet helpers.

    The JSON may contain either s3fs-style keys (key, secret, token, client_kwargs)
    or AWS-style keys (aws_access_key_id, aws_secret_access_key, aws_session_token, region_name, endpoint_url).

    It may also include an 'rds' block with {host, port, username, password, database}, or top-level aliases,
    and an optional 'default_s3_prefix'.
    """
    with open(config_path, 'r') as f:
        data = json.load(f)
    return _apply_config_data(data)

def _load_config_from_default_locations() -> bool:
    """Try to load configuration from default file locations.

    Order of precedence:
    - Env var STAREPANDAS_WORKER_SECRET (config JSON injected inline — the
      ECS/Secrets-Manager path; no file on disk required)
    - Env var STAREPANDAS_AWS_CONFIG
    - ./.config (current working directory)
    - <package_root>/.config (project root next to this module)
    - ~/.starepandas_aws_config.json
    - ~/.starepandas/aws.json
    Returns True if successfully loaded, else False.
    """
    # Highest precedence: the config JSON injected directly as an env var.
    # ECS populates STAREPANDAS_WORKER_SECRET from Secrets Manager, so the
    # worker container reads its config without a mounted .config file.
    secret_json = os.environ.get(WORKER_SECRET_ENV_VAR)
    if secret_json:
        try:
            _apply_config_data(json.loads(secret_json))
            return True
        except Exception:
            # A malformed inline secret must not mask the file-based
            # fallback below — fall through and try the on-disk candidates.
            pass

    candidates = []
    env_path = os.environ.get('STAREPANDAS_AWS_CONFIG')
    if env_path:
        candidates.append(env_path)
    candidates.append(os.path.join(os.getcwd(), '.config'))
    candidates.append(str(Path(__file__).resolve().parents[1] / '.config'))
    candidates.append(os.path.join(Path.home(), '.starepandas_aws_config.json'))
    candidates.append(os.path.join(Path.home(), '.starepandas', 'aws.json'))

    for cfg in candidates:
        try:
            if os.path.isfile(cfg):
                # Support simple .config key=value pairs as well as JSON
                try:
                    with open(cfg, 'r') as f:
                        txt = f.read().strip()
                    if txt.startswith('{'):
                        load_aws_configure(cfg)
                    else:
                        # Parse simple key=value lines
                        kv = {}
                        for line in txt.splitlines():
                            line = line.strip()
                            if not line or line.startswith('#'):
                                continue
                            if '=' in line:
                                k, v = line.split('=', 1)
                                kv[k.strip()] = v.strip()
                        # Map to expected fields
                        rds_block = {
                            'host': kv.get('rds_host') or kv.get('host'),
                            'port': int(kv.get('port', '5432')),
                            'username': kv.get('username') or kv.get('user'),
                            'password': kv.get('password'),
                            'database': kv.get('database') or 'postgres'
                        }
                        aws_configure(
                            key=kv.get('key'),
                            secret=kv.get('secret'),
                            region_name=kv.get('region_name') or kv.get('region'),
                            rds=rds_block
                        )
                        # Task 12: optional default_s3_prefix lets ingest callers
                        # omit s3_path and inherit the project's storage root.
                        dp = kv.get('default_s3_prefix')
                        if dp:
                            global _DEFAULT_S3_PREFIX
                            _DEFAULT_S3_PREFIX = dp.rstrip('/')
                        # C-6: cloud SDK endpoint + API key (key=value form).
                        ep = kv.get('endpoint')
                        if ep:
                            global _CLOUD_ENDPOINT
                            _CLOUD_ENDPOINT = ep.rstrip('/')
                        ak = kv.get('api_key')
                        if ak:
                            global _CLOUD_API_KEY
                            _CLOUD_API_KEY = ak
                except Exception:
                    # Fallback to JSON loader if parsing failed unexpectedly
                    load_aws_configure(cfg)
                return True
        except Exception:
            continue
    return False

def _ensure_rds_db_and_table(target_dbname='StarePodsMetadata'):
    """
    Ensure the RDS Postgres database and table exist, and return a connection to the target DB.
    Expects _AWS_RDS_OPTIONS with keys: host, port, username, password, database (admin DB to connect first).
    """
    if not _AWS_RDS_OPTIONS:
        # Attempt to auto-load default config file if available
        _load_config_from_default_locations()
    if not _AWS_RDS_OPTIONS:
        raise ValueError(
            "Missing RDS configuration. Call load_aws_configure(config_path) or aws_configure(..., rds=...) "
            "to set RDS connection parameters."
        )

    try:
        import psycopg2
        from psycopg2 import sql
    except ImportError as e:
        raise ImportError("psycopg2 is required for RDS metadata operations. Install 'psycopg2-binary'.") from e

    host = _AWS_RDS_OPTIONS.get('host')
    port = int(_AWS_RDS_OPTIONS.get('port', 5432))
    user = _AWS_RDS_OPTIONS.get('username') or _AWS_RDS_OPTIONS.get('user')
    password = _AWS_RDS_OPTIONS.get('password')
    admin_db = _AWS_RDS_OPTIONS.get('database') or 'postgres'

    if not all([host, user, password]):
        raise ValueError("RDS configuration incomplete: require host, username, password (and optionally port, database).")

    # Connect to admin DB to ensure target DB exists
    admin_conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=admin_db)
    admin_conn.set_session(autocommit=True)
    try:
        with admin_conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (target_dbname,))
            exists = cur.fetchone() is not None
            if not exists:
                cur.execute(sql.SQL("CREATE DATABASE {} ").format(sql.Identifier(target_dbname)))
    finally:
        admin_conn.close()

    # Connect to target DB and ensure table
    conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=target_dbname)
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS "PodsMetadata" (
                "Dataset" TEXT,
                "DataLevel" TEXT,
                "RawData Collected Time" TIMESTAMP,
                grouped_id BIGINT,
                "S3 bucket" TEXT,
                "Resolution level" INTEGER,
                "MetadataJson" JSONB,
                t_start TIMESTAMP,
                t_end TIMESTAMP,
                podcode TEXT
            )
            """
        )

        # Temporal-stare-pods issue 01: chunk temporal range + pod code.
        # Probe the catalog first (same pattern as the grouped_id upgrade
        # below) and only run DDL when something is actually missing: this
        # function runs on every connect — per granule at worker scale —
        # and even a no-op ALTER/CREATE INDEX takes table locks and needs
        # table ownership, which steady-state connects can't afford.
        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'PodsMetadata'
            AND column_name IN ('t_start', 't_end', 'podcode')
            """
        )
        present = {row[0] for row in cur.fetchall()}
        for col, decl in (('t_start', 'TIMESTAMP'), ('t_end', 'TIMESTAMP'),
                          ('podcode', 'TEXT')):
            if col not in present:
                cur.execute(
                    f'ALTER TABLE "PodsMetadata" ADD COLUMN IF NOT EXISTS {col} {decl}'
                )
        cur.execute(
            """
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'PodsMetadata'
            AND indexname IN ('idx_pods_podcode', 'idx_pods_temporal')
            """
        )
        have_idx = {row[0] for row in cur.fetchall()}
        if 'idx_pods_podcode' not in have_idx:
            cur.execute('CREATE INDEX IF NOT EXISTS idx_pods_podcode ON "PodsMetadata" (podcode)')
        if 'idx_pods_temporal' not in have_idx:
            cur.execute('CREATE INDEX IF NOT EXISTS idx_pods_temporal ON "PodsMetadata" (t_start, t_end)')


        # Check if grouped_id column needs to be upgraded from INTEGER to BIGINT
        cur.execute(
            """
            SELECT data_type 
            FROM information_schema.columns 
            WHERE table_name = 'PodsMetadata' 
            AND column_name = 'grouped_id'
            """
        )
        result = cur.fetchone()
        if result and result[0] == 'integer':
            print("Upgrading grouped_id column from INTEGER to BIGINT to support 64-bit STARE SIDs...")
            cur.execute('ALTER TABLE "PodsMetadata" ALTER COLUMN grouped_id TYPE BIGINT')
            print("✓ Column upgraded successfully")
        
        conn.commit()
    return conn

def _ensure_sqlite_db_and_table(db_path: str):
    """
    Open (creating if needed) a SQLite DB with the PodsMetadata table.

    This is the local-storage equivalent of ``_ensure_rds_db_and_table``.
    Uses the stdlib ``sqlite3`` module — no extra dependencies required.

    Parameters
    ----------
    db_path : str
        Path to the SQLite database file.  Parent directories are created
        automatically.

    Returns
    -------
    sqlite3.Connection
        Open connection with WAL journal mode enabled.  Caller is responsible
        for closing it.
    """
    import sqlite3
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    conn = sqlite3.connect(db_path)
    # WAL mode: allows concurrent readers while a writer is active
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS "PodsMetadata" (
            "Dataset"               TEXT,
            "DataLevel"             TEXT,
            "RawData Collected Time" TEXT,
            grouped_id              INTEGER,
            "LocalPath"             TEXT,
            "Resolution level"      INTEGER,
            "MetadataJson"          TEXT,
            t_start                 TEXT,
            t_end                   TEXT,
            podcode                 TEXT
        )
    """)
    # Temporal-stare-pods issue 01: upgrade a pre-temporal catalog in place.
    # SQLite has no ADD COLUMN IF NOT EXISTS, so consult the table info.
    existing_cols = {row[1] for row in conn.execute('PRAGMA table_info("PodsMetadata")')}
    for col in ('t_start', 't_end', 'podcode'):
        if col not in existing_cols:
            conn.execute(f'ALTER TABLE "PodsMetadata" ADD COLUMN {col} TEXT')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_pods_dataset ON "PodsMetadata" ("Dataset")')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_pods_grouped ON "PodsMetadata" (grouped_id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_pods_podcode ON "PodsMetadata" (podcode)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_pods_temporal ON "PodsMetadata" (t_start, t_end)')
    # Same uniqueness identity as the RDS pods_unique constraint — backs the
    # ON CONFLICT upsert that keeps local re-ingest idempotent.
    try:
        conn.execute(
            'CREATE UNIQUE INDEX IF NOT EXISTS pods_unique '
            'ON "PodsMetadata" ("Dataset", "RawData Collected Time", grouped_id)'
        )
    except sqlite3.IntegrityError as e:
        raise sqlite3.IntegrityError(
            f"Cannot upgrade catalog {db_path!r}: PodsMetadata already holds "
            'duplicate ("Dataset", "RawData Collected Time", grouped_id) rows, '
            "so the pods_unique index that keeps re-ingest idempotent cannot "
            "be created. Dedupe those rows or re-ingest into a fresh catalog."
        ) from e
    conn.commit()
    return conn


def _parse_s3_bucket(s3_path: str) -> str:
    if not s3_path.startswith('s3://'):
        return ''
    rest = s3_path[5:]
    return rest.split('/', 1)[0] if '/' in rest else rest


#: Per-point scan-time column produced by the granule readers.
DEFAULT_TIMESTAMP_COLUMN = 'timestamp'


def _chunk_temporal_range(df, column=DEFAULT_TIMESTAMP_COLUMN):
    """Temporal range [t_start, t_end] of one chunk — the min and max of the
    scan times of the data points it contains.

    Points with missing times are dropped before the min/max. Returns
    ``(None, None)`` when the column is absent or no point has a usable
    time, so a timestamp-less write still succeeds with an empty range.
    """
    if column not in df.columns:
        return None, None
    times = pandas.to_datetime(df[column], errors='coerce').dropna()
    if times.empty:
        return None, None
    return times.min().to_pydatetime(), times.max().to_pydatetime()


DEFAULT_SID_COLUMN_NAME = 'sids'
DEFAULT_TID_COLUMN_NAME = 'tids'
DEFAULT_TRIXEL_COLUMN_NAME = 'trixels'
DEFAULT_GEOMETRY_COLUMN_NAME = 'geometry'

def compress_sids_group(group):
    sids = group[1].to_numpy()  # zero element is group label, 1 element is the df
    if sids.dtype == np.dtype('O'):
        # If we receive a series of SID collections we merge all sids into a single 1D array
        # to_numpy() would have produced an array of lists in this case
        sids = np.concatenate(sids)
    sids = starepandas.compress_sids(sids)
    return tuple([group[0], sids])

def write_pod_pickle(g, fname, append=False, compress=None):
    """Write or append to a pickle."""
    logging.info('Writing to pickle: %s' % fname)
    if append:
        raise NotImplementedError('appending not implemented')
        with starepandas.io.pod.generic_open(fname)(fname, 'a+b') as f:
            pickle.dump(g, f)
    else:
        # Overwrite
        start = time.time()
        if compress == None:
            with open(fname, 'wb') as f:
                pickle.dump(g, f)
                logging.info('Writing chunk %s took %d seconds.' % (fname, time.time() - start))
        elif compress == 'bz2':
            with bz2.open(fname, 'wb') as f:
                pickle.dump(g, f)
                logging.info('Writing bz2 chunk %s took %d seconds.' % (fname, time.time() - start))
        else:
            raise ValueError('write_pod_pickle argument compress="%s" not understood.'%compress)
    return

def write_pod_hdf(g, fname, append=False):
    """Write or append to an HDF file."""
    # raise NotImplementedError
    # if append:
    #     pass
    # else:
    #     pass
    return

class STAREDataFrame(geopandas.GeoDataFrame):
    _metadata = ['_sid_column_name', '_trixel_column_name', '_geometry_column_name', '_tid_column_name']

    _sid_column_name = DEFAULT_SID_COLUMN_NAME
    _trixel_column_name = DEFAULT_TRIXEL_COLUMN_NAME
    _tid_column_name = DEFAULT_TID_COLUMN_NAME
    _geometry_column_name = DEFAULT_GEOMETRY_COLUMN_NAME

    def __init__(self, *args,
                 sids=None, add_sids=False, level=None,
                 trixels=None, add_trixels=False, n_partitions=1,
                 **kwargs):
        """
        A STAREDataFrame object is a pandas.DataFrame that has a special column
        with STARE indices and optionally a special column holding the trixel representation.
        In addition to the standard DataFrame constructor arguments,
        STARE also accepts the following keyword arguments:

        Parameters
        ----------
        sids : str or array-like
            If str, column to use as stare column. If array, will be set as 'stare' column on STAREDataFrame.
        add_sids : bool
            If true, STARE index values will be generated using a geometry column
        level: int
            If add_stare is True, then use level as the maximum STARE level
        trixels : str or array-like
            If str, column to use as trixel column. If array, will be set as 'trixel' column on STAREDataFrame.
        add_trixels : bool
            If true, trixels will be generated from the STARE column.

        Examples
        ---------
        # >>> cities = ['Buenos Aires', 'Brasilia', 'Santiago', 'Bogota', 'Caracas']
        # >>> latitudes = [-34.58, -15.78, -33.45, 4.60, 10.48]
        # >>> longitudes = [-58.66, -47.91, -70.66, -74.08, -66.86]
        # >>> data =  {'City': cities, 'Latitude': latitudes, 'Longitude': longitudes}
        # >>> sids = starepandas.sids_from_xy(longitudes, latitudes, level=5)
        # >>> sdf = starepandas.STAREDataFrame(data, sids=sids)
        """

        super().__init__(*args, **kwargs)

        if args and isinstance(args[0], (geopandas.GeoDataFrame, STAREDataFrame)):
            self._geometry_column_name = args[0]._geometry_column_name
            # self.set_crs(args[0].crs, inplace=True)

        if sids is not None:
            self.set_sids(sids, inplace=True)
        elif add_sids:
            if level is None:
                raise ValueError('Level has to be specified if SIDs are to be added')
            sids = self.make_sids(level=level, n_partitions=n_partitions)
            self.set_sids(sids, inplace=True)

        if trixels is not None:
            self.set_trixels(trixels, inplace=True)
        elif add_trixels:
            trixels = self.make_trixels(n_partitions=n_partitions)
            self.set_trixels(trixels, inplace=True)

    def __copy__(self):
        new_instance = super().__copy__()  # Call the parent class copy method
        # new_instance = self.copy()
        new_instance.__class__ = STAREDataFrame  # Ensure the correct class type
        return new_instance

    def __deepcopy__(self, memo=None):
        new_instance = super().__deepcopy__(memo)  # Call parent class deepcopy method
        # new_instance = self.copy()
        new_instance.__class__ = STAREDataFrame  # Ensure the correct class type

        # Copy the metadata attributes
        for key in self._metadata:
            setattr(new_instance, key, copy.deepcopy(getattr(self, key), memo))

        return new_instance

    def reset_index(self, inplace=False, drop=False):
        new_instance = super().reset_index(inplace=inplace, drop=drop)
        if not inplace:
            new_instance.__class__ = STAREDataFrame
            return new_instance

    def __getitem__(self, key):

        result = super().__getitem__(key)
        sid_col = self._sid_column_name

        if isinstance(result, (geopandas.GeoDataFrame, pandas.DataFrame, starepandas.STAREDataFrame)):
            result.__class__ = STAREDataFrame
            result._sid_column_name = sid_col
        elif isinstance(result, (geopandas.GeoSeries, pandas.Series)):
            # result.__class__ = starepandas.STARESeries
            pass
        else:
            pass
            # result.__class__ = geopandas.GeoDataFrame
        return result

    def __setattr__(self, attr, val):
        # have to special case geometry b/c pandas tries to use as column...
        if attr == "stare":
            object.__setattr__(self, attr, val)
        else:
            super().__setattr__(attr, val)

    def make_sids(self, level, convex=False, force_ccw=True, n_partitions=1):
        """
        Generates and returns the STARE representation of each feauture.

        Parameters
        -----------
        level: int; 0<=level<=27
            STARE level to use for the STARE lookup
        convex: bool
            Toggle if STARE indices for the convex hull rather than the G-Ring should be looked up
        force_ccw: bool
            Toggle if a counterclockwise orientation of the geometries should be enforced. Unfortunately, OGC and ESRI
            have oposing definitions. ([stackexchange](https://gis.stackexchange.com/questions/119150/order-of-polygon-vertices-in-general-gis-clockwise-or-counterclockwise.)).
            [ESRI](http://esri.github.io/geometry-api-java/doc/Polygon.html) defines exterior rings as clockwise, OGC as counterclockwise.
            We use the OGC definition, making it necessary to generally force CCW for polygons loaded from shapefules.
        n_partitions: int
            Number of partititions used to lookup STARE indices in parallel

        Returns
        ---------
        sids: numpy.ndarray
            array of (set of) STARE index values

        Examples
        ----------
        From points

        # >>> import starepandas, geopandas
        # >>> lats = [-72.609177, -72.648590, -72.591286]
        # >>> lons = [-41.255402, -42.054047, -41.625336]
        # >>> geoms = geopandas.points_from_xy(lons, lats)
        # >>> sdf = starepandas.STAREDataFrame(geometry=geoms)
        # >>> sdf.make_sids(level=6, convex=False)
        # 0    2299437706637111654
        # 1    2299435211084507366
        # 2    2299436587616075270
        # Name: sids, dtype: int64
        #
        # From polygons
        #
        # >>> gdf = geopandas.read_file(geopandas.datasets.get_path("naturalearth_lowres"))
        # >>> sdf = starepandas.STAREDataFrame(gdf)
        # >>> sids = sdf.make_sids(level=5)
        """

        sids = starepandas.sids_from_geoseries(self.geometry, level=level, convex=convex,
                                               force_ccw=force_ccw, n_partitions=n_partitions)
        return sids

    def drop_na_sids(self, inplace=False):
        """Drop all rows that have NA values for the SIDs and cast the column to numpy.int64 """
        if inplace:
            self.dropna(subset=[self._sid_column_name], inplace=inplace)
            self[self._sid_column_name] = self[self._sid_column_name].astype(np.dtype('int64'))
        else:
            frame = self.__deepcopy__()
            frame = frame.dropna(subset=[frame._sid_column_name], inplace=inplace)
            frame[frame._sid_column_name] = frame[frame._sid_column_name].astype(np.dtype('int64'))
            return frame

    def make_tids(self, column='ts_start', end_column=None, forward_res=48, reverse_res=48):
        """
        Generates and returns the STARE representation of each feauture.

        Parameters
        -----------
        column: str
            column name containing datetime
        end_column: str
            optional. Column containing the end of the timestamp
        forward_res: int
            forward resolution
        reverse_res: int
            reverse resolution
        Returns
        ---------
        tids: numpy.ndarray
            array of (set of) STARE index values

        Examples
        ----------
        From points

        # >>> import starepandas, geopandas
        """
        # Autoadjust resolution
        start_col = self[column]
        if not pandas.api.types.is_datetime64_any_dtype(start_col.dtype):
            raise TypeError('dtype of column must be numpy.datetime64')

        tids = starepandas.tivs_from_timeseries(self[column],
                                                scale='utc',
                                                format='datetime64',
                                                forward_res=forward_res,
                                                reverse_res=reverse_res)
        return tids

    def set_sids(self, col, inplace=False):
        """ Set the StareDataFrame  spatial indices using either an existing column or
        the specified input. By default, yields a new object.
        The original tid column is replaced with the input.

        Parameters
        -------------
        col: array-like
            f stare sids or column name
        inplace: boolean
            Modify the StareDataFrame in place (do not create a new object)

        Returns
        ---------
        df: STAREDataFrame
            the df with sids

        Examples
        --------
        # >>> import starepandas
        # >>> sdf = starepandas.STAREDataFrame()
        # >>> sids = [4611686018427387903, 2299435211084507590, 2299566194809236966]
        # >>> sdf.set_sids(sids, inplace=True)
        """

        # Most of the code here is taken from GeoDataFrame.set_geometry()
        if inplace:
            frame = self
        else:
            frame = self.__deepcopy__()

        if isinstance(col, (list, np.ndarray, pandas.Series)):
            frame[frame._sid_column_name] = col
        elif hasattr(col, "ndim") and col.ndim != 1:
            raise ValueError("Must pass array with one dimension only.")
        elif isinstance(col, str) and col in frame.columns:
            frame._sid_column_name = col
        else:
            raise ValueError("Must pass array-like object or column name")

        if not inplace:
            return frame

    def set_tids(self, col, inplace=False):
        """ Set the StareDataFrame temporal indices using either an existing column or
        the specified input. By default, yields a new object.
        The original tid column is replaced with the input.

        Parameters
        -------------
        col: array-like
            f stare tids or column name
        inplace: boolean
            Modify the StareDataFrame in place (do not create a new object)

        Returns
        ---------
        df: STAREDataFrame
            the df with tids

        Examples
        --------
        # >>> import starepandas
        # >>> sdf = starepandas.STAREDataFrame()
        # >>> tids = [4611686018427387903, 2299435211084507590, 2299566194809236966]
        # >>> sdf.set_tids(tids, inplace=True)
        """

        # Most of the code here is taken from GeoDataFrame.set_geometry()
        if inplace:
            frame = self
        else:
            frame = self.__deepcopy__()

        if isinstance(col, (list, np.ndarray, pandas.Series)):
            frame[frame._tid_column_name] = col
        elif hasattr(col, "ndim") and col.ndim != 1:
            raise ValueError("Must pass array with one dimension only.")
        elif isinstance(col, str) and col in frame.columns:
            frame._tid_column_name = col
        else:
            raise ValueError("Must pass array-like object or column name")

        if not inplace:
            return frame

    def has_trixels(self):
        return self._trixel_column_name in self

    def has_sids(self):
        return self._sid_column_name in self

    def make_trixels(self, sid_column=None, n_partitions=1, wrap_lon=True, num_workers=None):
        """
        Returns a Polygon or Multipolygon GeoSeries
        containing the trixels referred by the STARE indices

        Parameters
        -----------
        sid_column: str
            Column to use as STARE column. Default: 'stare'
        n_partitions: int
            number of (dask) workers to use to generate trixels
        wrap_lon: bool
            toggle if trixels should be wraped around antimeridian.

        num_workers: int
            number of workers to use

        Returns
        -----------
        trixels_series: numpy.array
            array of polygons or multipolygons representing the trixels

        Examples
        --------
        # >>> import starepandas
        # >>> sids = [648518346341351428, 900719925474099204, 1170935903116328964]
        # >>> sdf = starepandas.STAREDataFrame(sids=sids)
        # >>> trixels = sdf.make_trixels()
        """

        if sid_column is None:
            sid_column = self._sid_column_name
        if sid_column not in list(self.columns):
            raise Exception('sids column does not exist')
        trixels_series = starepandas.tools.trixel_conversions.trixels_from_stareseries(self[sid_column],
                                                                                       n_partitions=n_partitions,
                                                                                       num_workers=num_workers,
                                                                                       wrap_lon=wrap_lon)
        return trixels_series

    def add_trixels(self, n_partitions=1, num_workers=None, inplace=False, wrap_lon=True):
        """Combination of make_trixels() and set_trixels()"""
        sid_column = self._sid_column_name
        trixels = self.make_trixels(sid_column=sid_column, n_partitions=n_partitions,
                                    num_workers=num_workers, wrap_lon=wrap_lon)

        return self.set_trixels(trixels, inplace=inplace)

    def set_trixels(self, col, inplace=False):
        """
        Set the trixel column

        Parameters
        ------------
        col: array-like or string
            If array like, will add the array as a new trixel column. If string, will set the df['col']
            as the trixel column. If None, will generate trixels from the STARE column.
        inplace: bool
            Modify the StareDataFrame in place (do not create a new object)

        Returns
        -------
        df: DataFrame
            DataFrame or None


        Examples
        ---------
        # >>> import starepandas
        # >>> sids = [4611686018427387903, 4611686018427387903, 4611686018427387903]
        # >>> sdf = starepandas.STAREDataFrame(sids=sids)
        # >>> trixels = sdf.make_trixels()
        # >>> sdf.set_trixels(trixels, inplace=True)
        """

        if inplace:
            frame = self
        else:
            frame = self.__deepcopy__()

        if isinstance(col, (pandas.Series, geopandas.GeoSeries, list, np.ndarray)):
            col = geopandas.geodataframe._ensure_geometry(col)
            frame[frame._trixel_column_name] = col
        elif isinstance(col, str) and col in self.columns:
            frame._trixel_column_name = col
        else:
            raise ValueError("Must pass array-like object or column name")
        # frame.set_geometry(col, inplace=True)

        if not inplace:
            return frame

    def trixel_vertices(self):
        """ Returns the vertices and centerpoints of the trixels.
        Requires stare column to be set. Vertices are a tuple of:

        1. the latitudes of the corners
        2. the longitudes of the corners
        3. the latitudes of the centers
        4. the longitudes of the centers

        Returns
        ---------
        vertices
            A vertices data structure

        Examples
        ---------
        # >>> sids = np.array([3458764513820540928])
        # >>> df = starepandas.STAREDataFrame(sids=sids)
        # >>> df.trixel_vertices()
        (array([29.9999996 , 45.00000069, 29.9999996 ]), array([-170.26439001,  -45.        ,   80.26439001]), array([80.264389]), array([135.]))
        """
        return starepandas.tools.trixel_conversions.to_vertices(self[self._sid_column_name])

    def trixel_centers(self, vertices=None):
        """ Returns the trixel centers.

        If vertices is set, the trixel centers are extracted from the vertices (c.f. :func:`~trixel_vertices`).
        If not, they are generated from the stare column.

        Parameters
        --------------
        vertices: vertices data structure
            If set, the centers are extracted from the vertices data structure.

        Returns
        ---------
        trixel_centers : numpy.array
            Trixel centers. First dimension are the SIDs, second dimension lon/lat.

        Examples
        ---------
        # >>> sids = np.array([3458764513820540928])
        # >>> df = starepandas.STAREDataFrame(sids=sids)
        # >>> df.trixel_centers()
        array([[134.9      ,  80.264389]])
        """

        if vertices:
            return starepandas.tools.trixel_conversions.vertices2centers(vertices)
        else:
            return starepandas.tools.trixel_conversions.to_centers(self[self._sid_column_name])

    def trixel_centers_ecef(self, vertices=None):
        """ Returns the trixel centers as ECEF vectors.

        If vertices is set, the trixel centers are extracted from the vertices (c.f. :func:`~trixel_vertices`).
        If not, they are generated from the stare column.

        Parameters
        --------------
        vertices: vertices data structure
            If set, the centers are extracted from the vertices data structure.

        Returns
        ---------
        trixel_centers : numpy.array
            Trixel centers. First dimension are the sids, second dimension are x/y/z.

        Examples
        ---------
        # >>> sids = np.array([3458764513820540928])
        # >>> df = starepandas.STAREDataFrame(sids=sids)
        # >>> df.trixel_centers_ecef()
        array([[-0.11957316,  0.11957316,  0.98559856]])
        """
        if vertices:
            return starepandas.tools.trixel_conversions.vertices2centers_ecef(vertices)
        else:
            return starepandas.tools.trixel_conversions.to_centers_ecef(self[self._sid_column_name])

    def trixel_centerpoints(self, vertices=None):
        """ Returns the trixel centers as shapely points.

        If vertices is set, the trixel centers are extracted from the vertices (c.f. :func:`~trixel_vertices`).
        If not, they are generated from the stare column.

        Parameters
        ----------------
        vertices: tuple (vertices data structure)
            If set, the centers are extracted from the vertices.

        Returns
        ---------
        trixel_centerpoints: Geometery Array
            Series of shapely trixel center points

        Examples
        ---------
        # >>> sids = np.array([4458764513820540928])
        # >>> df = starepandas.STAREDataFrame(sids=sids)
        # >>> centers = df.trixel_centerpoints()
        # >>> print(centers[0])
        POINT (18.4 24.09)
        """
        if vertices:
            return starepandas.tools.trixel_conversions.vertices2centerpoints(vertices)
        else:
            return starepandas.tools.trixel_conversions.to_centerpoints(self[self._sid_column_name])

    def trixel_corners(self, vertices=None, from_trixels=False):
        """ Returns corners of trixels as lon/lat.

        If vertices is set, the trixel corners are extracted from vertices  (c.f. :func:`~trixel_vertices`).
        If from_trixels is True and dataframe contains trixel column, corners are extracted from trixels.
        If not, corners are generated from stare column

        Parameters
        ----------
        vertices : tuple (vertices data structure)
            If set, the centers are extracted from the vertices.

        from_trixels: bool
            If true and dataframe contains trixel column, corners are extracted from trixels.

        Returns
        ----------
        corners : numpy array
            Corners of the trixels in lon/lat representation. First dimension are the SIDs,
            second dimension the corners (1 through 3), third dimension lon/lat.

        Examples
        ----------
        # >>> sids = np.array([3458764513820540928])
        # >>> df = starepandas.STAREDataFrame(sids=sids)
        # >>> df.trixel_corners()
        array([[[-170.26439001,  29.9999996 ],
                [ -45.        ,  45.00000069],
                [  80.26439001,  29.9999996 ]]])
        """

        if vertices:
            corners = starepandas.tools.trixel_conversions.vertices2corners(vertices)
        elif from_trixels and self._trixel_column_name in self.columns:
            corners = []
            for trixel in self[self._trixel_column_name]:
                # Trixel is a polygon. Its first element is the outer ring.
                corners.append(tuple(trixel[0].boundary.coords)[0:3])
        else:
            corners = starepandas.tools.trixel_conversions.to_corners(self[self._sid_column_name])
        return corners

    def trixel_corners_ecef(self, vertices=None):
        """ Returns ECEF norm vectors of great circles constraining the trixels.

        If vertices is set, the trixel corners are extracted from vertices  (c.f. :func:`~trixel_vertices`).
        If not, corners are generated from stare column.

        Parameters
        ----------
        vertices : tuple (vertices data structure)
            If set, the centers are extracted from the vertices.

        Returns
        ----------
        corners : numpy array
            Corners of the trixels in ECEF representation. First dimension are the sids, second
            dimension the great circles, third dimension x/y/z

        Examples
        ----------
        # >>> sids = np.array([3458764513820540928])
        # >>> df = starepandas.STAREDataFrame(sids=sids)
        # >>> df.trixel_corners_ecef()
        array([[[-0.85355339, -0.14644661,  0.49999999],
                [ 0.49999999, -0.49999999,  0.70710679],
                [ 0.14644661,  0.85355339,  0.49999999]]])
        """
        corners = self.trixel_corners(vertices)
        corners_ecef = starepandas.tools.trixel_conversions.corners2ecef(corners)
        return corners_ecef

    def trixel_grings(self, vertices=None):
        """ Returns corners of trixels as ECEF.

        If vertices is set, the trixel corners are extracted from vertices  (c.f. :func:`~trixel_vertices`).
        If not, corners are generated from stare column

        Parameters
        ----------
        vertices : tuple (vertices data structure)
            If set, the centers are extracted from the vertices.

        Returns
        ----------
        corners : numpy array
            ECEF norm vectors of great circles constraining the trixels. First dimension are the sids, second
            dimension the great circles, third dimension x/y/z

        Examples
        ----------
        # >>> sids = np.array([3458764513820540928])
        # >>> df = starepandas.STAREDataFrame(sids=sids)
        # >>> df.trixel_grings()
        array([[[ 0.14644661,  0.85355339,  0.49999999],
                [-0.85355339, -0.14644661,  0.49999999],
                [ 0.49999999, -0.49999999,  0.70710679]]])
        """

        corners = self.trixel_corners_ecef(vertices)
        gring = starepandas.tools.trixel_conversions.corners2gring(corners)
        return gring

    def split_antimeridian(self, inplace=False, drop=False, trixel_column_name=None):
        """Splits trixels at the antimeridian

        This works on trixels that cross the meridian and whose longitudes have *not* been wrapped around the
        antimeridian. I.e. when creating the trixels use sdf.make_trixels(wrap_lon=False)


        Examples
        ----------
        # >>> cities = {'name': ['midway', 'Fiji', 'Baker', 'honolulu'],
        # ...           'lat': [28.2, -17.8,  0.2, 21.3282956],
        # ...           'lon': [-177.35, 178.1, -176.7, -157.9]}
        # >>> sdf = starepandas.STAREDataFrame(cities)
        # >>> sids = starepandas.sids_from_xy(sdf.lon, sdf.lat, level=1)
        # >>> sdf.set_sids(sids, inplace=True)
        # >>> trixels = sdf.make_trixels(wrap_lon=False)
        # >>> sdf.set_trixels(trixels, inplace=True)
        # >>> cites_split = sdf.split_antimeridian(inplace=False)
        # >>> max(max(cites_split.trixels[1].geoms[0].exterior.xy))
        # 180.0

        """
        if inplace:
            df = self
        else:
            df = self.__deepcopy__()

        if not trixel_column_name:
            trixel_column_name = df._trixel_column_name

        trixels = geopandas.GeoSeries(df[trixel_column_name])
        split = starepandas.tools.trixel_conversions.split_antimeridian_series(trixels, drop=drop)

        df[df._trixel_column_name] = split

        if not inplace:
            return df

    def plot(self, trixels=True, boundary=True, **kwargs):
        """ Generate a plot with matplotlib.
        Seminal method to
        `GeoDataFrame.plot() <https://geopandas.org/docs/reference/api/geopandas.GeoDataFrame.plot.html>`_
        All GeoDataFrame.plot() kwargs are available.

        Parameters
        ----------
        trixels: bool
            Toggle if trixels (rather than the SF geometry) is to be plotted
        boundary: bool
            Toggle if the ring is to be plotted as a linestring rather than the polygon. Only relevant if trixels==True

        Examples
        --------
        # >>> import starepandas
        # >>> import geopandas
        # >>> world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
        # >>> germany = world[world.name=='Germany']
        # >>> germany = starepandas.STAREDataFrame(germany, add_sids=True, level=8, add_trixels=True, n_partitions=1)
        # >>> ax = germany.plot(trixels=True, boundary=True, color='y', zorder=0)
        """
        df = self.__deepcopy__()

        if trixels:
            if not self.has_trixels():
                raise AttributeError('No trixels set (expected in "{}" column)'.format(self._trixel_column_name))
            df.set_geometry(self._trixel_column_name, inplace=True)
            if boundary:
                df = df[df.geometry.is_empty == False]
                df = df.set_geometry(df.geometry.boundary)
        else:
            df.set_geometry(self._geometry_column_name, inplace=True)
        return geopandas.plotting.plot_dataframe(df, **kwargs)

    def to_scidb(self, connection):
        pass

    def stare_intersects(self, other, method='binsearch', n_partitions=1, num_workers=None):
        """Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        each geometry that intersects `other`.
        An object is said to intersect `other` if its `ring` and `interior`
        intersects in any way with those of the other.

        Parameters
        -------------
        other: int or listlike
            The SID collection representing the spatial object to test if is intersected.
        method: str
            Method for STARE intersects test 'skiplist', 'binsearch' or 'nn'. Default: 'binsearch'.
        n_partitions: int
            number of dask dataframe partitions to use
        num_workers: int:
            number of dask workers to use

        Examples
        --------
        # >>> germany = [4251398048237748227, 4269412446747230211, 4278419646001971203,
        # ...            4539628424389459971, 4548635623644200963, 4566650022153682947]
        # >>> cities = {'name': ['berlin', 'madrid'], 'sid': [4258121269174388239, 4288120002905386575]}
        # >>> cities = starepandas.STAREDataFrame(cities, sids='sid')
        # >>> cities.stare_intersects(germany)
        0     True
        1    False
        dtype: bool
        """

        if isinstance(other, (int, np.int64)):
            # Other is a single STARE index value
            other = [other]
        elif isinstance(other, (np.ndarray, list)):
            # Other is a collection/set of STARE index values
            pass
        else:
            raise ValueError("Other must be array-like object or int64")

        intersects = starepandas.series_intersects(other=other,
                                                   series=self[self._sid_column_name],
                                                   method=method,
                                                   n_partitions=n_partitions,
                                                   num_workers=num_workers)
        return pandas.Series(intersects, index=self.index)

    def stare_disjoint(self, other, method='binsearch', n_partitions=1, num_workers=None):
        """  Returns a ``Series`` of ``dtype('bool')`` with value ``True`` for
        each geometry that is disjoint from `other`.
        This is the inverse operation of STAREDataFrame.stare_intersects()

        Parameters
        ------------
        other: array-like
            The STARE index collection representing the spatial object to test if is intersected.
        method: str
            Method for STARE intersects test 'skiplist', 'binsearch' or 'nn'. Default: 'binsearch'.
        n_partitions: int
            number of dask dataframe partitions to use
        num_workers: int:
            number of dask workers to use

        See also
        --------
        STAREDataFrame.stare_intersects : intersects test

        """
        return ~self.stare_intersects(other, method, n_partitions, num_workers)

    def stare_intersection(self, other):
        """Returns a ``STARESeries`` of the (STARE) spatial intersection of self with `other`.

        Parameters
        ------------
        other : Array-like
            The STARE index value collection representing the object to find the intersection with.

        Returns
        --------
        intersection : STARESeries
            A series of STARE index values representing the STARE interesection of each feature with other

        Examples
        ---------
        # >>> import shapely
        # >>> nodes1 = [[102, 33], [101, 35], [105, 34], [104, 33], [102, 33]]
        # >>> nodes2 = [[102, 34], [106, 35], [106, 33], [102, 33.5], [102, 34]]
        # >>> polygon1 = shapely.geometry.Polygon(nodes1)
        # >>> polygon2 = shapely.geometry.Polygon(nodes2)
        # >>> sids1 = starepandas.sids_from_polygon(polygon1, level=5, force_ccw=True)
        # >>> sids2 = starepandas.sids_from_polygon(polygon2, level=5, force_ccw=True)
        #
        # >>> df = starepandas.STAREDataFrame(sids=[sids1])
        # >>> df.stare_intersection(sids2).iloc[0]
        # array([694117292568477701, 701435641962954757, 701998591916376069])
        """
        data = []
        for srange in self[self._sid_column_name]:
            data.append(pystare.intersection(srange, other))
        return pandas.Series(data, index=self.index)

    def stare_dissolve(self, by=None, num_workers=1, geom=False, aggfunc="first", **kwargs):
        """
        Dissolves a dataframe subject to a field. I.e. grouping by a field/column.
        Seminal method to [GeoDataFrame.dissolve()](https://geopandas.org/en/stable/docs/user_guide/aggregation_with_dissolve.html)

        stare_dissolve() can be thought of as doing three things:
        - it dissolves all the SIDs within a given group together into a single set o SIDs (this means a) removing duplicate SIDs b) replacing 4 child SIDs with the parent SID), and
        - it aggregates all the rows of data in a group using groupby.aggregate, and
        - it combines those two results.

        Parameters
        -------------
        by: str
            column to use the dissolve on. If None, dissolve all rows.
        num_workers: int
            workers to use for the dissolve
        geom: bool
            Toggle if the geometry column is to be dissolved. Geom column Will be dropped if set to False.
        aggfunc: str
            aggregation function. E.g. 'first', 'sum', 'mean'.

        Examples
        --------
        # >>> import geopandas
        # >>> world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
        # >>> west = world[world['continent'].isin(['Europe', 'North America'])]
        # >>> west = starepandas.STAREDataFrame(west, add_sids=True, level=4, add_trixels=False)
        # >>> west.stare_dissolve(by='continent', aggfunc='sum') # doctest: +SKIP
        #                                                            stare  ...  gdp_md_est
        # continent                                                         ...
        # Europe         [648518346341351428, 900719925474099204, 10448...  ...  25284877.0
        # North America  [1170935903116328964, 1173187702930014212, 117...  ...  23505137.0
        """
        if by is None:
            sids = self[self._sid_column_name].to_numpy()
            if sids.dtype == np.dtype('O'):
                # If we receive a series of SID collections we merge all sids into a single 1D array
                # to_numpy() would have produced an array of lists in this case
                sids = np.concatenate(sids)
            sids = starepandas.compress_sids(sids)
            return sids
        else:
            data = self.drop(columns=[self._sid_column_name, self._trixel_column_name], errors='ignore')
            if geom:
                aggregated_data = data.dissolve(by=by, aggfunc=aggfunc, **kwargs)
            else:
                data = data.drop(columns=[self._geometry_column_name], errors='ignore')
                aggregated_data = data.groupby(by=by, **kwargs).agg(aggfunc)

        sids_groups = self.groupby(group_keys=True, by=by)[self._sid_column_name]

        if num_workers == 1:
            dissolved = []
            for group in sids_groups:
                dissolved.append(compress_sids_group(group))
        else:
            with multiprocessing.Pool(processes=num_workers) as pool:
                dissolved = pool.map(compress_sids_group, [group for group in sids_groups])

        sdf = STAREDataFrame(dissolved, columns=[by, self._sid_column_name])
        # NB: set_index(inplace=True) downcasts the subclass back to a plain
        # (Geo)DataFrame under current pandas, which would route set_sids to the
        # inplace-rejecting fallback. Use the non-inplace forms; set_sids then
        # re-wraps to a STAREDataFrame.
        sdf = sdf.set_index(by)
        sdf = sdf.set_sids(self._sid_column_name)

        aggregated = sdf.join(aggregated_data)
        aggregated.__class__ = STAREDataFrame
        return aggregated

    def spatial_level(self):
        """
        Returns the spatial level of each feature
        """
        sids = self[self._sid_column_name]
        return pystare.spatial_resolution(sids)

    def trixel_area(self, r=None):
        """
        Returns the approximate area of the trixel

        Parameters
        -------------
        r: float or int
             earth radius
        """
        sids = self[self._sid_column_name]
        solid_angel = pystare.to_area(sids)
        if r is None:
            return solid_angel
        else:
            return solid_angel * r ** 2

    def to_sids_level(self, level, inplace=False, clear_to_level=False):
        """
        Changes level of STARE index values to level; optionally clears location bits up to level.
        Caution: This method is not intended for use on features represented by sets of sids.

        Parameters
        ------------
        inplace: bool
            If True, modifies the DataFrame in place (do not create a new object).
        level: int
            STARE level to change to.
        clear_to_level: bool
            Toggle if the location bits below level should be cleared

        Returns
        -------------
        if not inplace, returns stare index values, otherwise None

        Examples
        --------
        # >>> sids = [2299437706637111721, 2299435211084507593, 2299566194809236969]
        # >>> sdf = starepandas.STAREDataFrame(sids=sids)
        # >>> sdf.to_sids_level(level=6, clear_to_level=False)
        #                   sids
        # 0  2299437706637111718
        # 1  2299435211084507590
        # 2  2299566194809236966
        """

        if inplace:
            df = self
        else:
            df = self.__deepcopy__()

        sids = df[df._sid_column_name]
        if pandas.api.types.is_integer_dtype(sids):
            # We have column of single SIDs and can send whole column to pystare
            sids = sids.astype(np.dtype('int64'))
            sids = pystare.spatial_coerce_resolution(sids, level)

            if clear_to_level:
                # pystare_terminator_mask uses << operator, which requires us to cast to numpy array first
                sids = pystare.spatial_clear_to_resolution(np.array(sids))
        else:
            pass

        df[df._sid_column_name] = sids
        if not inplace:
            return df

    def clear_to_level(self, inplace=False):
        """
        Clears location bits to level

        Parameters
        -----------
        inplace: bool
            If True, modifies the DataFrame in place (do not create a new object).

        Examples
        ----------
        # >>> sids = [2299437706637111721, 2299435211084507593, 2299566194809236969]
        # >>> sdf = starepandas.STAREDataFrame(sids=sids)
        # >>> sdf.clear_to_level(inplace=False)
        #                   sids
        # 0  2299437254470270985
        # 1  2299435055447015433
        # 2  2299564797819093001

        """
        if inplace:
            df = self
        else:
            df = self.__deepcopy__()

        sids = df[df._sid_column_name]
        sids = pystare.spatial_clear_to_resolution(np.array(sids))

        df[df._sid_column_name] = sids
        if not inplace:
            return df

    def to_sids_singlelevel(self, level=None, inplace=False):
        """
        Changes the STARE index values to single level representation (in contrary to multiresolution).

        Parameters
        -----------
        level: int
            level to change the sids to
        inplace: bool
            If True, modifies the DataFrame in place (do not create a new object).

        Returns
        ------------
        if not inplace, returns stare index values, otherwise None

        Examples
        ---------
        # >>> import geopandas
        # >>> world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
        # >>> germany  = world[world.name=='Germany']
        # >>> germany = starepandas.STAREDataFrame(germany, add_sids=True, level=6, add_trixels=False)
        # >>> len(germany.sids.iloc[0])
        # 43
        # >>> germany_singleres = germany.to_sids_singlelevel()
        # >>> len(germany_singleres.sids.iloc[0])
        # 46
        """

        if inplace:
            df = self
        else:
            df = self.__deepcopy__()

        sids_col = df[df._sid_column_name]

        new_sids_col = []
        for sids in sids_col:
            if level:
                r = level
            else:
                r = int(pystare.spatial_resolution(sids).max())
            sids = pystare.expand_intervals(sids, level=r, multi_resolution=False)
            new_sids_col.append(sids)

        df[df._sid_column_name] = new_sids_col
        if not inplace:
            return df

    def hex(self):
        """
        Returns the hex16 representation of the stare column

        Examples
        ---------
        # >>> sdf = starepandas.STAREDataFrame(sids=[2251799813685252, 4503599627370500])
        # >>> sdf.hex()
        # ['0x0008000000000004', '0x0010000000000004']
        #
        # >>> sdf = starepandas.STAREDataFrame(sids=[[2251799813685252, 4503599627370500],
        # ...                                        [4604930618986332164, 4607182418800017412]])
        # >>> sdf.hex()
        # [['0x0008000000000004', '0x0010000000000004'], ['0x3fe8000000000004', '0x3ff0000000000004']]
        """

        sids = []
        for row in self[self._sid_column_name]:
            try:
                # Ducktyping collection of sids
                sids.append(list(map(pystare.int2hex, row)))
            except TypeError:
                sids.append(pystare.int2hex(row))
        return sids

    def write_pods_spatial(self, pod_root, level, chunk_name, hex=True, path_format=None, append=False,
                           compress=None
                           ):
        pod_path_format = '{pod_root}/{pod}'
        path_format = '{pod_path_format}/{chunk_name}' if path_format is None else path_format
        pods_written = []

        grouped = self.groupby(self.to_sids_level(level=level, clear_to_level=True)[self._sid_column_name])
        for group in grouped.groups:
            # print('group: ',group,type(group),grouped.get_group(group).size)
            if group < 0:
                continue
            g = grouped.get_group(group)
            if hex:
                pod = pystare.int2hex(group)
            else:
                pod = group

            # Original
            # g.to_pickle('{pod_root}/{pod}/{chunk_name}'.format(pod_root=pod_root, pod=pod, chunk_name=chunk_name))

            # New MLR 2022-1117-1
            # Note: with the following approach we could update a headr that includes extent information.
            #
            dname = pod_path_format.format(pod_root=pod_root, pod=pod)
            if not Path(dname).exists():
                Path(dname).mkdir()

            fname = path_format.format(pod_path_format=dname, chunk_name=chunk_name)
            write_pod_pickle(g, fname, append, compress)
            pods_written.append(fname)

        return pods_written

    def write_pods_granule(self, pod_root, level, chunk_name, hex=True, path_format=None, append=False,
                           compress=None
                           ):
        start0 = time.time()
        pod_path_format = '{pod_root}/{pod}'
        path_format = '{pod_path_format}/{tchunk_name}-{chunk_name}' if path_format is None else path_format

        pods_written = []

        start = time.time()
        grouped = self.groupby(self.to_sids_level(level=level, clear_to_level=True)[self._sid_column_name])
        logging.info('Grouping chunk %s took %d seconds.' % (chunk_name, time.time() - start))

        for group in grouped.groups:

            # Future            
            # self.write_pods_granule_group(self,(group,pod_path_format,pod_root,chunk_name))            
            # print('group: ',group,type(group),grouped.get_group(group).size)
            if group < 0:  # This cannot be right. group is a dictionary.
                continue

            start = time.time()
            g = grouped.get_group(group)
            logging.info('Get group %s took %d seconds.' % (group, time.time() - start))

            if hex:
                pod = pystare.int2hex(group)
            else:
                pod = group

            dname = pod_path_format.format(pod_root=pod_root, pod=pod)
            if not Path(dname).exists():
                Path(dname).mkdir()
                pass

            # One might cheat and use the fact that ts_start and ts_end are for the granule, so index to [0]
            start = time.time()
            t_mnmx = (self['ts_start'].min(), self['ts_end'].max())
            logging.info('Get group %s min/max took %d seconds.' % (group, time.time() - start))

            start = time.time()
            dt_mnmx = [t.to_pydatetime() for t in t_mnmx]
            ds_tid = pystare.tiv_from_datetime2(dt_mnmx)
            logging.info('Get group %s min/max tiv took %d seconds.' % (group, time.time() - start))

            # ds_tpod = pystare.make_tpod_tuple(ds_tid,temporal_resolution)
            # tpod        = pystare.hex16(ds_tpod[0])
            tchunk_name = pystare.hex16(ds_tid)
            fname = path_format.format(pod_path_format=dname, chunk_name=chunk_name, tchunk_name=tchunk_name)
            write_pod_pickle(g, fname, append, compress)
            pods_written.append(fname)

        logging.info('write_pods_granule chunk %s took %d seconds total.' % (chunk_name, time.time() - start0))
        return pods_written

    def write_pods_tpod(self, pod_root, level, chunk_name, hex=True, path_format=None, append=False,
                        temporal_chunking_resolution=16, compress=None
                        ):
        """
        Parameters
        ----------
        pod_root: str
        level: int
        chunk_name: str
        hex: bool
            toggle hex
        path_format
        append: bool
        temporal_chunking_resolution: int
            defaults to 16 (28 days)

        Returns
        -------

        """

        """
        TODO: Add temporal partitioning. Currently broken.
        """
        raise NotImplementedError

        pod_path_format = '{pod_root}/{pod}'
        path_format = '{pod_path_format}/{tpod_name}-{tchunk_name}-{chunk_name}' if path_format is None else path_format
        pods_written = []

        grouped = self.groupby(self.to_sids_level(level=level, clear_to_level=True)[self._sid_column_name])
        for group in grouped.groups:
            # print('group: ',group,type(group),grouped.get_group(group).size)
            if group < 0:  # cannot be right. group is a dictionary
                continue
            g = grouped.get_group(group)
            if hex:
                pod = pystare.int2hex(group)
            else:
                pod = group

            dname = pod_path_format.format(pod_root=pod_root, pod=pod)
            if not Path(dname).exists():
                Path(dname).mkdir()
                pass

            t_mnmx = self.ts_start.min(), self.ts_end.max()
            dt_mnmx = [t.to_pydatetime() for t in t_mnmx]
            ds_tid = pystare.tiv_from_datetime2(dt_mnmx)

            tpod_name = pystare.format_tpod(pystare.make_tpod_tuple(ds_tid, temporal_chunking_resolution))

            # ds_tpod = pystare.make_tpod_tuple(ds_tid,temporal_resolution)
            # tpod        = pystare.hex16(ds_tpod[0])
            tchunk_name = pystare.hex16(ds_tid)
            fname = path_format.format(pod_path_format=dname,
                                       tpod_name=tpod_name,
                                       tchunk_name=tchunk_name,
                                       chunk_name=chunk_name)
            write_pod_pickle(g, fname, append, compress)
            pods_written.append(fname)

        return pods_written

    ### Just stashing this here for the moment.

    ### if temporal_chunking:
    ###     # link other pods to this one? sigh... no chunking really.
    ###     tpods = pystare.pods_in_query(ds_tid,temporal_resolution)
    ###     for tp_ in tpods:
    ###         tp=pystare.hex16(tp_[0])
    ###         if tp != tpod:
    ###             dst_name = path_format.format(pod_root=pod_root
    ###                                    , pod=pod
    ###                                    , chunk_name=chunk_name
    ###                                    , tpod=tp
    ###                                    , tchunk_name=tchunk_name
    ###                                    )
    ###             os.symlink(fname,dst_name) # creates dst_name symlinking to fname (the src)

    def write_pods(self, pod_root, level, chunk_name, hex=True, path_format=None, append=False,
                   temporal_chunking=None, compress=None):
        """ Writes dataframe into a STAREPods hierarchy.

        Appends the dataframe to the pod (pickle), if it exists.

        Returns list of pods written.

        Parameters
        --------------
        pod_root: str
            Root directory of STAREPods
        level: int
            level of STAREPods
        chunk_name: str
            name of the pod
        hex: bool
            toggle pods being hex vs int
        path_format: str
            defines how pods are to be named 
            default: '{pod_root}/{pod}/{chunk_name}'
        append: bool
            toggle appending to existing pods (default: False)
            Not implemented.
        temporal_chunking: dict
            toggle writing into temporal pods (default: None)
            Supported options...
            - {'partitioning':'granule'}
            - {'partitioning':'pod','resolution':16 } # 16 => month chunk (28 days)
        """

        if temporal_chunking is None:
            return self.write_pods_spatial(pod_root=pod_root, level=level, chunk_name=chunk_name, hex=hex,
                                           path_format=path_format, append=append, compress=compress)
        elif temporal_chunking['partitioning'] == 'granule':
            return self.write_pods_granule(pod_root=pod_root, level=level, chunk_name=chunk_name, hex=hex,
                                           path_format=path_format, append=append, compress=compress)
        elif temporal_chunking['partitioning'] == 'pod':
            return self.write_pods_tpod(pod_root=pod_root, level=level, chunk_name=chunk_name, hex=hex,
                                        path_format=path_format, append=append, compress=compress,
                                        temporal_chunking_resolution=temporal_chunking['resolution'])
        else:
            raise (Exception('Pod configuration not supported. temporal_chunking = %s' % (temporal_chunking)))

    @property
    def _constructor(self):
        return STAREDataFrame

    def to_array(self, column, shape=None, pivot=False):
        """Converts the 'column' to a numpy array.

        Either a shape argument has to be provided or the dataframe has to contain a column x and y
        holding the original array coordinates.

        If the dataframe has x/y columns, the column can also be pivoted. I.e. rather than
        reshaping according to the shape, pivoted along the x/y columns.
        This may be relevant if the dataframe's row order has changed.

        Parameters
        ----------
        column: str
            column name to be converted to an array
        shape: tuple
            x and y shape of the array. x*y has to equal the length of the dataframe
        pivot: bool
            if true, rather than simple reshaping, the dataframe is pivoted along the x and y column

        Examples
        ----------
        # >>> df = starepandas.STAREDataFrame({'x': [0, 0, 1, 1],
        # ...                                  'y': [1, 0, 0, 1],
        # ...                                  'a': [1, 2, 3, 4]})
        # >>> df.to_array('a', pivot=False)
        # array([[1, 2],
        #        [3, 4]])
        #
        # >>> df.to_array('a', pivot=True)
        # array([[2, 1],
        #        [3, 4]])
        #
        # See also
        --------
        STAREDataFrame.to_arrays

        """
        if shape is None:
            shape = (max(self['x']) + 1, max(self['y']) + 1)

        if pivot:
            array = self.pivot(index='x', columns='y', values=column).to_numpy()
        else:
            array = self[column].to_numpy().reshape(shape)
        return array

    def to_sids_array(self, shape=None, pivot=False):
        return self.to_array(self._sid_column_name, shape, pivot)

    def to_arrays(self, shape=None, pivot=False):
        """ Converts a STAREDataFrame into a dictionary of arrays; one array per column/field.

        This may be useful to write data back to granules.
        Either a shape argument has to be provided or the dataframe has to contain a column x and y
        holding the original array coordinates.
        If no shape is provided, the shape is assumed to be (max(x)+1, max(y)+1).

        If the dataframe has x/y columns, the column can also be pivoted. I.e. rather than
        reshaping according to the shape, pivoted along the x/y columns.
        This may be relevant if the dataframe's row order has changed.

        Parameters
        ----------
        shape: tuple
            x and y shape of the array. x*y has to equal the length of the dataframe
        pivot: bool
            if true, rather than simple reshaping, the dataframe is pivoted along the x and y column

        See also
        ---------
        STAREDataFrame.to_array
        """

        arrays = {}

        for column in self.columns:
            if column in ['x', 'y']:
                continue
            arrays[column] = self.to_array(column, shape=shape, pivot=pivot)

        return arrays

    def to_s3(self, s3_path, level, chunk_size=250000, storage_options=None,
                   dataset=None, data_level=None, raw_collected_time=None, metadata=None,
                   conn=None, granule_name=None):
        """
        Partition STAREDataFrame by SIDs at specified level and write to S3 as
        one Parquet file per partition.

        Quaternary layout (``docs/quaternary_storage_plan.md`` §2) — S3 is
        **flat**: every chunk lands directly under ``s3_path`` with a
        self-describing filename whose pod-code prefix doubles as the spatial
        query prefix::

            s3_path/<podcode>-<granule_name>-<dataset>.parquet

        e.g. ``s3://zarrpods/storage/q13211-1C.GPM.GMI.…V07B-GMI_S1.parquet``.
        When ``granule_name`` is omitted the granule component defaults to
        ``"data"``.  (Local writes use the hierarchical pod-code dir tree — see
        :meth:`to_local`.)

        Each Parquet file contains all DataFrame columns for that partition
        plus a ``__row_positions__`` column that preserves original row order
        for reconstitution. ``pixel_width`` and ``granule_name`` (when present
        in ``metadata``) are stored in the Parquet file's key-value metadata.

        Note: on-disk format is Parquet.

        Parameters
        ----------
        s3_path : str
            S3 path where the storage root will be created (e.g., "s3://bucket/granule_name")
        level : int
            STARE level for partitioning SIDs
        chunk_size : int, optional
            Unused; retained for API compatibility (default: 250000)
        storage_options : dict, optional
            S3 storage options including credentials and region
        dataset : str, optional
            Dataset name to record in metadata table
        data_level : str, optional
            Data level string to record in metadata table
        raw_collected_time : datetime, optional
            Timestamp when raw data was collected; defaults to UTC now if not provided
        metadata : dict, optional
            Additional metadata to store in the JSON field
        conn : psycopg2 connection, optional
            Existing database connection to reuse. If None, a new connection is created
            and closed at the end. If provided, the caller is responsible for closing it.
        granule_name : str, optional
            When set, splice this segment between the HTM partition path and the
            dataset leaf so multiple granules covering the same partition coexist
            as sibling Parquet files. Mirrors the layout produced by
            :meth:`to_local`. Must not contain ``/``.

        Returns
        -------
        str
            The S3 path where data was written
        """
        if granule_name is not None and '/' in granule_name:
            raise ValueError(f"granule_name must not contain '/': {granule_name!r}")
        # Resolve storage options: use per-call options over configured defaults
        merged_opts = dict(_AWS_S3_STORAGE_OPTIONS)
        if not merged_opts:
            # Attempt to auto-load default config file if available
            _load_config_from_default_locations()
            merged_opts = dict(_AWS_S3_STORAGE_OPTIONS)
        if storage_options:
            merged_opts.update(storage_options)
        if not merged_opts:
            raise ValueError(
                "Missing S3 configuration. Call load_aws_configure(config_path) or aws_configure(...) "
                "to set credentials/region, or pass storage_options to to_s3."
            )

        # Early exit for empty DataFrames
        if len(self) == 0:
            print("Warning: Cannot write empty DataFrame to S3")
            return s3_path

        # Cap partition level to avoid extreme fragmentation (thousands of tiny groups)
        partition_level = min(level, MAX_PARTITION_LEVEL)
        if partition_level < level:
            print(f"Note: Partition level capped from {level} to {partition_level} for optimal chunk size. "
                  f"SID data retains full level {level} resolution.")

        # Compute coerced SIDs for grouping without deep-copying the entire DataFrame
        sids_array = self[self._sid_column_name].to_numpy().astype(np.int64, copy=False)
        coerced_sids = pystare.spatial_coerce_resolution(sids_array, partition_level)
        coerced_sids = pystare.spatial_clear_to_resolution(coerced_sids)
        grouped = self.groupby(coerced_sids, sort=False)

        # Record original row order so we can reconstruct the exact order on read
        original_positions = pandas.Series(np.arange(len(self), dtype=np.int64), index=self.index)

        # Prepare RDS connection and metadata defaults
        owns_conn = conn is None
        if conn is None:
            conn = _ensure_rds_db_and_table('StarePodsMetadata')
        bucket_name = _parse_s3_bucket(s3_path)
        # §C10 #2 fix: refuse the silent utcnow() default. Per-granule
        # callers (io.granules.to_s3) derive the timestamp from the
        # filename. Direct STAREDataFrame.to_s3 callers must pass an
        # explicit timestamp — without one, retries produce divergent
        # rows that the §C10 #1 UNIQUE constraint cannot dedup.
        if raw_collected_time is None:
            raise ValueError(
                "raw_collected_time is required for STAREDataFrame.to_s3. "
                "For granule ingest, use starepandas.io.granules.to_s3 which "
                "derives a per-granule timestamp from the filename. For "
                "custom DataFrames, pass an explicit datetime — defaulting "
                "to utcnow() would break idempotency under retries (§C10 #2)."
            )
        ts = raw_collected_time
        base_meta = dict(metadata or {})

        # Collect metadata rows for batch insert at the end
        metadata_rows = []

        num_groups = len(grouped)
        print(f"Writing {num_groups} Parquet partitions to S3...")

        # Build a single s3fs filesystem instance and reuse it across all writes
        # so each partition is one S3 PUT.
        import pyarrow as pa
        import pyarrow.parquet as pq
        parquet_fs = s3fs.S3FileSystem(**merged_opts)

        # Write each partition as a single Parquet file using hierarchical paths
        written_count = 0
        for group_id, gdf in grouped:
            # Skip invalid groups if any
            if isinstance(group_id, (int, np.integer)) and group_id < 0:
                continue

            written_count += 1
            if written_count % 50 == 0:
                print(f"  Progress: {written_count}/{num_groups} partitions written...")

            # Quaternary layout (docs/quaternary_storage_plan.md §2): S3 is
            # FLAT — every chunk lands directly under the storage root, keyed by
            # a self-describing filename whose pod-code prefix doubles as the
            # spatial query prefix:
            #   <s3_path>/<podcode>-<granule_basename>-<dataset>.parquet
            podcode = sid_to_podcode(group_id)
            gbase = granule_name if granule_name is not None else "data"
            fname = chunk_filename(podcode, gbase, dataset or 'data')
            group_path = f"{s3_path}/{fname}"

            # Build a Table containing every column plus __row_positions__ to
            # preserve original row order. Match the legacy coercion:
            # anything that lands in a numpy object array (e.g. shapely
            # geometries, mixed-type columns) becomes a string column so
            # PyArrow can serialize it.
            row_pos = original_positions.loc[gdf.index].to_numpy(dtype=np.int64)
            arrays = {}
            for col in self.columns:
                values = gdf[col].to_numpy()
                if values.dtype == np.dtype('O'):
                    values = values.astype('U')
                arrays[col] = values
            arrays['__row_positions__'] = row_pos
            table = pa.table(arrays)

            # Embed pixel_width / granule_name in Parquet file-level metadata so
            # reconstitution can recover them without consulting the metadata DB.
            kv_md = {}
            if 'pixel_width' in base_meta:
                kv_md[b'pixel_width'] = str(int(base_meta['pixel_width'])).encode()
            if base_meta.get('granule_name'):
                kv_md[b'granule_name'] = str(base_meta['granule_name']).encode()
            if kv_md:
                existing = dict(table.schema.metadata or {})
                existing.update(kv_md)
                table = table.replace_schema_metadata(existing)

            # s3fs paths are protocol-stripped when passed as filesystem=
            parquet_path = group_path[len('s3://'):] if group_path.startswith('s3://') else group_path
            pq.write_table(
                table,
                parquet_path,
                filesystem=parquet_fs,
                compression='zstd',
                compression_level=3,
            )

            # Collect metadata for batch insert
            meta_row = dict(base_meta)
            meta_row.update({
                'grouped_id_full': group_id,
                'group_path': group_path,
                'num_rows': int(len(gdf)),
                'columns': list(arrays.keys()),
            })
            t_start, t_end = _chunk_temporal_range(gdf)
            metadata_rows.append(PartitionRow(
                dataset=dataset,
                data_level=data_level,
                raw_collected_time=ts,
                grouped_id=group_id,
                s3_bucket=bucket_name,
                resolution_level=int(partition_level),
                metadata_json=meta_row,
                t_start=t_start,
                t_end=t_end,
                podcode=podcode,
            ))

        # Persist metadata via the MetadataStore abstraction (§C9 M4 hedge —
        # localises the RDS↔DynamoDB swap point).
        if metadata_rows:
            store = RDSMetadataStore(conn=conn)
            inserted = store.write_partitions(metadata_rows)
            print(f"✓ Inserted {inserted} metadata rows into RDS")
        else:
            print("Warning: No valid STARE groups found to write")

        # Close the database connection only if we created it
        if owns_conn:
            try:
                conn.close()
            except Exception as e:
                print(f"Warning: Failed to close database connection: {e}")

        print(f"✓ Finished writing {num_groups} Parquet partitions to {s3_path}")
        return s3_path
    
    def to_local(self, local_path, level, chunk_size=250000, pixel_width=None,
                      db_path=None, dataset=None, data_level=None, granule_name=None,
                      raw_collected_time=None):
        """
        Partition STAREDataFrame by SIDs at specified level and write to the
        local filesystem as one Parquet file per partition.

        Layout (HTM-subtree, with optional granule segment, Parquet leaf)::

            local_path/Q00_X/Q01_Y/.../QN_M/[<granule_name>/]<dataset_name>.parquet

        When ``db_path`` is provided the function also records one metadata row
        per Parquet partition into a SQLite ``PodsMetadata`` table (same schema
        as the S3/RDS version but using ``LocalPath`` instead of ``S3 bucket``).

        Note: on-disk format is Parquet.

        Parameters
        ----------
        local_path : str
            Local root directory; HTM subtree is built underneath.
        level : int
            STARE level for partitioning SIDs.
        chunk_size : int, optional
            Unused (retained for backward compatibility with callers; Parquet
            row groups are sized by PyArrow defaults).
        pixel_width : int, optional
            Number of across-track pixels per scanline. Stored in the Parquet
            file's key-value metadata so :func:`reconstitute_hdf5_from_local`
            can rebuild the 2D HDF5 structure.
        db_path : str, optional
            Path to the SQLite database file. When provided, metadata rows are
            inserted after all Parquet files are written.
        dataset : str, optional
            Dataset name recorded in the SQLite metadata (e.g. ``"GMI_S1"``).
        data_level : str, optional
            Data level string recorded in the SQLite metadata (e.g. ``"L1C"``).
        raw_collected_time : datetime, optional
            Granule collection timestamp recorded in the SQLite metadata.
            Part of the row's uniqueness identity (dataset + collection time +
            pod), so passing a deterministic value keeps re-ingest
            idempotent — ``starepandas.io.granules.to_local`` derives one from
            the granule filename. Defaults to UTC now (each write then
            produces distinct rows).

        Returns
        -------
        str
            The local path where data was written
        """
        # Ensure root directory exists
        os.makedirs(local_path, exist_ok=True)

        # Cap partition level to avoid extreme fragmentation (mirrors to_s3 behaviour)
        partition_level = min(level, MAX_PARTITION_LEVEL)
        if partition_level < level:
            logging.debug(
                "to_local: partition level capped from %d to %d; "
                "SID data retains full resolution %d.",
                level, partition_level, level,
            )

        # Group by SIDs at the partition level, preserving encounter order
        sids_array = self[self._sid_column_name].to_numpy().astype(np.int64, copy=False)
        coerced_sids = pystare.spatial_coerce_resolution(sids_array, partition_level)
        coerced_sids = pystare.spatial_clear_to_resolution(coerced_sids)
        grouped = self.groupby(coerced_sids, sort=False)

        # Record original row order
        original_positions = pandas.Series(np.arange(len(self), dtype=np.int64), index=self.index)

        metadata_rows = []
        if raw_collected_time is not None:
            ts_iso = raw_collected_time.isoformat()
        else:
            ts_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

        # Quaternary layout (docs/quaternary_storage_plan.md §2) — local disk is
        # hierarchical (cumulative pod-code dir chain) with a self-describing
        # filename in the leaf:
        #   local_path/q13/q132/q1321/q13211/q13211-<granule_name>-<dataset>.parquet
        # Multiple granules' contributions to the same partition coexist as
        # sibling Parquet files differing only in their <granule_name> span.
        if granule_name is not None and '/' in granule_name:
            raise ValueError(f"granule_name must not contain '/': {granule_name!r}")
        dataset_name = dataset if dataset is not None else "data"

        import pyarrow as pa
        import pyarrow.parquet as pq

        for group_id, gdf in grouped:
            if isinstance(group_id, (int, np.integer)) and group_id < 0:
                continue

            # Quaternary layout (docs/quaternary_storage_plan.md §2): local disk
            # is HIERARCHICAL — the cumulative pod-code dir chain
            # (q13/q132/q1321/q13211/) with the same self-describing filename
            # inside the leaf.  The leaf dir name and the filename's pod-code
            # prefix are intentionally redundant so a chunk is identifiable from
            # its filename alone.
            podcode = sid_to_podcode(group_id)
            local_dirs = podcode_to_local_dirs(podcode)
            gbase = granule_name if granule_name is not None else "data"
            parent_dir = os.path.join(local_path, *local_dirs)
            os.makedirs(parent_dir, exist_ok=True)
            fname = chunk_filename(podcode, gbase, dataset_name)
            group_path = os.path.join(parent_dir, fname)

            # Build a Table containing every column plus __row_positions__ so
            # reconstitution can restore original row order. Match the legacy
            # coercion: anything that lands in a numpy object array
            # (e.g. shapely geometries, mixed-type columns) becomes a string
            # column so PyArrow can serialize it.
            row_pos = original_positions.loc[gdf.index].to_numpy(dtype=np.int64)
            arrays = {}
            for col in self.columns:
                values = gdf[col].to_numpy()
                if values.dtype == np.dtype('O'):
                    values = values.astype('U')
                arrays[col] = values
            arrays['__row_positions__'] = row_pos
            table = pa.table(arrays)

            # Embed pixel_width / granule_name in Parquet file-level metadata.
            kv_md = {}
            if pixel_width is not None:
                kv_md[b'pixel_width'] = str(int(pixel_width)).encode()
            if granule_name is not None:
                kv_md[b'granule_name'] = str(granule_name).encode()
            if kv_md:
                existing = dict(table.schema.metadata or {})
                existing.update(kv_md)
                table = table.replace_schema_metadata(existing)

            pq.write_table(
                table,
                group_path,
                compression='zstd',
                compression_level=3,
            )

            # Collect metadata row for SQLite insertion
            if db_path is not None:
                meta_blob = json.dumps({
                    "grouped_id_full": int(group_id),
                    "group_path": os.path.abspath(group_path),
                    "num_rows": len(gdf),
                    "columns": list(arrays.keys()),
                    "pixel_width": int(pixel_width) if pixel_width is not None else None,
                    "granule_name": granule_name,
                })
                t_start, t_end = _chunk_temporal_range(gdf)
                # Catalog the effective dataset name (same fallback as the
                # chunk filename). A NULL Dataset would never hit the
                # pods_unique upsert — SQLite treats NULLs as distinct — so
                # re-ingest would duplicate rows instead of refreshing.
                metadata_rows.append((
                    dataset_name,
                    data_level,
                    ts_iso,
                    int(group_id),
                    os.path.abspath(local_path),
                    partition_level,
                    meta_blob,
                    t_start.isoformat() if t_start is not None else None,
                    t_end.isoformat() if t_end is not None else None,
                    podcode,
                ))

        # Batch-insert metadata into SQLite. The upsert mirrors the RDS
        # ON CONFLICT path (metadata.py): re-ingest keeps the row count
        # stable and refreshes the temporal range and pod code.
        if db_path is not None and metadata_rows:
            conn = _ensure_sqlite_db_and_table(db_path)
            try:
                conn.executemany(
                    'INSERT INTO "PodsMetadata" '
                    '("Dataset", "DataLevel", "RawData Collected Time", grouped_id, '
                    '"LocalPath", "Resolution level", "MetadataJson", '
                    't_start, t_end, podcode) '
                    'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) '
                    'ON CONFLICT ("Dataset", "RawData Collected Time", grouped_id) '
                    'DO UPDATE SET "MetadataJson" = excluded."MetadataJson", '
                    '"LocalPath" = excluded."LocalPath", '
                    't_start = excluded.t_start, '
                    't_end = excluded.t_end, '
                    'podcode = excluded.podcode',
                    metadata_rows,
                )
                conn.commit()
            finally:
                conn.close()

        return local_path

    def to_hdf5(self, file_path, scan, pixel_width,
                compression='gzip', compression_opts=4, mode='w'):
        """
        Write STAREDataFrame to HDF5 in the original satellite granule format.

        Reconstitutes the 2D/3D structure expected by downstream HDF5-consuming
        tools.  The output layout mirrors the original granule exactly::

            file_path
            └── <scan>/            (e.g. "S1", "S2")
                ├── Latitude       float32  (N_scans, pixel_width)
                ├── Longitude      float32  (N_scans, pixel_width)
                ├── Tc             float32  (N_scans, pixel_width, N_channels)
                └── ScanTime/
                    ├── Year       int16  (N_scans,)
                    ├── Month      int8   (N_scans,)
                    ├── DayOfMonth int8   (N_scans,)
                    ├── Hour       int8   (N_scans,)
                    ├── Minute     int8   (N_scans,)
                    ├── Second     int8   (N_scans,)
                    └── MilliSecond int16  (N_scans,)

        (dtypes match the original GPM/SSMIS L1C granule format.)

        Parameters
        ----------
        file_path : str
            Destination HDF5 file path.
        scan : str
            HDF5 group name for the scan (e.g. ``"S1"``).  This parameter is
            **required**; there is no default because the group name is part of
            the granule contract.
        pixel_width : int
            Number of across-track pixels per scanline.  **Required** — without
            this the 1D flat arrays cannot be reshaped into the original 2D
            structure.
        compression : str, optional
            HDF5 compression filter (default ``'gzip'``).
        compression_opts : int, optional
            Compression level (default ``4``).

        Returns
        -------
        str
            ``file_path`` (for chaining / convenience).

        Raises
        ------
        ImportError
            If ``h5py`` is not installed.
        ValueError
            If ``pixel_width`` is ``None`` or ``scan`` is ``None``.
        """
        import h5py

        if pixel_width is None:
            raise ValueError(
                "pixel_width is required to reconstitute the 2D HDF5 structure. "
                "Pass the number of across-track pixels per scanline."
            )
        if scan is None:
            raise ValueError(
                "scan group name is required (e.g. 'S1'). "
                "There is no safe default because the name is part of the granule format."
            )

        n_rows = len(self)
        N_scans, remainder = divmod(n_rows, pixel_width)
        if remainder != 0:
            warnings.warn(
                f"len(df)={n_rows} is not divisible by pixel_width={pixel_width}. "
                f"Trailing {remainder} row(s) will be truncated.",
                UserWarning,
                stacklevel=2,
            )

        n_used = N_scans * pixel_width

        # Collect Tc column names in numeric order
        tc_cols = sorted(
            [c for c in self.columns if re.match(r'^Tc\d+$', c)],
            key=lambda s: int(s[2:]),
        )

        # Ensure output directory exists
        out_dir = os.path.dirname(os.path.abspath(file_path))
        os.makedirs(out_dir, exist_ok=True)

        with h5py.File(file_path, mode) as f:
            sg = f.require_group(scan)

            # Latitude / Longitude — float32, 2D (matches original granule dtype)
            sg.create_dataset(
                'Latitude',
                data=self['lat'].values[:n_used].reshape(N_scans, pixel_width).astype(np.float32),
                compression=compression,
                compression_opts=compression_opts,
            )
            sg.create_dataset(
                'Longitude',
                data=self['lon'].values[:n_used].reshape(N_scans, pixel_width).astype(np.float32),
                compression=compression,
                compression_opts=compression_opts,
            )

            # Tc — float32, 3D: (N_scans, pixel_width, N_channels)
            if tc_cols:
                tc_stack = np.stack(
                    [self[c].values[:n_used] for c in tc_cols], axis=-1
                ).reshape(N_scans, pixel_width, len(tc_cols)).astype(np.float32)
                sg.create_dataset(
                    'Tc',
                    data=tc_stack,
                    compression=compression,
                    compression_opts=compression_opts,
                )

            # ScanTime — 1D per scan, derived from timestamp column
            if 'timestamp' in self.columns:
                ts_all = pandas.to_datetime(self['timestamp'].values[:n_used])
                # Take first pixel of each scanline for the scan timestamp
                ts_per_scan = ts_all.values.reshape(N_scans, pixel_width)[:, 0]
                ts_per_scan = pandas.DatetimeIndex(ts_per_scan)

                # NaT timestamps produce NaN for integer fields — fill with 0.
                def _ts_int8(arr):
                    return pandas.array(arr, dtype='Int32').fillna(0).to_numpy(dtype=np.int8)
                def _ts_int16(arr):
                    return pandas.array(arr, dtype='Int32').fillna(0).to_numpy(dtype=np.int16)

                st = sg.require_group('ScanTime')
                st.create_dataset('Year',        data=_ts_int16(ts_per_scan.year))
                st.create_dataset('Month',       data=_ts_int8(ts_per_scan.month))
                st.create_dataset('DayOfMonth',  data=_ts_int8(ts_per_scan.day))
                st.create_dataset('Hour',        data=_ts_int8(ts_per_scan.hour))
                st.create_dataset('Minute',      data=_ts_int8(ts_per_scan.minute))
                st.create_dataset('Second',      data=_ts_int8(ts_per_scan.second))
                st.create_dataset('MilliSecond',
                                  data=_ts_int16(ts_per_scan.microsecond // 1000))
                # Derived fields present in the original GMI format
                day_of_year = pandas.array(ts_per_scan.day_of_year, dtype='Int32').fillna(0).to_numpy(dtype=np.int16)
                st.create_dataset('DayOfYear', data=day_of_year)
                # SecondOfDay is float64 in the original granule, including sub-second precision
                seconds_of_day = (
                    ts_per_scan.hour.astype(np.float64) * 3600.0
                    + ts_per_scan.minute.astype(np.float64) * 60.0
                    + ts_per_scan.second.astype(np.float64)
                    + ts_per_scan.microsecond.astype(np.float64) / 1e6
                )
                st.create_dataset('SecondOfDay',
                                  data=np.where(np.isnan(seconds_of_day.astype(float)), 0.0, seconds_of_day).astype(np.float64))

            # ── Extra fields stored by the instrument reader ──────────
            # Columns already written above (handled explicitly)
            _written = (
                {'lat', 'lon', 'timestamp', 'sids', self._sid_column_name}
                | set(tc_cols)
            )

            # Columns matching SCstatus_* go into a SCstatus subgroup as 1D (N_scans,)
            # Known dtypes match the original GMI granule.
            _scstatus_dtypes = {
                'FractionalGranuleNumber': np.float64,
                'SCaltitude':  np.float32,
                'SClatitude':  np.float32,
                'SClongitude': np.float32,
                'SCorientation': np.int16,
            }
            scstatus_cols = [c for c in self.columns if c.startswith('SCstatus_') and c not in _written]
            if scstatus_cols:
                sc_grp = sg.require_group('SCstatus')
                for col in scstatus_cols:
                    field_name = col[len('SCstatus_'):]   # strip prefix
                    # Each value is repeated pixel_width times per scan — take first
                    values = self[col].values[:n_used].reshape(N_scans, pixel_width)[:, 0]
                    dtype = _scstatus_dtypes.get(field_name)
                    if dtype is not None:
                        values = np.nan_to_num(values, nan=0).astype(dtype)
                    sc_grp.create_dataset(field_name, data=values,
                                          compression=compression, compression_opts=compression_opts)
                _written.update(scstatus_cols)

            # incidenceAngleIndex{1..N} columns → (N_scans, N_ch) 2D array, int8
            idx_cols = sorted(
                [c for c in self.columns if re.match(r'^incidenceAngleIndex\d+$', c) and c not in _written],
                key=lambda s: int(re.search(r'\d+$', s).group()),
            )
            if idx_cols:
                # Each column is the same value repeated pixel_width times — take first pixel
                raw = np.stack(
                    [self[c].values[:n_used].reshape(N_scans, pixel_width)[:, 0] for c in idx_cols],
                    axis=-1,
                )
                idx_stack = np.nan_to_num(raw, nan=0).astype(np.int8)   # (N_scans, N_ch)
                sg.create_dataset('incidenceAngleIndex', data=idx_stack,
                                  compression=compression, compression_opts=compression_opts)
                _written.update(idx_cols)

            # Per-pixel fields that need a trailing size-1 channel dimension restored:
            # incidenceAngle → float32 (N_scans, pixel_width, 1)
            # sunGlintAngle  → int8    (N_scans, pixel_width, 1)
            _3d_field_dtypes = {'incidenceAngle': np.float32, 'sunGlintAngle': np.int8}
            for col, dtype in _3d_field_dtypes.items():
                if col in self.columns and col not in _written:
                    raw = self[col].values[:n_used].reshape(N_scans, pixel_width, 1)
                    arr = np.nan_to_num(raw, nan=0).astype(dtype)
                    sg.create_dataset(col, data=arr,
                                      compression=compression, compression_opts=compression_opts)
                    _written.add(col)

            # Remaining per-pixel 2D columns (e.g. Quality, sunLocalTime)
            # Known dtypes for fields stored as float64 whose originals differ.
            _pixel_field_dtypes = {
                'Quality':      np.int8,
                'sunLocalTime': np.float32,
            }
            for col in self.columns:
                if col in _written:
                    continue
                try:
                    arr = self[col].values[:n_used].reshape(N_scans, pixel_width)
                    dtype = _pixel_field_dtypes.get(col)
                    if dtype is not None:
                        arr = np.nan_to_num(arr, nan=0).astype(dtype)
                    sg.create_dataset(col, data=arr,
                                      compression=compression, compression_opts=compression_opts)
                except (ValueError, TypeError):
                    pass  # skip columns that can't be reshaped (e.g. object dtype)

            # Group-level provenance attributes
            sg.attrs['StarePodsReconstitution'] = True
            sg.attrs['PixelWidth'] = int(pixel_width)
            sg.attrs['ReconstitutionDate'] = datetime.datetime.now(datetime.timezone.utc).isoformat()

        return file_path

    def generate_partition_path(self, sid, dataset_name=""):
        """
        Generate the pod code for a STARE SID (quaternary storage layout).

        Returns the compact, dynamic-length pod code (``"q13211"``) for ``sid``
        — see :func:`starepandas.staredataframe.sid_to_podcode` and
        ``docs/quaternary_storage_plan.md`` §2.  This is a thin instance-method
        wrapper kept for backward compatibility; new code should call
        ``sid_to_podcode`` directly.

        The pod code is a single path segment (no ``/``).  The S3 layout is
        flat (the pod code is the key prefix) and the local layout is
        hierarchical (the cumulative pod-code dir chain — see
        :func:`podcode_to_local_dirs`).  The old ``Q00_X/Q01_Y/…`` directory
        spelling is no longer emitted.

        Parameters
        ----------
        sid : int
            8-byte STARE SID integer.
        dataset_name : str, optional
            Ignored.  Datasets are now carried in the chunk *filename*
            (``<podcode>-<granule>-<dataset>.parquet``), not the path; the
            parameter is retained only so existing call sites keep working.

        Returns
        -------
        str
            The pod code (e.g. ``"q13211"``).

        Examples
        --------
        >>> sdf = STAREDataFrame()
        >>> sdf.generate_partition_path(podcode_to_sid("q13211"))
        'q13211'
        """
        return sid_to_podcode(sid)

    def parse_partition_path(self, partition_path):
        """
        Reconstruct a STARE SID from a pod code (quaternary storage layout).

        Reverse of :meth:`generate_partition_path`.  Accepts a pod code such as
        ``"q13211"`` and returns ``(sid, None)``.  The trailing ``None`` keeps
        the old ``(sid, dataset_name)`` tuple shape; the dataset is no longer
        embedded in the path (it lives in the chunk filename), so it is always
        ``None`` here.

        Parameters
        ----------
        partition_path : str
            A pod code (e.g. ``"q13211"``).  Any ``/``-joined prefix is
            tolerated — only the final segment (the pod code) is parsed.

        Returns
        -------
        tuple
            ``(sid, None)`` where ``sid`` is the reconstructed STARE SID.

        Examples
        --------
        >>> sdf = STAREDataFrame()
        >>> sid, _ = sdf.parse_partition_path("q13211")
        >>> sdf.generate_partition_path(sid)
        'q13211'
        """
        if not partition_path:
            raise ValueError("partition_path cannot be empty")
        # Tolerate a slash-joined prefix; the pod code is the final segment.
        podcode = partition_path.rstrip('/').split('/')[-1]
        return podcode_to_sid(podcode), None

    @classmethod
    def from_s3(cls, s3_path, storage_options=None):
        """
        Read STAREDataFrame from S3 Parquet partitions written by
        :meth:`to_s3`.

        Walks ``s3_path`` recursively, reads every ``*.parquet`` object, and
        concatenates the rows back in original order using
        ``__row_positions__``.

        Parameters
        ----------
        s3_path : str
            S3 path to the storage root (e.g. ``"s3://bucket/granule_name"``).
        storage_options : dict, optional
            S3 storage options including credentials and region.

        Returns
        -------
        STAREDataFrame
            The reconstructed STAREDataFrame in original row order.
        """
        import pyarrow.parquet as pq

        merged_opts = dict(_AWS_S3_STORAGE_OPTIONS)
        if not merged_opts:
            _load_config_from_default_locations()
            merged_opts = dict(_AWS_S3_STORAGE_OPTIONS)
        if storage_options:
            merged_opts.update(storage_options)
        if not merged_opts:
            raise ValueError(
                "Missing S3 configuration. Call load_aws_configure(config_path) or aws_configure(...) "
                "to set credentials/region, or pass storage_options to from_s3."
            )
        fs = s3fs.S3FileSystem(**merged_opts)

        # Recursively discover Parquet files under s3_path
        prefix = s3_path[len('s3://'):] if s3_path.startswith('s3://') else s3_path
        try:
            parquet_keys = [k for k in fs.find(prefix) if k.endswith('.parquet')]
        except Exception:
            parquet_keys = []

        if not parquet_keys:
            return cls()

        frames = []
        for key in parquet_keys:
            df_part = pq.read_table(key, filesystem=fs).to_pandas()
            if df_part.empty:
                continue
            frames.append(df_part)

        if not frames:
            return cls()

        df = pandas.concat(frames, ignore_index=True)
        if '__row_positions__' in df.columns:
            df = df.sort_values('__row_positions__').drop(
                columns=['__row_positions__']
            ).reset_index(drop=True)

        return cls(df)
    
    @classmethod
    def from_local(cls, local_path):
        """
        Read STAREDataFrame from local Parquet partitions written by
        :meth:`to_local`.

        Walks ``local_path`` recursively, reads every ``*.parquet`` file, and
        concatenates the rows back in original order using
        ``__row_positions__``.

        Parameters
        ----------
        local_path : str
            Local path to the storage root directory.

        Returns
        -------
        STAREDataFrame
            The reconstructed STAREDataFrame in original row order.
        """
        import pyarrow.parquet as pq

        if not os.path.isdir(local_path):
            return cls()

        parquet_files = []
        for root, _dirs, files in os.walk(local_path):
            for f in files:
                if f.endswith('.parquet'):
                    parquet_files.append(os.path.join(root, f))

        if not parquet_files:
            return cls()

        frames = []
        for fpath in parquet_files:
            df_part = pq.read_table(fpath).to_pandas()
            if df_part.empty:
                continue
            frames.append(df_part)

        if not frames:
            return cls()

        df = pandas.concat(frames, ignore_index=True)
        if '__row_positions__' in df.columns:
            df = df.sort_values('__row_positions__').drop(
                columns=['__row_positions__']
            ).reset_index(drop=True)

        return cls(df)

    def to_pickle_s3(self, s3_path, storage_options=None, compress=None):
        """
        Write STAREDataFrame to S3 as pickle file.
        
        Parameters
        ----------
        s3_path : str
            S3 path where the pickle file will be written (e.g., "s3://bucket/granule_name.pkl")
        storage_options : dict, optional
            S3 storage options including credentials and region
        compress : str, optional
            Compression method ('bz2' or None)
            
        Returns
        -------
        str
            The S3 path where data was written
        """
        import s3fs
        
        # Create S3 filesystem
        fs = s3fs.S3FileSystem(**storage_options or {})
        
        # Write pickle to S3
        with fs.open(s3_path, 'wb') as f:
            if compress == 'bz2':
                import bz2
                with bz2.open(f, 'wb') as bz2f:
                    pickle.dump(self, bz2f)
            else:
                pickle.dump(self, f)
        
        return s3_path
    
    def to_pickle_local(self, local_path, compress=None):
        """
        Write STAREDataFrame to local storage as pickle file.
        
        Parameters
        ----------
        local_path : str
            Local path where the pickle file will be written
        compress : str, optional
            Compression method ('bz2' or None)
            
        Returns
        -------
        str
            The local path where data was written
        """
        # Write pickle to local filesystem
        if compress == 'bz2':
            import bz2
            with bz2.open(local_path, 'wb') as f:
                pickle.dump(self, f)
        else:
            with open(local_path, 'wb') as f:
                pickle.dump(self, f)
        
        return local_path
    
    @classmethod
    def from_pickle_s3(cls, s3_path, storage_options=None, compress=None):
        """
        Read STAREDataFrame from S3 pickle file.
        
        Parameters
        ----------
        s3_path : str
            S3 path to the pickle file
        storage_options : dict, optional
            S3 storage options including credentials and region
        compress : str, optional
            Compression method ('bz2' or None)
            
        Returns
        -------
        STAREDataFrame
            The reconstructed STAREDataFrame
        """
        import s3fs
        
        # Create S3 filesystem
        fs = s3fs.S3FileSystem(**storage_options or {})
        
        # Read pickle from S3
        with fs.open(s3_path, 'rb') as f:
            if compress == 'bz2':
                import bz2
                with bz2.open(f, 'rb') as bz2f:
                    df = pickle.load(bz2f)
            else:
                df = pickle.load(f)
        
        return df
    
    @classmethod
    def from_pickle_local(cls, local_path, compress=None):
        """
        Read STAREDataFrame from local pickle file.
        
        Parameters
        ----------
        local_path : str
            Local path to the pickle file
        compress : str, optional
            Compression method ('bz2' or None)
            
        Returns
        -------
        STAREDataFrame
            The reconstructed STAREDataFrame
        """
        # Read pickle from local filesystem
        if compress == 'bz2':
            import bz2
            with bz2.open(local_path, 'rb') as f:
                df = pickle.load(f)
        else:
            with open(local_path, 'rb') as f:
                df = pickle.load(f)
        
        return df

    def to_sidecar(self, file_name, cover=False, shuffle=True, zlib=True):
        """ Writes STARE Sidecar

        """
        sids = self.to_array(self._sid_column_name)
        # lat = self.to_array(self['lat'])
        # lon = self.to_array(self['lon'])

        i = sids.shape[0]
        j = sids.shape[1]
        with netCDF4.Dataset(file_name, 'w', format="NETCDF4") as root_group:
            root_group.createDimension('i', i)
            root_group.createDimension('j', j)

            sids_netcdf = root_group.createVariable(varname='STARE_index',
                                                    datatype='u8',
                                                    dimensions=('i', 'j'),
                                                    chunksizes=[i, j],
                                                    shuffle=shuffle,
                                                    zlib=zlib)
            sids_netcdf.long_name = 'SpatioTemporal Adaptive Resolution Encoding (STARE) index'
            sids_netcdf[:, :] = sids
            if cover:
                sids_cover = self.stare_dissolve()
                l: int = sids_cover.size
                root_group.createDimension('l', l)
                cover_netcdf = root_group.createVariable(varname='STARE_cover',
                                                         datatype='u8',
                                                         dimensions='l',
                                                         chunksizes=[l],
                                                         shuffle=shuffle,
                                                         zlib=zlib)
                cover_netcdf.long_name = 'SpatioTemporal Adaptive Resolution Encoding (STARE) cover'
                cover_netcdf[:] = sids_cover

        def to_postgis(self, name, con, schema=None, if_exists="fail", index=False, index_label=None, chunksize=None, dtype=None):
            """
            This overwrites the geopandas.GeoDataFrame.to_postgis() method.
            Parameters
            ----------
            name
            con
            schema
            if_exists
            index
            index_label
            chunksize
            dtype

            Returns
            -------
            None

            """
            starepandas.io.postgis.write(gdf=self, engine=con, table_name=name)

def _dataframe_set_sids(self, col, inplace=False):
    # We create a function here so that we can take conventional DataFrames and convert them to sdfs
    if inplace:
        raise ValueError("Can't do inplace setting when converting from (Geo)DataFrame to STAREDataFrame")
    sdf = STAREDataFrame(self)
    # this will copy so that BlockManager gets copied
    return sdf.set_sids(col, inplace=False)

geopandas.GeoDataFrame.set_sids = _dataframe_set_sids
pandas.DataFrame.set_sids = _dataframe_set_sids
