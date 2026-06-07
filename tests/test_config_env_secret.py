"""Tests for the STAREPANDAS_WORKER_SECRET env-var config loader (C-4 Part A).

The cloud worker runs in ECS, where Secrets Manager injects the config as the
env var ``STAREPANDAS_WORKER_SECRET`` (a JSON string) rather than as a file on
disk. ``_load_config_from_default_locations`` must parse that env var directly,
populating the same module-level S3/RDS/default-prefix state the file path does.

Covers:

* The env var (JSON) populates RDS options, region, and default_s3_prefix.
* ``default_s3_prefix`` is routed to the module constant, NOT leaked into the
  s3fs storage options (which would later break ``s3fs.S3FileSystem(**opts)``).
* A minimal secret (RDS creds + region + prefix, no S3 key/secret) is accepted
  — this is the C-4 "minimal secret" decision: S3/SQS/DDB use the task role.
* The env var takes precedence over a file pointed to by STAREPANDAS_AWS_CONFIG.
* A malformed env var falls through to the file-based fallback instead of
  raising.
"""

import json
import os

import pytest


def _reset(monkeypatch, sdf_module):
    """Clear all module-level config state so a load is observable."""
    monkeypatch.setattr(sdf_module, "_AWS_S3_STORAGE_OPTIONS", {})
    monkeypatch.setattr(sdf_module, "_AWS_RDS_OPTIONS", {})
    monkeypatch.setattr(sdf_module, "_DEFAULT_S3_PREFIX", "")
    # Don't let a real ~/.config or repo .config interfere.
    monkeypatch.delenv("STAREPANDAS_AWS_CONFIG", raising=False)


def test_env_secret_populates_rds_region_and_prefix(monkeypatch):
    import starepandas.staredataframe as sdf_module
    _reset(monkeypatch, sdf_module)

    secret = {
        "region_name": "us-west-2",
        "default_s3_prefix": "s3://zarrpods/storage",
        "rds": {
            "host": "zarrpods.example.us-west-2.rds.amazonaws.com",
            "port": 5432,
            "username": "worker",
            "password": "s3cr3t",
            "database": "postgres",
        },
    }
    monkeypatch.setenv("STAREPANDAS_WORKER_SECRET", json.dumps(secret))

    assert sdf_module._load_config_from_default_locations() is True
    assert sdf_module._DEFAULT_S3_PREFIX == "s3://zarrpods/storage"
    assert sdf_module._AWS_RDS_OPTIONS["host"].endswith("rds.amazonaws.com")
    assert sdf_module._AWS_RDS_OPTIONS["username"] == "worker"
    assert sdf_module._AWS_RDS_OPTIONS["database"] == "postgres"
    # region routed into client_kwargs of the storage options.
    assert sdf_module._AWS_S3_STORAGE_OPTIONS["client_kwargs"]["region_name"] == "us-west-2"


def test_default_s3_prefix_not_leaked_into_storage_options(monkeypatch):
    """default_s3_prefix is consumed by the loader; it must NOT appear in the
    s3fs storage options or s3fs.S3FileSystem(**opts) would reject it."""
    import starepandas.staredataframe as sdf_module
    _reset(monkeypatch, sdf_module)

    secret = {
        "region_name": "us-west-2",
        "default_s3_prefix": "s3://zarrpods/storage/",  # trailing slash too
        "rds": {"host": "h", "port": 5432, "username": "u",
                "password": "p", "database": "postgres"},
    }
    monkeypatch.setenv("STAREPANDAS_WORKER_SECRET", json.dumps(secret))
    sdf_module._load_config_from_default_locations()

    assert "default_s3_prefix" not in sdf_module._AWS_S3_STORAGE_OPTIONS
    # trailing slash normalised
    assert sdf_module._DEFAULT_S3_PREFIX == "s3://zarrpods/storage"


def test_minimal_secret_without_s3_keys_is_accepted(monkeypatch):
    """The C-4 minimal-secret decision: no key/secret in the config. The load
    still succeeds and leaves no AWS key/secret behind (task role covers S3)."""
    import starepandas.staredataframe as sdf_module
    _reset(monkeypatch, sdf_module)

    secret = {
        "region_name": "us-west-2",
        "default_s3_prefix": "s3://zarrpods/storage",
        "rds": {"host": "h", "port": 5432, "username": "u",
                "password": "p", "database": "postgres"},
    }
    monkeypatch.setenv("STAREPANDAS_WORKER_SECRET", json.dumps(secret))
    assert sdf_module._load_config_from_default_locations() is True

    opts = sdf_module._AWS_S3_STORAGE_OPTIONS
    assert "key" not in opts and "secret" not in opts
    # but region is present, so the to_s3 "did you configure anything" guard passes.
    assert opts.get("client_kwargs", {}).get("region_name") == "us-west-2"


def test_env_secret_takes_precedence_over_file(monkeypatch, tmp_path):
    import starepandas.staredataframe as sdf_module
    _reset(monkeypatch, sdf_module)

    # A file config that would set a DIFFERENT prefix.
    cfg = tmp_path / ".config"
    cfg.write_text(
        "region_name=us-west-2\nrds_host=filehost\nport=5432\n"
        "username=fu\npassword=fp\ndatabase=postgres\n"
        "default_s3_prefix=s3://file/prefix\n"
    )
    monkeypatch.setenv("STAREPANDAS_AWS_CONFIG", str(cfg))

    secret = {
        "region_name": "us-west-2",
        "default_s3_prefix": "s3://env/prefix",
        "rds": {"host": "envhost", "port": 5432, "username": "eu",
                "password": "ep", "database": "postgres"},
    }
    monkeypatch.setenv("STAREPANDAS_WORKER_SECRET", json.dumps(secret))

    sdf_module._load_config_from_default_locations()
    # Env wins.
    assert sdf_module._DEFAULT_S3_PREFIX == "s3://env/prefix"
    assert sdf_module._AWS_RDS_OPTIONS["host"] == "envhost"


def test_malformed_env_secret_falls_through_to_file(monkeypatch, tmp_path):
    import starepandas.staredataframe as sdf_module
    _reset(monkeypatch, sdf_module)

    cfg = tmp_path / ".config"
    cfg.write_text(
        "region_name=us-west-2\nrds_host=filehost\nport=5432\n"
        "username=fu\npassword=fp\ndatabase=postgres\n"
        "default_s3_prefix=s3://file/prefix\n"
    )
    monkeypatch.setenv("STAREPANDAS_AWS_CONFIG", str(cfg))
    monkeypatch.setenv("STAREPANDAS_WORKER_SECRET", "{not valid json")

    assert sdf_module._load_config_from_default_locations() is True
    # Fell through to the file.
    assert sdf_module._DEFAULT_S3_PREFIX == "s3://file/prefix"
    assert sdf_module._AWS_RDS_OPTIONS["host"] == "filehost"
