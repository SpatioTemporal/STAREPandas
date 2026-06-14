"""Offline tests for the C-6 client SDK (starepandas.cloud).

No network or AWS access: the low-level HTTP helper
(``starepandas.cloud._http.request``) and boto3 are mocked. Covers request body
assembly, the ``x-api-key`` header, ``JobHandle`` polling, error surfacing, the
large-list S3 escape hatch, ``cancel()``'s 501→NotImplementedError mapping, and
the config reserved-key plumbing (endpoint/api_key must not leak into s3fs
storage options).
"""

import json

import pytest

import starepandas.cloud.client as client_mod
import starepandas.cloud.job_handle as jh_mod
from starepandas.cloud import (
    IngestError,
    JobHandle,
    JobNotFound,
    get_cloud_config,
    ingest_granules,
)

ENDPOINT = "https://api.example.com/v1"
API_KEY = "test-key"


class _Recorder:
    """Stand-in for ``_http.request`` that records calls and replays responses."""

    def __init__(self, responses):
        # responses: a single (status, payload) tuple or a list to pop from.
        self._responses = responses
        self.calls = []

    def __call__(self, method, url, api_key, body=None, timeout=30):
        self.calls.append({"method": method, "url": url, "api_key": api_key, "body": body})
        if isinstance(self._responses, list):
            return self._responses.pop(0)
        return self._responses


def _patch_config(monkeypatch):
    monkeypatch.setattr(
        client_mod, "get_cloud_config",
        lambda endpoint=None, api_key=None: (ENDPOINT, API_KEY),
    )


# --------------------------------------------------------------------------- #
# ingest_granules
# --------------------------------------------------------------------------- #

def test_ingest_builds_body_and_defaults_workers(monkeypatch):
    _patch_config(monkeypatch)
    rec = _Recorder((202, {"job_id": "abc123", "state": "running",
                           "total_granules": 1, "ticket_count": 1, "workers": 4}))
    monkeypatch.setattr(client_mod, "request", rec)

    handle = ingest_granules(granule_uris=["s3://b/g.HDF5"], instrument="GMI")

    assert isinstance(handle, JobHandle)
    assert handle.job_id == "abc123"
    assert handle.record["state"] == "running"
    call = rec.calls[0]
    assert call["method"] == "POST"
    assert call["url"] == ENDPOINT + "/ingest"
    assert call["api_key"] == API_KEY
    assert call["body"]["instrument"] == "GMI"
    assert call["body"]["workers"] == 4          # default
    assert call["body"]["granule_uris"] == ["s3://b/g.HDF5"]
    assert "granule_uris_s3" not in call["body"]


def test_ingest_passes_optional_fields(monkeypatch):
    _patch_config(monkeypatch)
    rec = _Recorder((202, {"job_id": "j", "state": "running"}))
    monkeypatch.setattr(client_mod, "request", rec)

    ingest_granules(
        granule_uris=["s3://b/g.HDF5"], instrument="GMI",
        s3_prefix="s3://b/out", callback_url="https://hook", options={"k": 1},
    )
    body = rec.calls[0]["body"]
    assert body["s3_prefix"] == "s3://b/out"
    assert body["callback_url"] == "https://hook"
    assert body["options"] == {"k": 1}


def test_ingest_empty_or_missing_inputs_raise(monkeypatch):
    _patch_config(monkeypatch)
    monkeypatch.setattr(client_mod, "request", _Recorder((202, {"job_id": "j"})))
    with pytest.raises(ValueError):
        ingest_granules(granule_uris=[], instrument="GMI")
    with pytest.raises(ValueError):
        ingest_granules(granule_uris=["s3://b/g"], instrument="")


def test_ingest_workers_over_cap_surfaces_400(monkeypatch):
    _patch_config(monkeypatch)
    rec = _Recorder((400, {"error": "workers exceeds hard cap of 4"}))
    monkeypatch.setattr(client_mod, "request", rec)

    with pytest.raises(IngestError) as exc:
        ingest_granules(granule_uris=["s3://b/g"], instrument="GMI", workers=8)
    assert exc.value.status_code == 400
    assert "cap" in str(exc.value)
    assert rec.calls[0]["body"]["workers"] == 8


def test_ingest_large_list_uploads_to_s3(monkeypatch):
    _patch_config(monkeypatch)
    # Force the escape hatch with a tiny threshold instead of a 4 MB list.
    monkeypatch.setattr(client_mod, "_INLINE_LIST_LIMIT_BYTES", 5)
    rec = _Recorder((202, {"job_id": "j", "state": "running"}))
    monkeypatch.setattr(client_mod, "request", rec)

    uploaded = {}

    class _FakeS3:
        def put_object(self, **kwargs):
            uploaded.update(kwargs)

    import boto3
    monkeypatch.setattr(boto3, "client", lambda svc: _FakeS3())

    ingest_granules(
        granule_uris=["s3://b/g1.HDF5", "s3://b/g2.HDF5"], instrument="GMI",
    )
    body = rec.calls[0]["body"]
    assert "granule_uris" not in body
    assert body["granule_uris_s3"].startswith("s3://zarrpods/_jobs/")
    assert body["granule_uris_s3"].endswith(".json")
    # The staged object holds the original list.
    assert json.loads(uploaded["Body"].decode("utf-8")) == [
        "s3://b/g1.HDF5", "s3://b/g2.HDF5"]
    assert uploaded["Bucket"] == "zarrpods"


# --------------------------------------------------------------------------- #
# JobHandle
# --------------------------------------------------------------------------- #

def _handle(monkeypatch, responses):
    rec = _Recorder(responses)
    monkeypatch.setattr(jh_mod, "request", rec)
    return JobHandle("j", ENDPOINT, API_KEY), rec


def test_status_returns_record_and_caches(monkeypatch):
    handle, rec = _handle(monkeypatch, (200, {"job_id": "j", "state": "running"}))
    rec_dict = handle.status()
    assert rec_dict["state"] == "running"
    assert handle.record["state"] == "running"
    assert rec.calls[0]["url"] == ENDPOINT + "/jobs/j"


def test_status_404_raises_jobnotfound(monkeypatch):
    handle, _ = _handle(monkeypatch, (404, {"error": "no such job"}))
    with pytest.raises(JobNotFound):
        handle.status()


def test_wait_polls_until_terminal(monkeypatch):
    monkeypatch.setattr(jh_mod.time, "sleep", lambda s: None)
    handle, rec = _handle(monkeypatch, [
        (200, {"state": "running"}),
        (200, {"state": "running"}),
        (200, {"state": "complete", "processed": 1}),
    ])
    record = handle.wait(poll_interval=0.01)
    assert record["state"] == "complete"
    assert len(rec.calls) == 3


def test_wait_returns_on_failed(monkeypatch):
    monkeypatch.setattr(jh_mod.time, "sleep", lambda s: None)
    handle, _ = _handle(monkeypatch, [(200, {"state": "failed", "failed": 2})])
    assert handle.wait()["state"] == "failed"


def test_wait_timeout_raises(monkeypatch):
    monkeypatch.setattr(jh_mod.time, "sleep", lambda s: None)
    handle, _ = _handle(monkeypatch, (200, {"state": "running"}))
    with pytest.raises(TimeoutError):
        handle.wait(timeout=0, poll_interval=10)


def test_failures_pagination_url(monkeypatch):
    handle, rec = _handle(monkeypatch, (200, {"job_id": "j", "count": 0, "failures": []}))
    handle.failures(next_token="tok 1")
    assert rec.calls[0]["url"].startswith(ENDPOINT + "/jobs/j/failures?next=")
    assert "tok%201" in rec.calls[0]["url"]


def test_cancel_calls_delete_then_raises(monkeypatch):
    handle, rec = _handle(monkeypatch, (501, {}))
    with pytest.raises(NotImplementedError):
        handle.cancel()
    assert rec.calls[0]["method"] == "DELETE"
    assert rec.calls[0]["url"] == ENDPOINT + "/jobs/j"


# --------------------------------------------------------------------------- #
# _http.request — header + JSON encoding (the only place urllib is exercised)
# --------------------------------------------------------------------------- #

def test_http_request_sets_api_key_header_and_encodes_json(monkeypatch):
    import starepandas.cloud._http as http_mod
    captured = {}

    class _FakeResp:
        status = 202

        def read(self):
            return b'{"job_id": "j"}'

        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    def _fake_urlopen(req, timeout=None):
        captured["headers"] = req.headers
        captured["data"] = req.data
        captured["method"] = req.get_method()
        return _FakeResp()

    monkeypatch.setattr(http_mod.urllib.request, "urlopen", _fake_urlopen)

    status, payload = http_mod.request(
        "POST", "https://api/x", "secret", body={"a": 1})
    assert status == 202
    assert payload == {"job_id": "j"}
    # urllib title-cases header keys.
    assert captured["headers"]["X-api-key"] == "secret"
    assert json.loads(captured["data"]) == {"a": 1}
    assert captured["method"] == "POST"


def test_http_request_returns_error_status_with_body(monkeypatch):
    import starepandas.cloud._http as http_mod
    import urllib.error
    import io

    def _fake_urlopen(req, timeout=None):
        raise urllib.error.HTTPError(
            "https://api/x", 400, "Bad Request", {},
            io.BytesIO(b'{"error": "bad"}'))

    monkeypatch.setattr(http_mod.urllib.request, "urlopen", _fake_urlopen)
    status, payload = http_mod.request("POST", "https://api/x", "k", body={})
    assert status == 400
    assert payload == {"error": "bad"}


# --------------------------------------------------------------------------- #
# config: endpoint/api_key plumbing
# --------------------------------------------------------------------------- #

def _reset_config(monkeypatch, sdf):
    monkeypatch.setattr(sdf, "_AWS_S3_STORAGE_OPTIONS", {})
    monkeypatch.setattr(sdf, "_AWS_RDS_OPTIONS", {})
    monkeypatch.setattr(sdf, "_CLOUD_ENDPOINT", "")
    monkeypatch.setattr(sdf, "_CLOUD_API_KEY", "")
    monkeypatch.delenv("STAREPANDAS_AWS_CONFIG", raising=False)
    monkeypatch.delenv("STAREPANDAS_CLOUD_ENDPOINT", raising=False)
    monkeypatch.delenv("STAREPANDAS_CLOUD_API_KEY", raising=False)


def test_endpoint_api_key_not_leaked_into_storage_options(monkeypatch):
    import starepandas.staredataframe as sdf
    _reset_config(monkeypatch, sdf)

    secret = {
        "region_name": "us-west-2",
        "endpoint": "https://api.example.com/v1/",
        "api_key": "k123",
        "rds": {"host": "h", "port": 5432, "username": "u",
                "password": "p", "database": "postgres"},
    }
    monkeypatch.setenv("STAREPANDAS_WORKER_SECRET", json.dumps(secret))
    assert sdf._load_config_from_default_locations() is True

    assert sdf._CLOUD_ENDPOINT == "https://api.example.com/v1"   # slash stripped
    assert sdf._CLOUD_API_KEY == "k123"
    assert "endpoint" not in sdf._AWS_S3_STORAGE_OPTIONS
    assert "api_key" not in sdf._AWS_S3_STORAGE_OPTIONS


def test_get_cloud_config_env_override(monkeypatch):
    import starepandas.staredataframe as sdf
    _reset_config(monkeypatch, sdf)
    monkeypatch.delenv("STAREPANDAS_WORKER_SECRET", raising=False)
    monkeypatch.setenv("STAREPANDAS_CLOUD_ENDPOINT", "https://env.api/v1/")
    monkeypatch.setenv("STAREPANDAS_CLOUD_API_KEY", "envkey")

    endpoint, api_key = get_cloud_config()
    assert endpoint == "https://env.api/v1"
    assert api_key == "envkey"


def test_get_cloud_config_missing_raises(monkeypatch):
    import starepandas.staredataframe as sdf
    _reset_config(monkeypatch, sdf)
    monkeypatch.delenv("STAREPANDAS_WORKER_SECRET", raising=False)
    # No config anywhere; loader returns nothing useful.
    monkeypatch.setattr(sdf, "_load_config_from_default_locations", lambda: False)
    with pytest.raises(RuntimeError):
        get_cloud_config()
