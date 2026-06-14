"""Client SDK entrypoint for the STARE-PODS cloud ingest service (C-6).

Pure client-side wrapper over the deployed REST API. Submits an ingest job and
returns a :class:`~starepandas.cloud.job_handle.JobHandle` the caller can poll:

    import starepandas as sp
    handle = sp.cloud.ingest_granules(granule_uris=[...], instrument="GMI")
    record = handle.wait()        # blocks until state in {complete, failed}

No new AWS resources are required — this speaks the already-live API. See
``docs/path_c_implementation.md`` §C-6 for the full contract.
"""

import json
import uuid

from starepandas.cloud._http import IngestError, request
from starepandas.cloud.config import get_cloud_config
from starepandas.cloud.job_handle import JobHandle

#: Hard cap the control plane enforces (>cap → 400). The SDK defaults to this.
DEFAULT_WORKERS = 4

#: Inline-body size above which the granule list is uploaded to S3 and sent as
#: ``granule_uris_s3`` instead. API Gateway / Lambda reject bodies over ~6 MB;
#: stay well under to leave room for the rest of the request envelope.
_INLINE_LIST_LIMIT_BYTES = 4 * 1024 * 1024

#: Bucket/prefix the scheduler is granted ``s3:GetObject`` on for large lists.
_JOBS_BUCKET = "zarrpods"
_JOBS_PREFIX = "_jobs"


def ingest_granules(
    granule_uris,
    instrument,
    s3_prefix=None,
    workers=DEFAULT_WORKERS,
    callback_url=None,
    options=None,
    block=False,
    *,
    endpoint=None,
    api_key=None,
    jobs_bucket=_JOBS_BUCKET,
    jobs_prefix=_JOBS_PREFIX,
):
    """Submit an ingest job to the cloud service.

    Parameters
    ----------
    granule_uris : list[str]
        ``s3://`` URIs of the granules to ingest. If the serialised list is
        larger than ~4 MB it is uploaded to ``s3://{jobs_bucket}/{jobs_prefix}/``
        and sent as a ``granule_uris_s3`` pointer instead (the Lambda body
        limit escape hatch, §C10).
    instrument : str
        Instrument name, e.g. ``"GMI"`` (required by the API).
    s3_prefix : str, optional
        Storage root override; the worker falls back to its configured default.
    workers : int
        Desired worker count. Default 4; the server hard-caps at 4 and returns
        ``400`` for anything larger — surfaced here as :class:`IngestError`.
    callback_url : str, optional
        URL the completion watcher POSTs the terminal payload to.
    options : dict, optional
        Passed through to the worker's ingest call.
    block : bool
        If True, call :meth:`JobHandle.wait` before returning.
    endpoint, api_key : str, optional
        Override the configured API endpoint / key (else read from ``.config``).
    jobs_bucket, jobs_prefix : str
        Where large granule lists are staged for the ``granule_uris_s3`` path.

    Returns
    -------
    JobHandle
        Handle to the accepted job (its ``record`` holds the ``202`` body).

    Raises
    ------
    IngestError
        If the server rejects the submission (validation, cost cap, ``workers``
        over the cap, internal error, or oversized inline body).
    ValueError
        If ``granule_uris`` is empty or ``instrument`` is missing.
    """
    if not instrument:
        raise ValueError("instrument is required")
    granule_uris = list(granule_uris or [])
    if not granule_uris:
        raise ValueError("granule_uris must be a non-empty list")

    endpoint, api_key = get_cloud_config(endpoint=endpoint, api_key=api_key)

    body = {"instrument": instrument, "workers": workers}
    if s3_prefix:
        body["s3_prefix"] = s3_prefix
    if callback_url:
        body["callback_url"] = callback_url
    if options:
        body["options"] = options

    # Large-list escape hatch: stage the list in S3 and send a pointer.
    serialized = json.dumps(granule_uris)
    if len(serialized.encode("utf-8")) > _INLINE_LIST_LIMIT_BYTES:
        body["granule_uris_s3"] = _upload_granule_list(
            serialized, jobs_bucket, jobs_prefix
        )
    else:
        body["granule_uris"] = granule_uris

    status_code, payload = request(
        "POST", "%s/ingest" % endpoint, api_key, body=body
    )
    if status_code != 202:
        raise IngestError(
            payload.get("error", "POST /ingest failed with HTTP %s" % status_code),
            status_code=status_code,
            payload=payload,
        )

    handle = JobHandle(
        payload["job_id"], endpoint, api_key, record=payload
    )
    if block:
        handle.wait()
    return handle


def _upload_granule_list(serialized, bucket, prefix):
    """Upload a serialised granule-URI list to S3, return the ``s3://`` pointer."""
    import boto3

    key = "%s/%s.json" % (prefix.strip("/"), uuid.uuid4())
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=serialized.encode("utf-8"),
        ContentType="application/json",
    )
    return "s3://%s/%s" % (bucket, key)
