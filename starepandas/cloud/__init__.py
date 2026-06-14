"""STARE-PODS cloud service (Path C).

This package holds the cloud-side counterparts of the local-host pipeline:
ticket sizing, the client SDK (C-6), the worker entrypoint (C-2), and config
helpers for the API endpoint.

The client SDK is the user-facing surface:

    import starepandas as sp
    handle = sp.cloud.ingest_granules(granule_uris=[...], instrument="GMI")
    record = handle.wait()        # blocks until state in {complete, failed}

See ``docs/path_c_implementation.md`` for the implementation plan and
``stare_pods_aws_parallel_plan.html`` §C for the design.
"""

from starepandas.cloud.ticket_sizing import split_into_tickets
from starepandas.cloud.client import ingest_granules, DEFAULT_WORKERS
from starepandas.cloud.job_handle import JobHandle, TERMINAL_STATES
from starepandas.cloud.config import get_cloud_config
from starepandas.cloud._http import CloudAPIError, IngestError, JobNotFound

__all__ = [
    "split_into_tickets",
    "ingest_granules",
    "DEFAULT_WORKERS",
    "JobHandle",
    "TERMINAL_STATES",
    "get_cloud_config",
    "CloudAPIError",
    "IngestError",
    "JobNotFound",
]
