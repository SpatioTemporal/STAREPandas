"""STARE-PODS cloud service (Path C).

This package holds the cloud-side counterparts of the local-host pipeline:
ticket sizing, the client SDK (filled in at C-6), the worker entrypoint
(filled in at C-2), and config helpers for the API endpoint.

See ``docs/path_c_implementation.md`` for the implementation plan and
``stare_pods_aws_parallel_plan.html`` §C for the design.
"""

from starepandas.cloud.ticket_sizing import split_into_tickets

__all__ = ["split_into_tickets"]
