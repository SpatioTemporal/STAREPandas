"""Cloud client config (Path C, C-6).

Reads the REST API ``endpoint`` and ``api_key`` from the same ``.config`` path
that ``staredataframe._load_config_from_default_locations`` consumes, so cloud
callers reuse the one existing config mechanism (file, ``STAREPANDAS_AWS_CONFIG``,
or the ``STAREPANDAS_WORKER_SECRET`` env-var branch). Both keys are routed to
module constants in ``staredataframe`` and skipped from the s3fs storage-options
(``_RESERVED_CONFIG_KEYS``), the same pattern used for ``default_s3_prefix``.

For CI / verification without a ``.config`` file, the values may be supplied via
``STAREPANDAS_CLOUD_ENDPOINT`` / ``STAREPANDAS_CLOUD_API_KEY`` env vars, which
take precedence over the config file.
"""

import os

import starepandas.staredataframe as _sdf

ENDPOINT_ENV_VAR = "STAREPANDAS_CLOUD_ENDPOINT"
API_KEY_ENV_VAR = "STAREPANDAS_CLOUD_API_KEY"


def get_cloud_config(endpoint=None, api_key=None):
    """Resolve the cloud API ``(endpoint, api_key)`` pair.

    Precedence (first non-empty wins for each field):

    1. Explicit ``endpoint`` / ``api_key`` arguments.
    2. ``STAREPANDAS_CLOUD_ENDPOINT`` / ``STAREPANDAS_CLOUD_API_KEY`` env vars.
    3. The ``endpoint`` / ``api_key`` keys from the loaded ``.config``.

    The ``.config`` is loaded on demand (via
    ``staredataframe._load_config_from_default_locations``) if neither module
    constant is populated yet.

    Returns
    -------
    tuple(str, str)
        ``(endpoint, api_key)`` — endpoint has any trailing slash stripped.

    Raises
    ------
    RuntimeError
        If either value cannot be resolved, with guidance on how to set it.
    """
    endpoint = endpoint or os.environ.get(ENDPOINT_ENV_VAR)
    api_key = api_key or os.environ.get(API_KEY_ENV_VAR)

    # Lazily load the config file if the module constants are still empty and we
    # don't already have both values from arguments / env.
    if (not endpoint or not api_key) and (
        not _sdf._CLOUD_ENDPOINT or not _sdf._CLOUD_API_KEY
    ):
        _sdf._load_config_from_default_locations()

    endpoint = endpoint or _sdf._CLOUD_ENDPOINT
    api_key = api_key or _sdf._CLOUD_API_KEY

    if not endpoint:
        raise RuntimeError(
            "Cloud API endpoint is not configured. Set 'endpoint' in your "
            ".config (or the %s env var) to the API base URL, e.g. "
            "https://<id>.execute-api.<region>.amazonaws.com/v1/" % ENDPOINT_ENV_VAR
        )
    if not api_key:
        raise RuntimeError(
            "Cloud API key is not configured. Set 'api_key' in your .config "
            "(or the %s env var)." % API_KEY_ENV_VAR
        )

    return endpoint.rstrip("/"), api_key
