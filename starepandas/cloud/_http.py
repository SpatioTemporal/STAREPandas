"""Minimal HTTP helper + error types for the cloud client SDK (C-6).

Uses the standard-library ``urllib.request`` so the SDK pulls in no new
dependency (the repo deliberately avoids ``requests``). All cloud calls go
through :func:`request`, which attaches the ``x-api-key`` header and decodes the
JSON body — including the body of an error response, so server ``{error}``
messages surface to the caller.
"""

import json
import urllib.error
import urllib.request


class CloudAPIError(Exception):
    """Base error for a non-success response from the cloud REST API.

    Attributes
    ----------
    status_code : int or None
        The HTTP status (None on a transport-level failure).
    payload : dict
        The decoded JSON body, if any (``{}`` otherwise).
    """

    def __init__(self, message, status_code=None, payload=None):
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload or {}


class IngestError(CloudAPIError):
    """``POST /ingest`` was rejected (e.g. 400 validation / cost cap, 500)."""


class JobNotFound(CloudAPIError):
    """``GET /jobs/{id}`` returned 404 — no such job."""


def request(method, url, api_key, body=None, timeout=30):
    """Make a JSON request to the cloud API.

    Parameters
    ----------
    method : str
        HTTP method (``GET``, ``POST``, ``DELETE``).
    url : str
        Fully-qualified URL.
    api_key : str
        Value for the ``x-api-key`` header.
    body : dict, optional
        JSON-serialised into the request body (sets ``Content-Type``).
    timeout : float
        Socket timeout in seconds.

    Returns
    -------
    tuple(int, dict)
        ``(status_code, decoded_json_body)``. The body is ``{}`` when empty or
        non-JSON.

    Raises
    ------
    CloudAPIError
        On a transport-level failure (DNS, connection, timeout). HTTP error
        *statuses* are returned, not raised — callers decide what counts as an
        error so they can read the server ``{error}`` message.
    """
    data = None
    headers = {"x-api-key": api_key, "Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"

    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, _decode(resp.read())
    except urllib.error.HTTPError as exc:
        # An HTTP error status (4xx/5xx) still carries a body we want to read.
        return exc.code, _decode(exc.read())
    except urllib.error.URLError as exc:
        raise CloudAPIError(
            "Failed to reach cloud API at %s: %s" % (url, exc.reason)
        ) from exc


def _decode(raw):
    if not raw:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return {}
