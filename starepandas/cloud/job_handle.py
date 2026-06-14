"""``JobHandle`` — client-side handle to a cloud ingest job (C-6).

Returned by :func:`starepandas.cloud.ingest_granules`. Wraps the read-side of
the REST API (``GET /jobs/{id}``, ``GET /jobs/{id}/failures``,
``DELETE /jobs/{id}``) so a caller can poll a submitted job to completion:

    handle = sp.cloud.ingest_granules(granule_uris=[...], instrument="GMI")
    record = handle.wait()        # blocks until state in {complete, failed}
"""

import time

from starepandas.cloud._http import JobNotFound, request

#: Job states that mean the worker pipeline has finished (success or failure).
#: These are the exit conditions for :meth:`JobHandle.wait`.
TERMINAL_STATES = frozenset({"complete", "failed"})


class JobHandle:
    """Handle to a single submitted ingest job.

    Parameters
    ----------
    job_id : str
        The server-assigned job id.
    endpoint : str
        API base URL (no trailing slash).
    api_key : str
        ``x-api-key`` value.
    record : dict, optional
        The ``202`` body from ``POST /ingest`` — cached as the initial state so
        ``handle.record`` is populated before the first poll.
    """

    def __init__(self, job_id, endpoint, api_key, record=None):
        self.job_id = job_id
        self.endpoint = endpoint.rstrip("/")
        self.api_key = api_key
        #: Last-seen job record (updated by status()/wait()).
        self.record = record or {}

    def __repr__(self):
        state = self.record.get("state", "?")
        return "JobHandle(job_id=%r, state=%r)" % (self.job_id, state)

    @property
    def _job_url(self):
        return "%s/jobs/%s" % (self.endpoint, self.job_id)

    def status(self):
        """Fetch the current job record via ``GET /jobs/{id}``.

        Returns
        -------
        dict
            The full job record. Also cached on ``self.record``.

        Raises
        ------
        JobNotFound
            If the job id is unknown (404).
        """
        status_code, payload = request("GET", self._job_url, self.api_key)
        if status_code == 404:
            raise JobNotFound(
                payload.get("error", "Job %s not found" % self.job_id),
                status_code=404,
                payload=payload,
            )
        self.record = payload
        return payload

    def wait(self, timeout=None, poll_interval=10):
        """Poll until the job reaches a terminal state.

        Parameters
        ----------
        timeout : float, optional
            Maximum seconds to wait. ``None`` waits indefinitely.
        poll_interval : float
            Seconds between ``GET /jobs/{id}`` polls.

        Returns
        -------
        dict
            The terminal job record (``state`` in :data:`TERMINAL_STATES`).

        Raises
        ------
        TimeoutError
            If ``timeout`` elapses before a terminal state is reached.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            record = self.status()
            if record.get("state") in TERMINAL_STATES:
                return record
            if deadline is not None and time.monotonic() + poll_interval > deadline:
                raise TimeoutError(
                    "Job %s did not reach a terminal state within %s s "
                    "(last state: %r)"
                    % (self.job_id, timeout, record.get("state"))
                )
            time.sleep(poll_interval)

    def failures(self, next_token=None):
        """Fetch per-granule failure rows via ``GET /jobs/{id}/failures``.

        Parameters
        ----------
        next_token : str, optional
            Pagination cursor returned as ``next`` in a prior page.

        Returns
        -------
        dict
            ``{job_id, count, failures: [...], next?}``.
        """
        url = "%s/failures" % self._job_url
        if next_token:
            from urllib.parse import quote

            url = "%s?next=%s" % (url, quote(str(next_token)))
        _, payload = request("GET", url, self.api_key)
        return payload

    def cancel(self):
        """Attempt to cancel the job via ``DELETE /jobs/{id}``.

        Cancellation is deferred in v1 (§C11 Decision 4); the server always
        returns ``501`` and this raises :class:`NotImplementedError`.
        """
        request("DELETE", self._job_url, self.api_key)
        raise NotImplementedError(
            "Job cancellation is not implemented in v1 (deferred per §C11 "
            "Decision 4); DELETE /jobs/%s returns 501." % self.job_id
        )
