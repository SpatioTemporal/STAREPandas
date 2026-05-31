"""STARE-PODS cloud worker (Path C C-2).

Long-polls an SQS queue, processes tickets via
:func:`starepandas.ingest_granules_s3`, and exits cleanly after
``IDLE_THRESHOLD`` consecutive empty polls.

Run as the container entrypoint::

    python -m starepandas.cloud.worker

Ticket payload shape (produced by the scheduler Lambda in C-4)::

    {
        "job_id":        "<uuid4>",
        "instrument":    "GMI",
        "s3_prefix":     "s3://zarrpods/storage",   # optional
        "granule_uris":  ["s3://…/granule1.HDF5", …],
        "options":       {"scan": "S1", "level": 10, …}
    }

Config (env vars, no positional args):

* ``SQS_QUEUE_URL`` — required.
* ``JOBS_TABLE_NAME`` (default ``StarePodsJobs``) — PK ``job_id``.
* ``FAILURES_TABLE_NAME`` (default ``StarePodsFailures``) — PK ``job_id``,
  SK ``granule_uri``. Holds both success markers and failure rows; status
  endpoint filters on ``state='failed'``.
* ``AWS_REGION`` (default ``us-west-2``).
* ``IDLE_THRESHOLD`` (default ``3``).
* ``POLL_WAIT_SECONDS`` (default ``20`` — SQS long-poll max).
* ``WORK_DIR`` (default ``/tmp/starepods-worker``) — local scratch for
  downloaded granules.

§C10 #3 fix — the per-granule conditional-write makes the
``StarePodsJobs[job_id].processed`` counter idempotent under SQS at-least-once
redelivery. §C10 #9 / Decision 9 — on RDS auth errors (Secrets Manager
rotation in flight) the worker exits cleanly without deleting the message
so SQS redelivers to a fresh container.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import sys
import tempfile
from dataclasses import dataclass
from typing import Any, Dict, Optional

import boto3
import fsspec

import starepandas

logger = logging.getLogger(__name__)

_DEFAULT_REGION = "us-west-2"
_DEFAULT_JOBS_TABLE = "StarePodsJobs"
_DEFAULT_FAILURES_TABLE = "StarePodsFailures"
_DEFAULT_IDLE_THRESHOLD = 3
_DEFAULT_POLL_WAIT = 20
_DEFAULT_WORK_DIR = "/tmp/starepods-worker"

# Cap error strings to keep DynamoDB items well under the 400 KB limit and
# avoid blowing up the per-item attribute budget on giant tracebacks.
_ERROR_TRUNCATE_BYTES = 1024


def _is_rds_auth_error(exc: BaseException) -> bool:
    """True when ``exc`` looks like a credential-rotation failure.

    Decision 9 / §C10 #9: on these we exit cleanly and let SQS redeliver
    after the visibility timeout — the rotated secret reaches the next
    container via the ECS task definition's ``secrets`` block.
    """
    try:
        import psycopg2
    except ImportError:
        return False
    if not isinstance(exc, (psycopg2.OperationalError, psycopg2.InterfaceError)):
        return False
    msg = str(exc).lower()
    return any(
        marker in msg
        for marker in (
            "authentication failed",
            "password authentication",
            "role does not exist",
            "no password supplied",
        )
    )


@dataclass
class WorkerConfig:
    """Frozen runtime config — built once from env vars at start-up."""

    queue_url: str
    jobs_table: str = _DEFAULT_JOBS_TABLE
    failures_table: str = _DEFAULT_FAILURES_TABLE
    region: str = _DEFAULT_REGION
    idle_threshold: int = _DEFAULT_IDLE_THRESHOLD
    poll_wait: int = _DEFAULT_POLL_WAIT
    work_dir: str = _DEFAULT_WORK_DIR

    @classmethod
    def from_env(cls) -> "WorkerConfig":
        queue_url = os.environ.get("SQS_QUEUE_URL")
        if not queue_url:
            raise RuntimeError("SQS_QUEUE_URL env var is required")
        return cls(
            queue_url=queue_url,
            jobs_table=os.environ.get("JOBS_TABLE_NAME", _DEFAULT_JOBS_TABLE),
            failures_table=os.environ.get(
                "FAILURES_TABLE_NAME", _DEFAULT_FAILURES_TABLE
            ),
            region=os.environ.get("AWS_REGION", _DEFAULT_REGION),
            idle_threshold=int(
                os.environ.get("IDLE_THRESHOLD", _DEFAULT_IDLE_THRESHOLD)
            ),
            poll_wait=int(os.environ.get("POLL_WAIT_SECONDS", _DEFAULT_POLL_WAIT)),
            work_dir=os.environ.get("WORK_DIR", _DEFAULT_WORK_DIR),
        )


class _RDSAuthRotation(Exception):
    """Sentinel raised inside the worker to short-circuit back to the run
    loop's graceful-exit path. Carries the original psycopg2 error."""

    def __init__(self, original: BaseException):
        super().__init__(str(original))
        self.original = original


class Worker:
    """SQS-driven ingest worker.

    Public entrypoint: :meth:`run` (called by ``main`` / ``-m`` invocation).
    Smaller helpers are exposed for unit testing under moto.
    """

    def __init__(
        self,
        config: WorkerConfig,
        *,
        sqs=None,
        ddb=None,
        ingest_fn=None,
        fetch_fn=None,
    ):
        self.cfg = config
        self.sqs = sqs or boto3.client("sqs", region_name=config.region)
        self.ddb = ddb or boto3.client("dynamodb", region_name=config.region)
        # Injection points for tests; production uses the module defaults.
        self._ingest = ingest_fn or starepandas.ingest_granules_s3
        self._fetch = fetch_fn or self._default_fetch
        os.makedirs(self.cfg.work_dir, exist_ok=True)

    # ── ticket / granule processing ─────────────────────────────────────

    def _default_fetch(self, uri: str, dest_dir: str) -> str:
        """Download a remote granule URI to ``dest_dir`` and return the
        local path. Local paths (no scheme or ``file://``) are returned
        as-is without copying."""
        scheme_sep = uri.find("://")
        if scheme_sep == -1 or uri.startswith("file://"):
            return uri[7:] if uri.startswith("file://") else uri
        fname = os.path.basename(uri.rstrip("/")) or "granule.bin"
        local = os.path.join(dest_dir, fname)
        with fsspec.open(uri, "rb") as src, open(local, "wb") as dst:
            for chunk in iter(lambda: src.read(8 * 1024 * 1024), b""):
                dst.write(chunk)
        return local

    def _claim_granule(
        self,
        job_id: str,
        granule_uri: str,
        state: str,
        error: Optional[str] = None,
    ) -> bool:
        """§C10 #3 — conditionally record a per-granule outcome.

        Writes one item ``(job_id, granule_uri)`` to the failures-shaped
        table with ``attribute_not_exists(granule_uri)``. Returns True
        when we are the first writer for this granule (caller should
        increment the job-wide counter); False when another delivery
        already counted it.
        """
        item: Dict[str, Dict[str, str]] = {
            "job_id": {"S": job_id},
            "granule_uri": {"S": granule_uri},
            "state": {"S": state},
            "finished_at": {
                "S": datetime.datetime.now(datetime.timezone.utc)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z")
            },
        }
        if error:
            truncated = error.encode("utf-8")[:_ERROR_TRUNCATE_BYTES].decode(
                "utf-8", errors="ignore"
            )
            item["error"] = {"S": truncated}
        try:
            self.ddb.put_item(
                TableName=self.cfg.failures_table,
                Item=item,
                ConditionExpression="attribute_not_exists(granule_uri)",
            )
            return True
        except self.ddb.exceptions.ConditionalCheckFailedException:
            logger.info(
                "granule already counted job_id=%s uri=%s — skipping increment",
                job_id,
                granule_uri,
            )
            return False

    def _increment_job_counter(self, job_id: str, counter: str) -> None:
        if counter not in ("processed", "failed"):
            raise ValueError(f"unexpected counter name: {counter}")
        self.ddb.update_item(
            TableName=self.cfg.jobs_table,
            Key={"job_id": {"S": job_id}},
            UpdateExpression="ADD #c :one",
            ExpressionAttributeNames={"#c": counter},
            ExpressionAttributeValues={":one": {"N": "1"}},
        )

    def _process_uri(self, ticket: Dict[str, Any], uri: str) -> None:
        """Download, ingest, and record outcome for one granule URI.

        Raises :class:`_RDSAuthRotation` for credential-rotation errors
        so the outer run loop can exit cleanly without deleting the
        message. All other exceptions are caught here and recorded as
        ``state='failed'`` rows.
        """
        job_id = ticket["job_id"]
        with tempfile.TemporaryDirectory(dir=self.cfg.work_dir) as tmp:
            try:
                local = self._fetch(uri, tmp)
                self._ingest(
                    data_path=local,
                    instrument=ticket["instrument"],
                    s3_prefix=ticket.get("s3_prefix"),
                    **(ticket.get("options") or {}),
                )
            except Exception as exc:
                if _is_rds_auth_error(exc):
                    raise _RDSAuthRotation(exc) from exc
                logger.exception("granule failed job_id=%s uri=%s", job_id, uri)
                if self._claim_granule(job_id, uri, "failed", str(exc)):
                    self._increment_job_counter(job_id, "failed")
                return
        if self._claim_granule(job_id, uri, "processed"):
            self._increment_job_counter(job_id, "processed")

    def process_ticket(self, body: str) -> None:
        """Parse and process one SQS message body. Exposed for tests."""
        ticket = json.loads(body)
        for uri in ticket["granule_uris"]:
            self._process_uri(ticket, uri)

    # ── main loop ───────────────────────────────────────────────────────

    def run(self) -> int:
        """Long-poll the queue. Returns the desired exit code (0 on a
        clean idle-exit or on a credential-rotation graceful exit)."""
        idle = 0
        while True:
            resp = self.sqs.receive_message(
                QueueUrl=self.cfg.queue_url,
                WaitTimeSeconds=self.cfg.poll_wait,
                MaxNumberOfMessages=1,
            )
            msgs = resp.get("Messages", [])
            if not msgs:
                idle += 1
                logger.info(
                    "empty poll %d/%d", idle, self.cfg.idle_threshold
                )
                if idle >= self.cfg.idle_threshold:
                    logger.info("idle threshold reached — exiting")
                    return 0
                continue
            idle = 0
            msg = msgs[0]
            try:
                self.process_ticket(msg["Body"])
            except _RDSAuthRotation as e:
                logger.error(
                    "RDS auth error (%s) — exiting cleanly; SQS will redeliver",
                    e.original,
                )
                return 0
            except Exception:
                # Ticket-level failure that wasn't already attributed to a
                # specific granule (e.g. malformed JSON). Don't delete the
                # message — let visibility-timeout expire and SQS redeliver.
                logger.exception("ticket processing failed; leaving message for redelivery")
                continue
            self.sqs.delete_message(
                QueueUrl=self.cfg.queue_url,
                ReceiptHandle=msg["ReceiptHandle"],
            )


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    cfg = WorkerConfig.from_env()
    logger.info(
        "starting worker queue=%s jobs_table=%s failures_table=%s region=%s "
        "idle_threshold=%d poll_wait=%d",
        cfg.queue_url, cfg.jobs_table, cfg.failures_table, cfg.region,
        cfg.idle_threshold, cfg.poll_wait,
    )
    return Worker(cfg).run()


if __name__ == "__main__":  # pragma: no cover — exercised via -m
    sys.exit(main())
