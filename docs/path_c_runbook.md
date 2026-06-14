# STARE-PODS cloud ingest — operations runbook

How to **submit**, **inspect**, and **operate** the Path C cloud ingest service.
Goal: a second person can run and troubleshoot a job using only this document.

- **Region / account:** `us-west-2` / `637388276731`
- **Stack:** `StarePodsInfraStack` (AWS CDK, `infra/cdk/`)
- **Design / history:** `docs/path_c_implementation.md` (local-only); `stare_pods_aws_parallel_plan.html` §C.

---

## 1. Architecture in one paragraph

`POST /ingest` (API Gateway + `starepods-scheduler` Lambda) validates the request,
splits the granule list into **tickets**, enqueues them on SQS `starepods-tickets`,
bumps `JobsControl.active_jobs`, and scales the ECS service `starepods-workers` up.
Up to **4** Fargate workers pull tickets (one at a time), read each granule from
S3, write Parquet partitions + RDS `PodsMetadata` rows, and update the job's
`processed`/`failed` counters in DynamoDB. The `starepods-completion-watcher`
Lambda (EventBridge `rate(1 min)`) closes drained jobs (`running→complete|failed`),
decrements `active_jobs`, scales ECS back to **0**, and POSTs the callback. Job
state is read with `GET /jobs/{id}`.

```
client ─POST /ingest─► API GW ─► scheduler ─► SQS tickets ─► ECS workers ─► S3 (Parquet) + RDS (metadata)
                                     │                            │
                              JobsControl++                  job counters (DDB)
                                                                  │
                          EventBridge 1-min ─► completion-watcher ─► close job + scale ECS→0 + callback
```

---

## 2. Prerequisites

```bash
conda activate starepandas_3.12_v3
cd <repo>/stare_pods_aws_parallel
```

- **Config:** `starepandas/.config` (gitignored) holds S3/RDS creds plus the
  cloud SDK keys `endpoint` + `api_key`. Point the loader at it:
  `export STAREPANDAS_AWS_CONFIG="$PWD/starepandas/.config"`.
  - Alternatively set `STAREPANDAS_CLOUD_ENDPOINT` / `STAREPANDAS_CLOUD_API_KEY`.
- **AWS CLI / boto3 ops** below assume zarpodder creds (in `.config`). The CDK
  helper `infra/cdk/cdk-zarpodder.sh <cmd>` exports them without echoing secrets.
- **Granules must be in S3** (workers pull by `s3://` URI) and have **datable
  filenames** for the instrument (GMI/AMSR2/SSMIS/ATMS) — the scheduler 400s an
  undatable name. One job = **one instrument**.

---

## 3. Submit a job

### 3a. Python SDK (preferred)

```python
import starepandas as sp

handle = sp.cloud.ingest_granules(
    granule_uris=["s3://bucket/path/granule1.HDF5", ...],  # one instrument's granules
    instrument="SSMIS",                                    # GMI | AMSR2 | SSMIS | ATMS
    workers=4,                                             # default 4; hard cap 4
    s3_prefix="s3://zarrpods/testing-s3/loadtest-jan/storage",  # optional; else worker default
    callback_url="https://…",                             # optional; watcher POSTs terminal payload
    options=None,                                          # optional; passed to the worker ingest
)
print(handle.job_id, handle.record)        # 202 body: state=running, total_granules, ticket_count, workers
record = handle.wait(poll_interval=15)     # blocks until state in {complete, failed}
assert record["state"] == "complete"
# also: handle.status(), handle.failures(), handle.cancel() (-> NotImplementedError, 501)
```
- Lists up to ~4 MiB are sent inline; larger lists are auto-uploaded to
  `s3://zarrpods/_jobs/{uuid}.json` and sent as a pointer.
- **Do not blindly retry `POST /ingest`** — each POST mints a *new* `job_id`
  (the pipeline is idempotent on data, not on jobs).

### 3b. curl

```bash
EP=https://yujvpbbs7j.execute-api.us-west-2.amazonaws.com/v1
KEY=<api-key-value>      # see §9 to fetch it

curl -sS -X POST "$EP/ingest" -H "x-api-key: $KEY" -H 'content-type: application/json' \
  -d '{"instrument":"SSMIS","workers":4,
       "granule_uris":["s3://bucket/g1.HDF5","s3://bucket/g2.HDF5"]}'
# -> 202 {"job_id":"…","state":"running","total_granules":2,"ticket_count":1,"workers":4}
```
Validation failures return `400 {"error":…}` (e.g. `workers>4`, undatable filename, cost cap).

---

## 4. Inspect a job

### 4a. Status endpoint / SDK
```bash
curl -sS "$EP/jobs/<job_id>" -H "x-api-key: $KEY" | jq
# states: enqueued -> running -> complete | failed
curl -sS "$EP/jobs/<job_id>/failures" -H "x-api-key: $KEY" | jq   # per-granule failures (+ ?next= page)
```
```python
sp.cloud.JobHandle("<job_id>", endpoint, api_key).status()
```

### 4b. DynamoDB (job record + control counter)
```bash
aws dynamodb get-item --region us-west-2 --table-name StarePodsJobs \
  --key '{"job_id":{"S":"<job_id>"}}' | jq
# the scale-down gate (singleton item in the same table):
aws dynamodb get-item --region us-west-2 --table-name StarePodsJobs \
  --key '{"job_id":{"S":"JobsControl"}}' | jq '.Item.active_jobs'
```

### 4c. Workers, queue, logs
```bash
# how many workers are running
aws ecs describe-services --region us-west-2 --cluster starepods \
  --services starepods-workers --query 'services[0].{desired:desiredCount,running:runningCount}'
# queue depth (visible + in-flight)
aws sqs get-queue-attributes --region us-west-2 \
  --queue-url https://sqs.us-west-2.amazonaws.com/637388276731/starepods-tickets \
  --attribute-names ApproximateNumberOfMessages ApproximateNumberOfMessagesNotVisible
# worker logs (ECS log group, 30-day retention)
aws logs tail /ecs/starepods-worker --region us-west-2 --since 30m --follow
```

---

## 5. Knobs & defaults

| Knob | Where | Default | Notes |
|---|---|---|---|
| `MAX_TICKET_SIZE` | `ticket_sizing.py` + vendored copy + scheduler env | **40** | max granules/ticket; kept at 40 (see §8 visibility-timeout headroom) |
| `MIN_TICKET_SIZE` | `ticket_sizing.py` | **1** | scheduler passes only MAX today → MIN=1 |
| `workers` cap (`WORKER_CAP`) | scheduler | **4** | `>4` → 400 |
| ticket size formula | — | `max(MIN, min(MAX, ceil(N/W)))` | balances across W; caps at MAX; work-steals beyond W·MAX |
| SQS visibility timeout | `cdk.json queueVisibilityTimeoutSeconds` | **21600 (6 h)** | redelivery window; see §8 duplicate-processing caveat |
| `maxReceiveCount` → DLQ | `cdk.json queueMaxReceiveCount` | **3** | then → `starepods-tickets-dlq` |
| `IDLE_THRESHOLD` / `POLL_WAIT_SECONDS` | worker env | **3 / 20 s** | empty polls before a worker idle-exits |
| task size | `cdk.json taskCpu/taskMemoryMiB` | **2 vCPU / 8 GB** | |
| `MAX_PARTITION_LEVEL` | `staredataframe.py` | **4** | Parquet trixel granularity (revisit in local-worker opt) |
| Budgets threshold | `cdk.json budgetMonthlyUsd` | **$200** | tag-filtered `Project=starepods` |

After changing a ticket-sizing constant, **update the vendored copy**
(`infra/cdk/lambdas/common/ticket_sizing.py`) too — the parity test fails
otherwise — then redeploy (§7).

---

## 6. Operations

### 6a. Stuck / stalled job
Symptoms: `state=running` long after workers should be done, or queue not draining.
1. Check queue depth + worker count (§4c). If messages are in-flight
   (`…NotVisible > 0`) workers are still holding tickets — wait (visibility is 6 h).
2. Check worker logs (§4c) for errors / a hung granule.
3. If a worker **stalled** (in-flight but no progress): stop the task; SQS
   redelivers the ticket after the visibility window. Safe — writes are idempotent.
   ```bash
   aws ecs list-tasks --region us-west-2 --cluster starepods --service-name starepods-workers
   aws ecs stop-task --region us-west-2 --cluster starepods --task <taskArn> --reason "stalled"
   ```
4. **Abandon a job** (purge its remaining work): purge the queue (affects ALL
   jobs — only when one job owns the queue) and scale to 0:
   ```bash
   aws sqs purge-queue --region us-west-2 --queue-url <tickets-url>
   aws ecs update-service --region us-west-2 --cluster starepods \
     --service starepods-workers --desired-count 0
   ```
   Then reconcile the counter (§6b).

### 6b. `JobsControl.active_jobs` drift / forced scale-to-0
The watcher decrements `active_jobs` and scales to 0 when it hits 0. If you
manually purged/abandoned, the counter can be left > 0 (service never scales
down). Reset it and scale down by hand:
```bash
aws dynamodb put-item --region us-west-2 --table-name StarePodsJobs \
  --item '{"job_id":{"S":"JobsControl"},"active_jobs":{"N":"0"}}'
aws ecs update-service --region us-west-2 --cluster starepods \
  --service starepods-workers --desired-count 0
```

### 6c. RDS UNIQUE-conflict diagnosis
`PodsMetadata` has `pods_unique UNIQUE ("Dataset","RawData Collected Time",grouped_id)`
and inserts are `ON CONFLICT … DO UPDATE` (idempotent). A spike in write
conflicts usually means **duplicate ticket delivery** (visibility-timeout
overrun or duplicate scheduler invocation). It is *not* data corruption — the
upsert refreshes the row. To confirm, compare `processed` in the job record vs
distinct partitions in S3, and check worker logs for redelivery of the same
`granule_uri`.

### 6d. Dead-letter queues
```bash
# tickets that failed maxReceiveCount=3 times
aws sqs get-queue-attributes --region us-west-2 \
  --queue-url https://sqs.us-west-2.amazonaws.com/637388276731/starepods-tickets-dlq \
  --attribute-names ApproximateNumberOfMessages
# callbacks that failed 3x (watched by alarm starepods-callbacks-dlq-depth)
aws sqs receive-message --region us-west-2 \
  --queue-url https://sqs.us-west-2.amazonaws.com/637388276731/starepods-callbacks-dlq \
  --max-number-of-messages 10
```
To **reprocess** a DLQ'd ticket: read it, re-`send-message` to `starepods-tickets`
(or re-submit the job for those granules). Delete from the DLQ once handled.

### 6e. Rotate the worker secret (no in-flight breakage)
Secret `starepods/worker/.config` (Secrets Manager) is injected at task start via
`STAREPANDAS_WORKER_SECRET`. In-flight workers already hold their copy.
1. Update the secret value:
   ```bash
   aws secretsmanager put-secret-value --region us-west-2 \
     --secret-id starepods/worker/.config --secret-string file://new.config.json
   ```
2. New tasks pick it up automatically. To force a refresh, scale to 0 and let the
   next job start fresh tasks.
3. **RDS credential rotation:** the worker exits gracefully on an auth error
   (`_is_rds_auth_error` → Decision 9) and SQS redelivers, so a rotation mid-job
   self-heals on the next task.

### 6f. Roll back / update the worker image
Image: `637388276731.dkr.ecr.us-west-2.amazonaws.com/starepods/worker:<tag>`
(default tag `dev`; controlled by `cdk.json starepods:workerImageTag`).
- **Roll back:** push/point `workerImageTag` to a known-good tag (or digest) and
  redeploy (§7). The task definition picks up the new image on the next task.
- New tasks only — running tasks keep their image until they exit.

### 6g. Deploy / diff (CDK)
```bash
./infra/cdk/cdk-zarpodder.sh diff   StarePodsInfraStack
./infra/cdk/cdk-zarpodder.sh deploy StarePodsInfraStack --require-approval never
```
Lambda code (scheduler/status/watcher) and the vendored helpers ship from the
`infra/cdk/lambdas` asset — editing them changes the asset hash and a deploy
updates all three functions.

---

## 7. Common error → likely cause

| Symptom | Likely cause | Action |
|---|---|---|
| `POST /ingest` → 400 `workers exceeds…` | `workers > 4` | resubmit with `workers≤4` |
| `POST /ingest` → 400 undatable filename | filename not parseable for the instrument | fix the name / instrument; only GMI/AMSR2/SSMIS/ATMS supported |
| `POST /ingest` → 400 cost cap | request exceeds the §C9 cap | reduce scope |
| Job stuck `running`, queue empty, workers 0 | watcher didn't close (rare) or counter drift | check watcher logs; reconcile §6b |
| Job stuck `running`, in-flight messages | worker slow/stalled on a ticket | wait (6 h window) or stop task (§6a) |
| Same granule processed twice | visibility-timeout overrun or duplicate scheduler invoke | benign (idempotent); see §6c — consider heartbeat (deferred) |
| RDS write-conflict spike | duplicate ticket delivery | §6c; upsert handles it |
| `starepods-callbacks-dlq` depth > 0 (alarm) | callback receiver down / rejecting | inspect payloads §6d; fix receiver; replay if needed |
| Tasks fail to start | bad image tag / secret / NAT | check ECS task stopped-reason + worker logs |
| Budgets alarm silent | `Project` cost-allocation tag not activated in Billing | activate the tag (one-time) |

---

## 8. Load test reference (run #1, 2026-06-14)

69 SSMIS granules, `workers=4`, job `5ab7ec3b-…`:

| Metric | Value |
|---|---|
| Result | complete, 69/69, 0 failed |
| Tickets / workers | 4 / 4 (`ceil(69/4)=18`) |
| Wall-time | ~24 min (1443 s) |
| Throughput | ~2.9 granules/min (~21 s/granule aggregate); ~77 s/granule per worker |
| Cold start | ~120–138 s |
| Parquet objects | 104,465 (~1,514/granule) |
| Cost | ≈ **$0.80** — S3 PUTs ~$0.52 > Fargate ~$0.20 > NAT ~$0.09 |

**Tuning notes carried out of run #1:**
- **Visibility timeout raised 1 h → 6 h** so a slow instrument (SSMIS ~77 s/granule
  → 40-granule ticket ≈ 51 min) fits comfortably; `MAX_TICKET_SIZE` kept at 40.
  ⚠️ This does *not* eliminate duplicate processing — a ticket past 6 h or a
  stalled worker is still redelivered, so two workers may process the same ticket.
  Tolerated because writes are idempotent; a worker **heartbeat** is the only full
  fix (deferred).
- Per-granule cost is dominated by **tiny-Parquet-object writes** (S3 PUT > compute),
  not STARE math — revisit `MAX_PARTITION_LEVEL` in the local-worker optimization.
- The `>160`-granule **work-stealing regime is not yet validated** (deferred).

For Budgets calibration: a 1000-granule SSMIS run extrapolates to ~$0.80 × (1000/69)
≈ **~$12** per run (PUT-dominated); set the monthly threshold from your expected
run cadence × that figure.

---

## 9. Resource inventory

| Resource | Name / value |
|---|---|
| API endpoint | `https://yujvpbbs7j.execute-api.us-west-2.amazonaws.com/v1/` |
| API key id | `ozx438r4cd` — value: `aws apigateway get-api-key --api-key ozx438r4cd --include-value --region us-west-2 --query value --output text` (zarpodder has `starepods-read-apikey`) |
| ECS cluster / service | `starepods` / `starepods-workers` (desiredCount 0 idle) |
| SQS tickets / DLQ | `starepods-tickets` (vis 6 h, maxReceive 3) / `starepods-tickets-dlq` |
| SQS callbacks DLQ | `starepods-callbacks-dlq` (alarm `starepods-callbacks-dlq-depth`) |
| DynamoDB | `StarePodsJobs` (PK `job_id`; also holds `JobsControl` singleton), `StarePodsFailures` (PK `job_id`, SK `granule_uri`); TTL `expires_at` |
| RDS (Postgres) | `starepodsmetadata.cgxwy3lllofm.us-west-2.rds.amazonaws.com:5432` db `postgres`, table `PodsMetadata` (UNIQUE `pods_unique`) |
| Secrets Manager | `starepods/worker/.config` |
| Lambdas | `starepods-scheduler`, `starepods-status`, `starepods-completion-watcher` |
| EventBridge | `starepods-completion-tick` (`rate(1 min)`) |
| ECR image | `637388276731.dkr.ecr.us-west-2.amazonaws.com/starepods/worker:dev` |
| Budgets | `starepods-monthly` ($200, tag `Project=starepods`) |
| CDK helper | `infra/cdk/cdk-zarpodder.sh <diff|deploy|destroy>` |
| Log groups (30-day) | `/ecs/starepods-worker`, scheduler/status/watcher Lambda groups |

---

*Last updated: 2026-06-14 (C-7, after load test #1 + 6 h visibility timeout).*
