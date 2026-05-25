# Path C — Step-by-Step Implementation Plan

Companion to `stare_pods_aws_parallel_plan.html` §C. The HTML describes **what** Path C is; this document is the actionable **how**, with concrete file paths, function signatures, AWS resource names, and verification gates.

**Total effort:** ~25.5 days of focused work across 7 phases.

**Current status (2026-05-25):**
- ✅ **C-1 COMPLETE** — landed on `stare_pods_aws_parallel` in 7 commits (`5e33cb6`, `f890af9`, `18b16c2`, `c84dea7`, `e39fd99`, `d180a0d`, + this slice). Refactor + §C10 #1/#2 idempotency fixes + S3 layout alignment + full-granule parity all verified end-to-end against live RDS + S3 + local SQLite.
- ⏳ **C-2 through C-7** — not started. C-2 onward is unblocked: the cloud worker imports `starepandas.ingest_granules_s3` directly via the task-7 module extraction.

**Conventions:**
- Each phase starts with **Prerequisites** (blocking dependencies) and ends with **Definition of done** (DoD) — both must be true before moving on.
- Steps reference the HTML plan by section (e.g. §C10 #1 = critical finding 1 in the post-design review).
- Verification gates use the existing CLAUDE.md skills (`/basic-verification-stare-pandas`, `/stare-pods-verification`).
- AWS resources use the names already declared in §C6.
- All AWS resources must be tagged `Project=starepods` for the §C9 Budgets alarm.

**Branch strategy:**
- **C-1 has landed on `stare_pods_aws_parallel`** — refactor + idempotency fixes already in main project branch.
- **C-2 through C-7 develop on a feature branch** (e.g. `path-c-cloud-service`) and only become user-visible at C-6.

**Open decisions (all resolved for v1):**
- Decision 3 (concurrent-job model) → **Shared workers** (one SQS queue + one ECS service; DynamoDB `JobsControl#active_jobs` counter gates scale-down). Baked into C-3 / C-5 below.
- Decision 4 (cancellation semantics) → **No cancel in v1** (`DELETE /jobs/{id}` returns 501). Baked into C-4 below.

---

## C-1. Library refactor + idempotency fixes ✅ COMPLETE (2026-05-25)

**Goal:** Move ingest functions out of `demo_lib.py`, introduce the `MetadataStore` interface, and fix §C10 #1 and #2 in the RDS schema so both the local-host pipeline and future cloud workers are safe under retries.

**Status:** All planned work shipped. Six commits, ~12 hours of focused work, plus two follow-up commits for demo/CLAUDE.md alignment and full-granule parity. End-to-end verified live against AWS S3 + RDS Postgres + local SQLite.

**Shipped commits (chronological):**

| # | Hash | Description |
|---|---|---|
| 1 | `5e33cb6` | refactor(C-1): extract `MetadataStore` + `cloud/ticket_sizing` modules |
| 2 | `f890af9` | fix(C-1): RDS write retries + per-granule timestamp derivation (§C10 #2) |
| 3 | `18b16c2` | fix(C-1): UNIQUE constraint + `ON CONFLICT DO UPDATE` (§C10 #1) |
| 4 | `c84dea7` | feat(C-1, task 12): align S3 layout with local (HTM→granule→dataset) + `default_s3_prefix` |
| 5 | `e39fd99` | refactor(C-1, task 7): extract ingest functions into `starepandas/ingest.py` |
| 6 | `d180a0d` | fix(demo): update `s3_starepods_examples.py` for task-12 layout + refresh CLAUDE.md |
| 7 | *(pending commit)* | feat(reconstitute): accept `bbox=None` + `area_sids=None` as "full granule" (task 13 parity with local) + S3 demo BBOX=None + notebook sync |

### Steps (status)

- [x] **Created `starepandas/ingest.py`** *(commit `e39fd99`, task 7)*
  - [x] `ingest_granules_local(data_path, instrument, local_root="/tmp/stare_pods_local", scan=None, level=10, db_path=None, **kw)`.
  - [x] `ingest_granules_s3(data_path, instrument, s3_prefix=None, scan=None, level=10, clean_before_run=False, **kw)`.
  - [x] `clean_s3_prefix(s3_prefix)` extracted alongside.
  - [x] `LocalStarePodsDemo.ingest_granules` and `StarePodsDemo.ingest_granules` are now ~10-line shims that delegate.
  - [x] Re-exported at top level: `starepandas.ingest_granules_s3`, `starepandas.ingest_granules_local`, `starepandas.clean_s3_prefix` — the import path the Path C worker (C-2) will use without pulling in the demo classes.

- [x] **Created `starepandas/metadata.py` — `MetadataStore` interface (§C9 M4 hedge)** *(commit `5e33cb6`)*
  - [x] `MetadataStore` `Protocol` with `write_partitions(rows)`, `find(**filters)`, `delete_by_prefix(s3_prefix)`.
  - [x] `RDSMetadataStore` wraps the existing psycopg2 paths (INSERT, SELECT, DELETE).
  - [x] No `DynamoDBMetadataStore` yet — triggers for adding it live in §C9.
  - [x] `STAREDataFrame.to_s3` and `StarePodsDemo.clean_s3_prefix` both routed through the new interface.

- [x] **Created `starepandas/cloud/` package skeleton** *(commit `5e33cb6`)*
  ```
  starepandas/cloud/
  ├── __init__.py          ← re-exports split_into_tickets
  ├── ticket_sizing.py     ← pure functions (§C2)
  └── config.py            ← placeholder (filled in at C-6)
  ```

- [x] **Implemented ticket sizing (§C2) in `cloud/ticket_sizing.py`** *(commit `5e33cb6`)*
  - [x] `split_into_tickets(granule_uris, workers, max_ticket_size=40) -> list[list[str]]`.
  - [x] 18 unit tests cover all four rows of §C2's table plus boundary cases.

- [x] **Fixed §C10 #1 — `PodsMetadata` INSERTs are now idempotent** *(commit `18b16c2`)*
  - [x] Pre-flight against the 201,115-row live table on 2026-05-25 confirmed all rows had non-zero microseconds — i.e. every row was stamped with `utcnow()`, none were real timestamps. Cleanest path: `TRUNCATE`.
  - [x] Companion S3 cleanup: 173,069 zarr-era objects across 6 granule-named prefixes deleted; only 5 named project prefixes remain (`amsr2-demo`, `gmi-demo`, `gmi-demo-parquet`, `ssmis-demo`, `testing-s3`).
  - [x] `TRUNCATE TABLE "PodsMetadata"` + `ALTER TABLE "PodsMetadata" ADD CONSTRAINT pods_unique UNIQUE ("Dataset", "RawData Collected Time", grouped_id)` applied live.
  - [x] `RDSMetadataStore.write_partitions` now uses `INSERT … ON CONFLICT ("Dataset", "RawData Collected Time", grouped_id) DO UPDATE SET "MetadataJson"=EXCLUDED."MetadataJson"` — both in the batch and the row-by-row fallback, via a shared `_ON_CONFLICT_CLAUSE` constant.
  - [x] Regression test live: insert same `PartitionRow` twice → 1 row, not 2; insert with mutated MetadataJson → DO UPDATE refreshes the JSON. Also wired into `starepods_verify.py` as check #9 so it stays a permanent gate.

- [x] **Fixed §C10 #2 — stopped defaulting `raw_collected_time` to `utcnow()`** *(commit `f890af9`)*
  - [x] `io.granules.to_s3` derives `raw_collected_time` from the granule filename via `starepandas/io/granules/_timestamps.py` (GMI / SSMIS / ATMS / AMSR2 / MODIS patterns).
  - [x] `STAREDataFrame.to_s3` raises `ValueError` when `raw_collected_time` is `None` instead of silently falling back to `utcnow()`.
  - [x] Without this, the §C10 #1 UNIQUE constraint couldn't dedup retries.

- [x] **Aligned S3 layout with local — HTM → granule → dataset** *(commit `c84dea7`, task 12)*

  **What was wrong:** `to_s3` and `to_local` produced *inverted* layouts.

  | Surface | Pre-fix layout |
  |---|---|
  | `to_local` | `<root>/Q00_X/.../QN_M/<granule>/<dataset>.parquet` ✅ |
  | `to_s3` + how `demo_lib` built `s3_prefix` | `s3://…/<prefix>/<granule>/Q00_X/.../QN_M/<dataset>.parquet` ❌ inverted |

  **Now (unified):**

  ```
  LOCAL:  <root>/Q00_X/Q01_Y/.../QN_M/<granule>/<dataset>.parquet
  S3:     s3://zarrpods/storage/Q00_X/Q01_Y/.../QN_M/<granule>/<dataset>.parquet
                       └─ default_s3_prefix (new .config field) ─┘
  ```

  - [x] `STAREDataFrame.to_s3` accepts `granule_name=None`; when set, splices between HTM segments and dataset leaf — mirrors `to_local` lines 1973-1983 exactly.
  - [x] `io.granules.to_s3` derives `granule_name` from `file_path` basename and threads it to all three `df.to_s3()` call sites (auto-detect scans, explicit scan, single-DataFrame).
  - [x] `StarePodsDemo.ingest_granules` (now a shim) no longer pre-appends granule basename; passes prefix directly.
  - [x] `.config` carries `default_s3_prefix=s3://zarrpods/storage`.
  - [x] `_load_config_from_default_locations` parses the new field; populates module-level `_DEFAULT_S3_PREFIX`.
  - [x] `io.granules.to_s3` falls back to `_DEFAULT_S3_PREFIX` when `s3_path` is omitted; raises a clear error if neither is configured.
  - [x] `reconstitute_hdf5_from_s3` works unchanged for the new layout because the S3 reader uses RDS-cataloged `group_path` directly, and the local walker already filters `parts[:-1]` for `Q*`-prefix segments (which naturally skips a granule sub-dir).
  - [x] 6 unit tests in `tests/test_s3_layout.py` cover `granule_name` validation, `default_s3_prefix` loading (present / with trailing slash / absent), and full-layout shape via `to_local`.
  - [x] Existing `gmi-demo-parquet/<granule>/HTM/…` data stays in the old layout — no migration; new writes use the new layout.

- [x] **Task 13 — full-granule parity** *(this slice, pending commit)*

  After task 12 the S3 demo still differed from the local demo: `BBOX = (115,-30,120,-25)` (Perth subset) vs the local demo's `BBOX = None` (full granule). The `reconstitute_hdf5_from_s3` signature also required exactly-one of `bbox` / `area_sids` (raising on both-None), so the S3 demo couldn't simply be flipped to `None`.

  - [x] `reconstitute_hdf5_from_s3` — both-None now means "no spatial filter; include every partition for the dataset"; the storage-level coercion + `effective_query_ids` filter is skipped in that branch.
  - [x] `StarePodsDemo.reconstitute_hdf5` — relaxed `(bbox is None) == (area_sids is None)` to `bbox is not None and area_sids is not None` (at most one). Log line handles all three cases.
  - [x] `s3_starepods_examples.py` — `BBOX = None` (matches local demo).
  - [x] `s3_starepods_examples.ipynb` — synced with the .py (4 cells updated: config, ingest, find, reconstitute, RDS verify); outputs cleared.
  - [x] Verified live: S3 demo with `BBOX=None` reconstitutes the full granule (131.81 s) producing 263 GMI_S1 + 251 GMI_S2 partitions — identical to the local demo's counts.

- [x] **Worth-tightening — RDS batch INSERT retries** *(commit `f890af9`)*
  - [x] `tenacity.retry` (3 attempts, exponential backoff) around the batch INSERT in `RDSMetadataStore.write_partitions`.
  - [x] Retries only `OperationalError` / `InterfaceError`; non-transient errors fall through to the row-by-row fallback unchanged.
  - [x] `rds_write_failures` counter logged via stdlib `logging`.

- [x] **Verification**
  - [x] Created `~/.claude/scripts/starepandas_verify.py` (basic API gate; was documented in CLAUDE.md but missing on disk). **6/6 PASS** as of 2026-05-25.
  - [x] `~/.claude/scripts/starepods_verify.py` extended from 7 → 10 checks: added (a) pytest of all C-1 unit-test files, (b) `pods_unique` constraint existence assertion, (c) live idempotency regression via `RDSMetadataStore`. **10/10 PASS** as of 2026-05-25.
  - [x] Live demos pass end-to-end in `starepandas_3.12_v3` conda env: `local_starepods_examples.py` and `s3_starepods_examples.py` both write 263 GMI_S1 + 251 GMI_S2 partitions; reconstituted HDF5 is byte-identical to the original for every dataset (44/44 across `/S1` + `/S2`, including subgroups). The only deltas are reconstitute-stamped provenance attributes (`PixelWidth`, `ReconstitutionDate`, `StarePodsReconstitution`), which is intentional.

### DoD — achieved

- [x] `demo_lib.py` is ~150 lines lighter; three demo methods are thin shims.
- [x] All existing notebooks run unchanged (`local_starepods_examples.ipynb` needed no edits; `s3_starepods_examples.ipynb` synced to the post-task-12/13 .py script).
- [x] Re-running `to_s3` on the same granule produces zero new `PodsMetadata` rows (live regression in starepods_verify.py check #9).
- [x] All 10 STARE-PODS verification checks pass + 6 basic verification checks pass + 56 unit tests pass.
- [x] Data fidelity confirmed: S3 and local reconstituted HDF5s have all 44 datasets byte-identical to the original granule.

---

## C-2. Worker container (4 days)

**Goal:** A Docker image that long-polls SQS, processes tickets via `ingest_granules_s3`, and exits cleanly. Pushed to ECR, manually tested against a real SQS queue.

**Prerequisites:** C-1 complete. (Worker imports `starepandas.ingest.ingest_granules_s3`.)

### Steps

- [ ] **Dockerfile** (`infra/worker/Dockerfile`)
  - [ ] Base: `python:3.12-slim`.
  - [ ] Install `pystare`, `starepandas`, `boto3`, `fsspec`, `tenacity`.
  - [ ] COPY entrypoint script; CMD `python -m starepandas.cloud.worker`.

- [ ] **Worker entrypoint** (`starepandas/cloud/worker.py`)
  - [ ] Long-poll loop per §C3 pseudocode.
  - [ ] `IDLE_THRESHOLD = 3` consecutive empty polls → `sys.exit(0)`.
  - [ ] Per-ticket: iterate granule URIs, call `ingest_granules_s3`, update DynamoDB.

- [ ] **Fix §C10 #3 — DynamoDB conditional `processed` write**
  - [ ] Per-granule key in `StarePodsFailures`-shaped table: PK=`job_id`, SK=`granule_uri`.
  - [ ] Increment `StarePodsJobs[job_id].processed` only if the per-granule key didn't already exist (`attribute_not_exists` condition).
  - [ ] On `ConditionalCheckFailedException`, silently skip increment (granule was already counted by a previous delivery).

- [ ] **Fix §C10 #5 — visibility-timeout safety**
  - [ ] Option A: raise queue visibility to 60 min (simplest).
  - [ ] Option B: worker calls `ChangeMessageVisibility(+15 min)` every 10 min while processing a ticket.
  - [ ] **Recommend A for v1**; revisit if 60 min holds tickets too long during failure modes.

- [ ] **Decision 9 — Secrets Manager rotation behaviour** (§C10 #9)
  - [ ] v1: worker exits cleanly on RDS auth error and lets SQS redeliver. Document this as the chosen option.

- [ ] **Local Docker test**
  - [ ] `docker build -t starepods-worker:dev .`
  - [ ] Create test SQS queue + DynamoDB table in `us-west-2`.
  - [ ] Drop a hand-crafted ticket JSON into the queue.
  - [ ] Run the container locally with AWS creds mounted; verify it processes the ticket and exits after 60 s idle.

- [ ] **ECR push**
  - [ ] Create ECR repo `starepods/worker` (creation moved earlier from C-3 to unblock testing).
  - [ ] `aws ecr get-login-password | docker login …`; tag + push `:dev`.

### DoD

- Container processes a real ticket against test S3 + RDS + DynamoDB.
- Container exits on its own after `IDLE_THRESHOLD` empty polls.
- Re-delivering the same ticket (purge + re-enqueue) does not double-increment `processed`.
- Image is in ECR at `starepods/worker:dev`.

---

## C-3. AWS infrastructure (4 days)

**Goal:** All AWS resources from §C6 provisioned via CDK or Terraform, tagged for cost tracking.

**Prerequisites:** C-2 image in ECR. Decision 3 = **shared workers** (one SQS queue + one ECS service); this section reflects that choice.

### Steps

- [ ] **Network**
  - [ ] VPC `starepods-vpc` (10.0.0.0/16) in `us-west-2`.
  - [ ] 2 private subnets in different AZs (10.0.1.0/24, 10.0.2.0/24).
  - [ ] No NAT gateway (avoid §C10 NAT cost trap).
  - [ ] VPC endpoints (Gateway): S3, DynamoDB.
  - [ ] VPC endpoints (Interface): SQS, Secrets Manager, ECR, ECR Docker, CloudWatch Logs.

- [ ] **ECS cluster + worker service**
  - [ ] Cluster `starepods` (Fargate).
  - [ ] Task definition `starepods-worker:1`: **2 vCPU / 8 GB** (§C10 worth-tightening — start small, scale up post-Path A).
  - [ ] Inject Secrets Manager secret via `secrets` block at `/etc/starepods/.config`; set `STAREPANDAS_AWS_CONFIG` env var.
  - [ ] Service `starepods-workers`, `desiredCount=0` default.
  - [ ] CloudWatch log group with 30-day retention.

- [ ] **SQS**
  - [ ] Queue `starepods-tickets` (visibility 60 min per C-2 Option A, max-receives → DLQ = 3).
  - [ ] DLQ `starepods-tickets-dlq`.
  - [ ] DLQ `starepods-callbacks-dlq` (for failed webhook posts; created here to keep all queues together).

- [ ] **DynamoDB**
  - [ ] Table `StarePodsJobs`: PK `job_id`, attributes for `state`, `total_granules`, `processed`, `failed`, `created_at`, `s3_prefix`, `callback_url`. TTL on `expires_at` set to 30 days from creation.
  - [ ] Table `StarePodsFailures`: PK `job_id`, SK `granule_uri`. TTL 30 days.
  - [ ] **`JobsControl` item** in `StarePodsJobs` (or a tiny separate table): PK = literal `"JobsControl"`, attribute `active_jobs` (Number). Initialised to 0. Atomically incremented by the scheduler when a job transitions to `running`, decremented by the watcher when a job reaches a terminal state. Gates §C5 scale-down — see C-5 below. **(Decision 3 = shared workers.)**
  - [ ] On-demand billing mode (matches bursty workload, no need to predict capacity).

- [ ] **Secrets Manager**
  - [ ] Secret `starepods/worker/.config` with current AWS + RDS connection block.
  - [ ] Rotation policy: 90 days, built-in RDS Postgres rotation Lambda.

- [ ] **IAM roles** (least privilege)
  - [ ] `starepods-scheduler-role`: SQS SendMessage, DynamoDB PutItem/UpdateItem on `StarePodsJobs`, ECS UpdateService on `starepods-workers`.
  - [ ] `starepods-status-role`: DynamoDB GetItem on `StarePodsJobs`/`StarePodsFailures`.
  - [ ] `starepods-completion-watcher-role`: SQS GetQueueAttributes, DynamoDB query, ECS UpdateService, EventBridge DisableRule.
  - [ ] `starepods-worker-task-role`: SQS ReceiveMessage/DeleteMessage/ChangeMessageVisibility, DynamoDB UpdateItem (conditional), S3 (read source, write to `s3://zarrpods/*`), RDS connect, Secrets Manager GetSecretValue, ECR pull.

- [ ] **Tagging**
  - [ ] Every resource carries `Project=starepods` (Budgets alarm in C-4 filters on this).

### DoD

- All resources from §C6 exist in `us-west-2`.
- ECS service can launch a `starepods-worker:1` task that reaches SQS via the VPC endpoint (no NAT).
- Worker task can fetch the `.config` secret and connect to RDS over VPC endpoints.
- Every resource shows the `Project=starepods` tag.

---

## C-4. Scheduler Lambda + API Gateway (4 days)

**Goal:** `POST /ingest` accepts a job, splits into tickets, enqueues, scales up the ECS service, returns a `job_id`. Status endpoint for polling.

**Prerequisites:** C-1 (`ticket_sizing` module) + C-3 (SQS, ECS service, DynamoDB tables).

### Steps

- [ ] **API Gateway**
  - [ ] REST API `starepods-api` in `us-west-2`.
  - [ ] Resources / methods:
    - [ ] `POST /ingest` → `starepods-scheduler`.
    - [ ] `GET  /jobs/{id}` → `starepods-status`.
    - [ ] `DELETE /jobs/{id}` → 501 Not Implemented (§C11 Decision 4 deferred to v2).
    - [ ] `GET  /jobs/{id}/failures` → `starepods-status` (alt path).
  - [ ] API key + usage plan; `x-api-key` header required.
  - [ ] Burst/rate limits per key (defence-in-depth, not cost control): e.g. 10 req/s, 1000 req/day.

- [ ] **Scheduler Lambda `starepods-scheduler`**
  - [ ] Validate request body (granule_uris, instrument, s3_prefix, options).
  - [ ] **Cost cap (§C9):** if `workers > 4` → return 400 with the standard message *"workers=N exceeds the v1 cap of 4 — please re-submit with workers=4 (or omit the parameter)."*
  - [ ] **Lambda 6 MB workaround (§C10 worth-tightening):** if request body > 5 MB, expect an S3 pointer (`s3://zarrpods/_jobs/{uuid}.json`) instead of the inline URI list; client SDK uploads first.
  - [ ] Stamp `raw_collected_time` per granule (derive from filename — same logic as C-1 §C10 #2 fix).
  - [ ] Generate `job_id` (uuid4).
  - [ ] **Fix §C10 #4 — enqueue-then-running state machine (shared-workers model):**
    1. `dynamodb.put_item(state='enqueued', total_granules=N, ...)`.
    2. Split granule URIs via `cloud.ticket_sizing.split_into_tickets`.
    3. `sqs.send_message(...)` × N tickets (each tagged with `job_id` for routing).
    4. `dynamodb.update_item(JobsControl, ADD active_jobs 1)` — atomic increment.
    5. `dynamodb.update_item(state='running')` only after all tickets enqueued and the counter is bumped.
    6. `ecs.update_service(desiredCount=max(W, currentDesired))` — never scale *down* here, only *up*; another job may already be running with a larger `W`.
  - [ ] Return 202 + `{job_id, state: 'enqueued'}`.

- [ ] **Status Lambda `starepods-status`**
  - [ ] `GET /jobs/{id}`: read DynamoDB, return current state.
  - [ ] `GET /jobs/{id}/failures`: query `StarePodsFailures` by `job_id`, paginate, return URI + error message list.

- [ ] **AWS Budgets alarm** (§C9 cost cap)
  - [ ] Budget filtered by tag `Project=starepods`.
  - [ ] Monthly threshold: initial $200 (calibrate in C-7 after measuring 1000-granule cost).
  - [ ] Alerts at 80% forecast + 100% actual → SNS topic → email.

### DoD

- `curl -X POST … /ingest` with a tiny granule list returns 202 and a `job_id`.
- `curl GET … /jobs/{id}` shows the state transition `enqueued → running`.
- Requests with `workers=8` return 400 with the standard message.
- AWS Budgets alarm exists and is wired to email.

---

## C-5. Completion watcher (3 days)

**Goal:** Every 30 s, detect jobs that have drained, tear down workers, notify callers.

**Prerequisites:** C-3, C-4. Decision 3 = **shared workers**, so teardown is conditional on the `JobsControl.active_jobs` counter (see step below).

### Steps

- [ ] **EventBridge rule** `starepods-completion-tick`
  - [ ] Schedule: `rate(30 seconds)` (or finer with EventBridge Scheduler if needed).
  - [ ] Target: `starepods-completion-watcher` Lambda.

- [ ] **Completion Watcher Lambda `starepods-completion-watcher`**
  - [ ] **Outside VPC** (§C10 #8): reaches DynamoDB and SQS via public IAM endpoints; avoids NAT cost.
  - [ ] Iterate jobs where `state='running'`:
    - [ ] DynamoDB `GetItem(job_id)` → `processed + failed == total_granules`?
      - **Note:** With shared workers, the SQS-empty sanity gate from §C5 is dropped — the queue holds other jobs' tickets, so SQS depth is not authoritative for "is this job done". The per-granule conditional write (§C10 #3) makes the per-job counter authoritative on its own.
    - [ ] If counter check passes:
      - [ ] Determine final state: `complete` if `failed == 0`, else `failed`.
      - [ ] `dynamodb.update_item(state=<final>, completed_at=now)`.
      - [ ] **Shared-workers teardown (Decision 3):**
        1. `dynamodb.update_item(JobsControl, ADD active_jobs -1)` — atomic decrement, return updated value.
        2. If `active_jobs == 0`: `ecs.update_service(desiredCount=0)` — last job done, tear down the fleet.
        3. If `active_jobs > 0`: do NOT scale down — other jobs are still running on the shared workers.
      - [ ] POST `callback_url` if provided (retry 3× with exponential backoff, fallthrough to `starepods-callbacks-dlq`).
      - [ ] Disable this job's EventBridge rule (or leave it ticking and gate on `state` — simpler).

- [ ] **CloudWatch metrics**
  - [ ] Custom metric `JobsCompleted`, `JobsFailed`, `CallbackRetries`.
  - [ ] Alarm on `StarePodsCallbackFailuresDLQ` depth > 0.

### DoD

- A small job (5 granules, 1 ticket) progresses `running → complete` within 30–60 s of the last `delete_message`.
- ECS service scales back to 0 after completion.
- Callback URL receives the POST with the documented payload shape (§C5).
- DLQ alarm fires on a deliberately failing callback URL.

---

## C-6. Client SDK (3 days)

**Goal:** `starepandas.cloud.ingest_granules(...)` returns a `JobHandle` that polls `GET /jobs/{id}`. Round-trip smoke test against the deployed service.

**Prerequisites:** C-4 + C-5 deployed.

### Steps

- [ ] **`starepandas/cloud/client.py`**
  - [ ] `ingest_granules(granule_uris, s3_prefix, instrument, workers=4, callback_url=None, block=False, options=None) -> JobHandle`.
  - [ ] If `len(granule_uris)` × avg URI size > 5 MB → upload to `s3://zarrpods/_jobs/{uuid}.json` first, send pointer (§C10 Lambda 6 MB).
  - [ ] POST to `<endpoint>/ingest` with `x-api-key`.

- [ ] **`starepandas/cloud/job_handle.py`**
  - [ ] `JobHandle.status() -> dict` — single GET.
  - [ ] `JobHandle.wait(timeout=None, poll_interval=10) -> dict` — poll until terminal state.
  - [ ] `JobHandle.cancel() -> None` — calls DELETE; in v1 raises `NotImplementedError` with a clear message (cancellation deferred per §C11 Decision 4).

- [ ] **`starepandas/cloud/config.py`**
  - [ ] Read `endpoint` and `api_key` from `.config` file (same `_load_config_from_default_locations` path used elsewhere).

- [ ] **End-to-end smoke test** (10 granules)
  - [ ] Run from a workstation outside AWS.
  - [ ] Verify Parquet partitions land in S3, RDS rows appear, callback URL receives the POST.

### DoD

- A notebook can call `sp.cloud.ingest_granules(...).wait()` and get back a `state='complete'` dict.
- All four endpoints respond (200/202/501 as designed).
- Verification scripts updated with a cloud-mode check (gated behind an env var so they don't try to hit AWS in offline runs).

---

## C-7. Load test + tune + runbook (3 days)

**Goal:** Run a 1000-granule batch end-to-end, capture metrics, tune knobs, calibrate the Budgets threshold, and document operations.

**Prerequisites:** Everything above; ~1000 representative granules staged in a test bucket (§C9).

### Steps

- [ ] **Load test**
  - [ ] Run `sp.cloud.ingest_granules(uris=[1000 URIs])` with `workers=4`.
  - [ ] Capture: total wall-time, per-granule time distribution, RDS insert latency, S3 PUT retry count, worker idle exits, cold-start time per worker.

- [ ] **Tune**
  - [ ] If P95 ticket time > visibility timeout → adjust (heartbeat or longer timeout).
  - [ ] If many idle exits before queue drains → lower `IDLE_THRESHOLD` or warm-pool.
  - [ ] If RDS insert latency dominates → batch size in `MetadataStore`.

- [ ] **Calibrate cost cap** (§C9)
  - [ ] Measured cost of 1000-granule run × 3 → set as monthly Budgets threshold.

- [ ] **Runbook** (`docs/path_c_runbook.md`)
  - [ ] How to submit a job (curl + SDK examples).
  - [ ] How to inspect job state (DynamoDB query, status endpoint).
  - [ ] How to handle stuck jobs (manual SQS purge, ECS scale-down, RDS UNIQUE conflict diagnosis).
  - [ ] How to rotate the Secrets Manager secret without breaking in-flight workers.
  - [ ] How to roll back to a previous worker image (ECR tag + ECS service update).
  - [ ] Common error → likely cause table (e.g. `RDSWriteFailures` spike → ON CONFLICT contention → probably duplicate scheduler invocations).

### DoD

- Load test report shows median per-granule time and total wall-time, written into the runbook.
- Budgets alarm threshold calibrated from real cost data.
- A second person can submit and inspect a job using only the runbook.

---

## Cross-cutting deliverables

These are touched in multiple phases but worth tracking explicitly:

- [ ] **Observability** (§C10 worth-tightening)
  - [ ] CloudWatch metrics: `TicketDuration`, `GranuleDuration` (per-instrument), `RDSInsertLatency`, `S3PUTRetries`, `WorkerIdleExits`, `CallbackRetries`.
  - [ ] All Lambda + ECS log groups: 30-day retention (not the default "forever").
  - [ ] CloudWatch dashboard: queue depth, active workers, jobs by state, P95 ticket time.

- [ ] **CI/CD**
  - [ ] GitHub Action: lint + tests + `pytest tests/` on PR.
  - [ ] GitHub Action: build + push worker image to ECR on merge to main.
  - [ ] CDK/Terraform plan + apply via OIDC role (no long-lived keys in CI).

- [x] **Verification scripts** (CLAUDE.md) — C-1 portion done
  - [x] Created `~/.claude/scripts/starepandas_verify.py` (was documented but missing); 6/6 checks pass.
  - [x] `~/.claude/scripts/starepods_verify.py` extended 7 → 10 checks with the `pods_unique` constraint assertion + live idempotency regression + C-1 unit-test pytest gate.
  - [ ] Add new `starepods_cloud_verify.py` after C-6 (gated by env var; pings status endpoint).

---

## Sequencing summary

```
C-1 ✅ DONE — landed on stare_pods_aws_parallel (commits 5e33cb6…d180a0d + this slice)
  │
  ▼
C-2 ──┐ pending
      ├──► feature branch path-c-cloud-service
C-3 ──┘                  │
                         ▼
                   C-4 ──► C-5 ──► C-6 ──► C-7 ──► merge to main
```

C-2 and C-3 can run in parallel (different people / contexts); both are unblocked now that C-1 is done and verified. The cloud worker container (C-2) imports `starepandas.ingest_granules_s3` directly via the task-7 module extraction — no further refactor needed before that work begins.
