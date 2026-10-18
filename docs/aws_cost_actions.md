# AWS cost actions — ledger

Account `637388276731`. One entry per deliberate change made to lower the AWS
bill, so that a later reader knows **what** was removed, **why**, on what
**evidence**, what it **saves**, and **how to undo it**. Newest review first.
Operational detail for the STARE-PODS stack lives in `docs/path_c_runbook.md`.

---

## Review of 2026-10-18 — after the Q1-2025 bulk ingest

**Context.** The Q1-2025 store was complete on 2026-09-07 (7,360 granules,
12,451,867 chunks, 512 GB under `s3://zarrpods/storage/`). Cost Explorer:

| Period | Account total | Notes |
|---|---|---|
| Jun–Aug 2026 (avg) | $427 / mo | pre-ingest baseline |
| Sep 2026 | $592 | ingest month: S3 requests +$77, Fargate $23, RDS up-sized 09-06 |
| Oct 2026 (to 10-18) | $16.63 / day → ≈ $515 + tax | flat since the run ended |

STARE-PODS was ≈ $170 / mo of the run rate: RDS `starepodsmetadata`
db.t4g.large ≈ $99, NAT gateway + EIP ≈ $37, S3 `zarrpods` 743 GiB ≈ $18,
RDS `starepods` micro ≈ $14, the rest (ECR, Lambda, DDB, SQS, logs) < $2.
The remaining ≈ $360 / mo belongs to other projects in the same account
(Bayesics EFS + its AWS Backup, Bayesics EC2, SageMaker Studio, a 2023 EKS VPC).

How the review was done: the `zarpodder` user is S3-only, but it may assume
the CDK bootstrap role `cdk-hnb659fds-lookup-role-637388276731-us-west-2`
(AWS `ReadOnlyAccess`), which exposes Cost Explorer and every `Describe*`.
Read-only throughout; nothing was changed during the review itself.

### Action 2 — StarePodsInfraStack: NAT gateway removed (idle posture) — DONE 2026-10-18

- **What.** `starepods:natGateways` 1 → 0 in `infra/cdk/cdk.json`; stack
  redeployed. Deleted: NAT `nat-077843af83654d376`, EIP `184.33.254.127`,
  the two private default routes. The `private` subnets converted in place to
  `PRIVATE_ISOLATED` (same subnet IDs, retagged); public subnets, S3/DDB
  gateway endpoints, ECS service (desiredCount 0) unchanged.
- **Why.** The NAT is needed only while Fargate workers run. CloudWatch
  showed 0 bytes through it in the 14 days before; the service has been at
  0 tasks since the run ended on 2026-09-07.
- **Saves.** ≈ $37 / mo ($0.045/h NAT + $0.005/h public IPv4).
- **Side effect to remember.** In the idle posture a worker task **cannot
  start** (no route to RDS, ECR, SQS, Secrets Manager). Before any ingest:
  `infra/cdk/cdk-zarpodder.sh deploy StarePodsInfraStack -c starepods:natGateways=1`
  (≈ 3 min), and back to 0 afterwards. Runbook §6i. The committed `cdk.json`
  always holds the posture that is deployed.
- **Code.** `infra/cdk/stacks/starepods_infra_stack.py` (`worker_subnet_type`
  follows `nat_gateways`; the old `int(ctx(...) or 1)` could never express 0).

### Action 3 — RDS `starepods` (db.t4g.micro) deleted — PENDING (needs admin credentials)

- **What.** RDS instance `starepods`, us-west-2, PostgreSQL 16, created
  2025-07-31, 20 GB gp2, publicly accessible, no tags, in no stack.
- **Evidence it is unused.** Connecting with the catalog's own master
  credentials shows only the default `postgres` database (7.7 MB, **zero user
  tables**) and `rdsadmin`; `pg_stat` was never reset. Nothing in `.config`,
  the repo or CloudFormation points at it. The one "connection" a day in
  CloudWatch is the 11:20 UTC backup window; ≈ 2 write IOPS is engine
  housekeeping. Created four days after `starepodsmetadata` — a second
  attempt that was never used.
- **Saves.** ≈ $14 / mo.
- **How.** `infra/scripts/aws_cost_actions_2026_10_18.py delete-rds-starepods --execute`
  with admin credentials; it takes a final snapshot `starepods-final-2026-10-18`
  first (a few MB of data, cents per month to keep).
- **Reverse.** Restore the snapshot (`rds restore-db-instance-from-db-snapshot`).

### Action 7 — CloudFormation `EKS-VPC-Stack` (us-east-2) deleted — PENDING (needs admin credentials)

- **What.** AWS sample template "Amazon EKS Sample VPC – Private and Public
  subnets", created 2023-11-01 for the eksctl cluster `EKS-Cluster`. Holds VPC
  `vpc-0bf0a15aae1483c16`, 4 subnets, an internet gateway, **two NAT gateways**
  (`nat-08887bf629a5b2e6e`, `nat-0f53b38ab10e27a85`) with two EIPs, route
  tables, one security group. Companion orphan:
  `eksctl-EKS-Cluster-addon-iamserviceaccount-kube-system-ebs-csi-controller-sa`
  (one IAM role, $0).
- **Evidence it is unused.** No EKS cluster exists in any region; the only
  EKS API event in 90 days of CloudTrail is this review's `ListClusters`. The
  VPC's only network interfaces are the two NATs' own; no EC2, RDS, EFS mount
  target or load balancer inside it. Both NATs carried 0 bytes in 30 days.
  The SageMaker Studio domain, the us-east-2 EFS and the stopped `Uploading`
  instance live in `vpc-0555c227d37ba79a9` and are not touched.
- **Saves.** ≈ $73 / mo (2 × NAT hours + 2 public IPv4).
- **How.** `infra/scripts/aws_cost_actions_2026_10_18.py delete-eks-vpc-stack --execute`
  with admin credentials (re-checks the evidence before deleting).
- **Reverse.** Re-create from the public sample template; nothing in it was
  custom.

### Reviewed on 2026-10-18 and left as is

| Item | ≈ $/mo | Finding | Decision |
|---|---|---|---|
| RDS `starepodsmetadata` db.t4g.large | 99 | idle (CPU 4 %, ≤ 1 connection); 11 GB used of 20 | open — options: 1-yr reservation (≈ −$35), resize to db.t4g.medium (≈ −$49, re-time notebook Part 6), stop between sessions |
| S3 `zarrpods/storage/` 479 GiB, 12.4 M objects | 11.5 | avg object 35 KB | keep in Standard — IA / Intelligent-Tiering minimums and monitoring would cost more than the storage |
| S3 `zarrpods/raw/` 262 GiB | 6 | raw Q1 granules, read only for a re-ingest | candidate: lifecycle to Glacier Instant Retrieval (≈ −$5) |
| ECR `starepods/worker` 11 untagged images | 0.4 | no lifecycle rule | candidate: keep last 3 |
| AWS Backup of `bayesics-efs` (1.9 TB, 35 daily points) | 95 | largest line in the account; 3× the EFS it protects | other project — cold tier or disable |
| Bayesics EC2 `StarePods` t3.large, running 24/7 | 65 | CPU 0.4 % avg over 14 days | other project — stop when not fetching data |
| RDS `bayesicspg` db.t3.micro | 15 | 0 connections in 14 days | other project |
| Stopped EC2s holding 2 EIPs, 70 GB gp2; idle EIP us-east-1 | 14 | idle-address charge | other project |
| SageMaker Studio (us-east-2), 2 spaces, no running apps | 22 | volumes only | other project |
| S3 `bayesics-archive` + `microphysics` 324 GiB Standard | 7.5 | archival | other project — Glacier |
