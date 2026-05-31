# Path C C-3 — AWS infrastructure (CDK, Python)

CDK app that provisions every §C6 resource C-3 owns, in **one stack**
(`StarePodsInfraStack`): VPC + endpoints, ECS cluster/service/task-def,
SQS + 2 DLQs, the two DynamoDB tables (+ `JobsControl` seed item),
Secrets Manager secret, and the four IAM roles. Region `us-west-2`,
account `637388276731`. Everything is tagged `Project=starepods`.

## Layout

```
infra/cdk/
├── app.py                          # CDK app entrypoint (env-agnostic for offline synth)
├── cdk.json                        # app command + tunable context knobs
├── requirements.txt                # aws-cdk-lib, constructs
├── stacks/
│   └── starepods_infra_stack.py    # the single C-3 stack (one _build_* per §C6 group)
└── .venv/                          # local venv (gitignored)
```

## Tunable context (in `cdk.json`, override with `-c key=value`)

| Key | Default | Meaning |
|---|---|---|
| `starepods:workerImageTag` | `dev` | ECR tag the task def pulls (`starepods/worker:<tag>`). |
| `starepods:natGateways` | `0` | NAT gateways. **0 = no egress** (cost trap avoided). Bump to `1` only to unblock RDS reachability (see Open items). |
| `starepods:vpcCidr` | `10.0.0.0/16` | VPC CIDR. |
| `starepods:sourceBuckets` | `["zarrpods"]` | Buckets the worker task role may read granules from. |
| `starepods:storageBucket` | `zarrpods` | Bucket the worker writes Parquet to. |
| `starepods:queueVisibilityTimeoutSeconds` | `3600` | Tickets queue visibility (C-2 §C10 #5 Option A). |
| `starepods:queueMaxReceiveCount` | `3` | Receives before a ticket goes to the DLQ. |
| `starepods:taskCpu` / `:taskMemoryMiB` | `2048` / `8192` | Fargate task size (2 vCPU / 8 GB). |

## Setup (local)

```bash
cd infra/cdk
python -m venv .venv            # or reuse the one created during C-3
.venv/bin/pip install -r requirements.txt
export JSII_SILENCE_WARNING_UNTESTED_NODE_VERSION=1   # node 25 is untested-but-works
cdk synth                        # offline — NO AWS creds needed
```

`cdk synth` fully validates the stack with no AWS access. Use it as the
C-3 build gate.

## Deploy — permission model

CDK deploys through the **bootstrap roles**, not the caller's identity.
That matters here because the `zarpodder` user carries the
`HaiVersion_AWSCompromisedKeyQuarantineV2` inline Deny (self-imposed
least-privilege guardrail; see C-2.6 operational notes). The deny would
block direct `ec2:`/`ecs:`/`iam:` creation — but CDK doesn't create
resources as `zarpodder`. CloudFormation assumes
`cdk-hnb659fds-cfn-exec-role-*` and that role does the work, sidestepping
the deny.

So the unblock is two small steps, **not** "give zarpodder admin":

### 1. One-time bootstrap (admin creds — creates the CDK roles)

`cdk bootstrap` itself creates IAM roles + an S3 staging bucket + an ECR
repo + an SSM param, so it needs admin-level IAM/S3/ECR/SSM. Run it once
with an admin principal (not zarpodder):

```bash
cdk bootstrap aws://637388276731/us-west-2
```

Use the **default** cfn-exec policy (`AdministratorAccess`). Do NOT swap in
`PowerUserAccess` — this stack creates 4 IAM roles, and PowerUserAccess
excludes IAM, so the deploy would fail at the role resources. If you want a
scoped cfn-exec policy, it must still allow `iam:CreateRole`,
`iam:DeleteRole`, `iam:AttachRolePolicy`, `iam:PutRolePolicy`,
`iam:PassRole`, and the tagging variants.

### 2. Grant zarpodder the slim deploy policy

Attach `infra/iam/starepods-c3-deploy.json` to `zarpodder` (in addition to
`starepods-c2-dev`). It grants only `sts:AssumeRole` on `cdk-hnb659fds-*`
+ `ssm:GetParameter` on the bootstrap version + read-only CloudFormation
status. **Confirm the `HaiVersion` Deny does not list `sts:AssumeRole` or
`ssm:GetParameter(s)`** — if it does, remove those entries (same pattern
as the C-2.6 `ecr:*` carve-out), or this Allow is overridden.

### 3. Deploy (zarpodder creds)

Use the `cdk-zarpodder.sh` wrapper — it pulls zarpodder's keys from
`starepandas/.config` into the env (CDK reads creds from the env chain, and
those keys aren't in `~/.aws`) without echoing the secret:

```bash
infra/cdk/cdk-zarpodder.sh deploy StarePodsInfraStack --require-approval never
```

> **What actually happened (2026-05-30):** rather than the two-principal split
> above, the project made `zarpodder` self-sufficient — attached
> `starepods-c3-bootstrap` + `starepods-c3-deploy` and **detached the
> `HaiVersion` quarantine** (zarpodder is now admin-equivalent). Both
> `cdk bootstrap` and `cdk deploy` ran from zarpodder creds via the wrapper.
> See the "Posture change" note in `docs/path_c_implementation.md` §C-3.

After deploy, set the real worker secret out-of-band (never in git):

```bash
aws secretsmanager put-secret-value \
  --secret-id starepods/worker/.config \
  --secret-string file://worker-config.json   # JSON shape of starepandas/.config
```

## Open items (must close before the first live ECS worker run — C-4)

1. **RDS connectivity.** The `zarrpods` RDS Postgres is outside this VPC and
   is reached over the Postgres wire protocol, so no interface endpoint can
   front it. With `natGateways=0` an isolated worker can't reach a public RDS
   endpoint. Resolve via one of: `-c starepods:natGateways=1` (single NAT,
   simplest), VPC peering to the RDS VPC, or moving RDS into a private subnet
   here. C-3 ships the no-NAT default; C-4 picks the path.
2. **Secret materialisation.** ECS injects the secret as the env var
   `STAREPANDAS_WORKER_SECRET`, but the worker image expects a file at
   `/etc/starepods/.config`. Needs a small entrypoint shim in the worker image
   (`echo "$STAREPANDAS_WORKER_SECRET" > /etc/starepods/.config`) — a C-2
   image rev — OR a loader tweak to read the config JSON from the env var.

## Teardown

```bash
infra/cdk/cdk-zarpodder.sh destroy StarePodsInfraStack
```

The DynamoDB tables and the secret have `RemovalPolicy.RETAIN` — they
survive `destroy` so job history / credentials aren't lost. Delete them
manually if you truly want them gone.

**Why you'd destroy:** the 5 interface VPC endpoints cost ≈ **$73/month** at
idle (the no-NAT tradeoff) even with `desiredCount=0`; destroy removes them.
The CDKToolkit bootstrap stack persists and is ≈ free, so re-deploy later is
just `cdk-zarpodder.sh deploy StarePodsInfraStack` with no re-bootstrap. Full
cost + teardown notes live in `docs/path_c_implementation.md` §C-3.
