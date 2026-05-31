#!/usr/bin/env python3
"""CDK app entrypoint for Path C C-3 — AWS infrastructure.

Synthesises a single stack (``StarePodsInfraStack``) holding every resource
from plan §C6 that C-3 owns: VPC + endpoints, ECS cluster/service/task-def,
SQS + DLQs, the two DynamoDB tables (+ ``JobsControl`` counter item),
Secrets Manager secret, and the four IAM roles.

The stack is **environment-agnostic** so ``cdk synth`` runs offline with no
AWS credentials. Account/region are taken from the deploying creds
(``CDK_DEFAULT_ACCOUNT`` / ``CDK_DEFAULT_REGION``) at ``cdk deploy`` time.

Every resource is tagged ``Project=starepods`` (app-level) so the §C9
Budgets alarm picks it up.
"""
import os

import aws_cdk as cdk

from stacks.starepods_infra_stack import StarePodsInfraStack

app = cdk.App()

# Pin to the deploying account/region when available (deploy time), else
# stay env-agnostic so `cdk synth` works offline. We deliberately do NOT
# hardcode the account so the same app can target a sandbox account.
env = None
account = os.environ.get("CDK_DEFAULT_ACCOUNT")
region = os.environ.get("CDK_DEFAULT_REGION") or "us-west-2"
if account:
    env = cdk.Environment(account=account, region=region)

StarePodsInfraStack(
    app,
    "StarePodsInfraStack",
    env=env,
    description="Path C C-3 — STARE-PODS cloud-service base infrastructure "
    "(VPC, ECS, SQS, DynamoDB, Secrets Manager, IAM).",
)

# Project-wide tag for the §C9 Budgets alarm and cost tracking.
cdk.Tags.of(app).add("Project", "starepods")

app.synth()
