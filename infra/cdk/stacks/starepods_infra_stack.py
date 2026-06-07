"""Path C C-3 — STARE-PODS base infrastructure stack.

One CloudFormation stack holding every §C6 resource that C-3 owns. Kept as a
single stack (not split) so the whole base layer deploys and tears down with
one ``cdk deploy`` / ``cdk destroy`` and there are no cross-stack exports to
juggle for v1. Code is organised into ``_build_*`` methods, one per §C6 group.

Design notes baked in here (see docs/path_c_implementation.md §C-3):

* **Shared-workers model** (Decision 3): one SQS queue + one ECS service.
  The ``JobsControl`` counter item gates scale-down (decremented by the
  C-5 completion watcher).
* **Single NAT** (C-4 decision, 2026-06-07): workers run in
  ``PRIVATE_WITH_EGRESS`` subnets and reach the out-of-VPC ``zarrpods`` RDS
  (and all public AWS APIs) through one NAT gateway. This superseded the
  original no-NAT / 5-interface-endpoint design once a NAT became necessary
  for RDS reachability — with a NAT present the interface endpoints were
  redundant, so they were dropped (net idle ~\\$73/mo → ~\\$32/mo). The free
  S3 + DDB *gateway* endpoints are kept so bulky traffic bypasses the NAT.
  ``natGateways`` context key defaults to 1; set it to 0 only if you also
  restore the interface endpoints + isolated subnets (see ``_build_network``).
* **Visibility timeout 3600 s** (C-2 §C10 #5 Option A) — overrides the
  "15 min" figure in the §C6 table; the worker has no heartbeat, so a long
  visibility window plus the idempotent counter absorbs overruns.
* **desiredCount=0** default — the scheduler (C-4) scales the service up;
  the completion watcher (C-5) scales it back to 0.
"""
from __future__ import annotations

import os

from aws_cdk import (
    Aws,
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_apigateway as apigw,
    aws_budgets as budgets,
    aws_dynamodb as dynamodb,
    aws_ec2 as ec2,
    aws_ecr as ecr,
    aws_ecs as ecs,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_secretsmanager as secretsmanager,
    aws_sqs as sqs,
    custom_resources as cr,
)
from constructs import Construct

# Directory holding the Lambda source (common/ + scheduler/ + status/).
_LAMBDAS_DIR = os.path.join(os.path.dirname(__file__), "..", "lambdas")


class StarePodsInfraStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── Context-driven knobs (overridable via cdk.json / -c flags) ──────
        ctx = self.node.try_get_context
        self.worker_image_tag: str = ctx("starepods:workerImageTag") or "dev"
        self.nat_gateways: int = int(ctx("starepods:natGateways") or 1)
        self.vpc_cidr: str = ctx("starepods:vpcCidr") or "10.0.0.0/16"
        self.source_buckets: list[str] = ctx("starepods:sourceBuckets") or ["zarrpods"]
        self.storage_bucket: str = ctx("starepods:storageBucket") or "zarrpods"
        self.log_retention_days: int = int(ctx("starepods:logRetentionDays") or 30)
        self.task_cpu: int = int(ctx("starepods:taskCpu") or 2048)
        self.task_mem: int = int(ctx("starepods:taskMemoryMiB") or 8192)
        self.visibility_timeout_s: int = int(
            ctx("starepods:queueVisibilityTimeoutSeconds") or 3600
        )
        self.max_receive_count: int = int(ctx("starepods:queueMaxReceiveCount") or 3)
        self.ddb_ttl_days: int = int(ctx("starepods:ddbTtlDays") or 30)
        # C-4 Part B — control plane knobs.
        self.worker_cap: int = int(ctx("starepods:workerCap") or 4)        # §C9 cost cap
        self.max_ticket_size: int = int(ctx("starepods:maxTicketSize") or 40)  # §C2
        self.api_rate_limit: int = int(ctx("starepods:apiRateLimit") or 10)    # req/s
        self.api_burst_limit: int = int(ctx("starepods:apiBurstLimit") or 20)
        self.api_daily_quota: int = int(ctx("starepods:apiDailyQuota") or 1000)
        self.budget_email: str = ctx("starepods:budgetEmail") or ""
        self.budget_monthly_usd: int = int(ctx("starepods:budgetMonthlyUsd") or 200)

        # Build order matters: network → data → roles → compute → seed → api.
        self._build_network()
        self._build_messaging()
        self._build_tables()
        self._build_secret()
        self._build_iam_roles()
        self._build_compute()
        self._seed_jobs_control()
        self._build_api()
        self._build_budget()
        self._outputs()

    # ── §C6 Network ─────────────────────────────────────────────────────────
    def _build_network(self) -> None:
        """VPC with a public subnet (hosting one NAT) + private-egress subnets
        for the workers.

        RDS reachability: the existing ``zarrpods`` RDS Postgres lives outside
        this VPC and is reached over its wire protocol (not an AWS API), so it
        cannot be fronted by an interface endpoint. Workers therefore run in
        ``PRIVATE_WITH_EGRESS`` subnets and reach RDS — and every other public
        AWS API (ECR, Logs, Secrets, SQS) — through a single NAT gateway. This
        replaces the earlier no-NAT design (C-4 decision, 2026-06-07): once a
        NAT exists the 5 interface endpoints are redundant, so they were dropped
        — net idle cost ~\\$73/mo (5 interface endpoints) → ~\\$32/mo (1 NAT).
        The two *gateway* endpoints (S3, DDB) are free and kept, so bulky S3
        image-layer / Parquet I/O and DDB traffic still bypass the NAT (no
        per-GB NAT data-processing charge on those).
        """
        self.vpc = ec2.Vpc(
            self,
            "Vpc",
            vpc_name="starepods-vpc",
            ip_addresses=ec2.IpAddresses.cidr(self.vpc_cidr),
            max_azs=2,
            nat_gateways=self.nat_gateways,
            # Order matters: ``private`` is FIRST so CDK preserves the existing
            # isolated subnets' CIDRs (10.0.0.0/24, 10.0.1.0/24) when converting
            # them in place to egress. Listing ``public`` first allocated it
            # 10.0.0.0/26, which overlapped the live private /24 and failed the
            # in-place migration with a CIDR conflict (2026-06-07). With private
            # first, the new public subnets land in fresh space (10.0.2.x/26).
            subnet_configuration=[
                # Workers run here — egress to RDS + AWS APIs via the NAT.
                ec2.SubnetConfiguration(
                    name="private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
                # Public subnet hosts the NAT gateway (and the IGW route).
                ec2.SubnetConfiguration(
                    name="public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=26,
                ),
            ],
        )

        # Gateway endpoints (free) — S3 (also carries ECR image layers) + DDB.
        # Kept even with a NAT: they route this bulky traffic off the NAT,
        # avoiding the ~\\$0.045/GB NAT data-processing charge.
        self.vpc.add_gateway_endpoint(
            "S3Endpoint", service=ec2.GatewayVpcEndpointAwsService.S3
        )
        self.vpc.add_gateway_endpoint(
            "DynamoDbEndpoint", service=ec2.GatewayVpcEndpointAwsService.DYNAMODB
        )

        # Interface endpoints intentionally REMOVED (C-4 decision): with a NAT
        # in place, ECR (api+dkr), CloudWatch Logs, Secrets Manager, and SQS are
        # all reachable over the NAT, so the hourly interface-endpoint ENIs were
        # pure redundant cost (~\\$73/mo). Re-add them only if a future posture
        # requires AWS-API traffic to stay on Amazon's private network.

    # ── §C6 SQS ─────────────────────────────────────────────────────────────
    def _build_messaging(self) -> None:
        # DLQ for tickets that fail `maxReceiveCount` receives.
        self.tickets_dlq = sqs.Queue(
            self,
            "TicketsDlq",
            queue_name="starepods-tickets-dlq",
            retention_period=Duration.days(14),
        )
        # Main work queue. Visibility 3600 s per C-2 Option A.
        self.tickets_queue = sqs.Queue(
            self,
            "TicketsQueue",
            queue_name="starepods-tickets",
            visibility_timeout=Duration.seconds(self.visibility_timeout_s),
            retention_period=Duration.days(1),
            receive_message_wait_time=Duration.seconds(20),  # server-side long-poll
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=self.max_receive_count, queue=self.tickets_dlq
            ),
        )
        # DLQ for failed completion-watcher webhook POSTs (wired up in C-5).
        self.callbacks_dlq = sqs.Queue(
            self,
            "CallbacksDlq",
            queue_name="starepods-callbacks-dlq",
            retention_period=Duration.days(14),
        )

    # ── §C6 DynamoDB ──────────────────────────────────────────────────────────
    def _build_tables(self) -> None:
        # Job-state table. PK job_id. Also holds the singleton JobsControl item.
        self.jobs_table = dynamodb.Table(
            self,
            "JobsTable",
            table_name="StarePodsJobs",
            partition_key=dynamodb.Attribute(
                name="job_id", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="expires_at",
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
            removal_policy=RemovalPolicy.RETAIN,  # never auto-delete job history
        )
        # Per-granule failure detail. PK job_id, SK granule_uri.
        self.failures_table = dynamodb.Table(
            self,
            "FailuresTable",
            table_name="StarePodsFailures",
            partition_key=dynamodb.Attribute(
                name="job_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="granule_uri", type=dynamodb.AttributeType.STRING
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            time_to_live_attribute="expires_at",
            point_in_time_recovery_specification=dynamodb.PointInTimeRecoverySpecification(
                point_in_time_recovery_enabled=True
            ),
            removal_policy=RemovalPolicy.RETAIN,
        )

    # ── §C6 Secrets Manager ───────────────────────────────────────────────────
    def _build_secret(self) -> None:
        """Worker config secret.

        Created with an empty JSON template; the real value (AWS + RDS block,
        identical shape to ``starepandas/.config`` rendered as JSON) is set
        out-of-band so credentials never enter source or CloudFormation:

            aws secretsmanager put-secret-value \\
              --secret-id starepods/worker/.config \\
              --secret-string file://worker-config.json

        Rotation (§C6: 90-day RDS Postgres rotation Lambda) is deferred to
        ops/C-7; the worker already exits gracefully on rotation
        (``_is_rds_auth_error`` → Decision 9), so SQS redelivery to a fresh
        task picks up the rotated secret.
        """
        self.worker_secret = secretsmanager.Secret(
            self,
            "WorkerConfigSecret",
            secret_name="starepods/worker/.config",
            description="STARE-PODS worker .config (AWS + RDS). Value set out-of-band.",
            removal_policy=RemovalPolicy.RETAIN,
            # Placeholder template; overwritten by put-secret-value.
            secret_string_value=None,
        )

    # ── §C6 IAM roles ─────────────────────────────────────────────────────────
    def _build_iam_roles(self) -> None:
        """The four least-privilege roles from §C6.

        The three Lambda roles (scheduler/status/completion-watcher) are
        created here even though their Lambdas land in C-4/C-5 — C-3 owns the
        roles so later phases just reference them by name. The worker task role
        is attached to the ECS task definition in ``_build_compute``.
        """
        acct, region = Aws.ACCOUNT_ID, Aws.REGION
        ecs_service_arn = (
            f"arn:aws:ecs:{region}:{acct}:service/starepods/starepods-workers"
        )

        # 1. Scheduler Lambda role (C-4): enqueue tickets, write job state,
        #    scale the worker service up.
        self.scheduler_role = iam.Role(
            self,
            "SchedulerRole",
            role_name="starepods-scheduler-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )
        self.tickets_queue.grant_send_messages(self.scheduler_role)
        self.jobs_table.grant(self.scheduler_role, "dynamodb:PutItem", "dynamodb:UpdateItem")
        self.scheduler_role.add_to_policy(
            iam.PolicyStatement(
                actions=["ecs:UpdateService", "ecs:DescribeServices"],
                resources=[ecs_service_arn],
            )
        )

        # 2. Status Lambda role (C-4): read-only job + failure lookups.
        self.status_role = iam.Role(
            self,
            "StatusRole",
            role_name="starepods-status-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )
        self.jobs_table.grant_read_data(self.status_role)
        self.failures_table.grant_read_data(self.status_role)

        # 3. Completion-watcher Lambda role (C-5): detect drained jobs, scale
        #    down, disable its own EventBridge rule. Runs OUTSIDE the VPC.
        self.watcher_role = iam.Role(
            self,
            "CompletionWatcherRole",
            role_name="starepods-completion-watcher-role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSLambdaBasicExecutionRole"
                )
            ],
        )
        self.jobs_table.grant_read_write_data(self.watcher_role)
        self.failures_table.grant_read_data(self.watcher_role)
        self.callbacks_dlq.grant_send_messages(self.watcher_role)
        self.watcher_role.add_to_policy(
            iam.PolicyStatement(
                actions=["sqs:GetQueueAttributes"],
                resources=[self.tickets_queue.queue_arn],
            )
        )
        self.watcher_role.add_to_policy(
            iam.PolicyStatement(
                actions=["ecs:UpdateService", "ecs:DescribeServices"],
                resources=[ecs_service_arn],
            )
        )
        self.watcher_role.add_to_policy(
            iam.PolicyStatement(
                actions=["events:DisableRule", "events:EnableRule"],
                resources=[
                    f"arn:aws:events:{region}:{acct}:rule/starepods-completion-tick"
                ],
            )
        )

        # 4. Worker task role (ECS): the runtime identity the container assumes.
        self.worker_task_role = iam.Role(
            self,
            "WorkerTaskRole",
            role_name="starepods-worker-task-role",
            assumed_by=iam.ServicePrincipal("ecs-tasks.amazonaws.com"),
        )
        # SQS consume.
        self.tickets_queue.grant_consume_messages(self.worker_task_role)
        # DynamoDB conditional counter writes (§C10 #3).
        self.jobs_table.grant(
            self.worker_task_role, "dynamodb:UpdateItem", "dynamodb:GetItem"
        )
        self.failures_table.grant(
            self.worker_task_role, "dynamodb:PutItem", "dynamodb:UpdateItem"
        )
        # Secrets Manager (worker reads .config at startup, belt-and-braces with
        # the execution-role injection).
        self.worker_secret.grant_read(self.worker_task_role)
        # S3: read source granules + read/write the storage bucket.
        read_buckets = sorted(set(self.source_buckets) | {self.storage_bucket})
        self.worker_task_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject", "s3:ListBucket"],
                resources=[
                    arn
                    for b in read_buckets
                    for arn in (f"arn:aws:s3:::{b}", f"arn:aws:s3:::{b}/*")
                ],
            )
        )
        self.worker_task_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:PutObject", "s3:DeleteObject"],
                resources=[f"arn:aws:s3:::{self.storage_bucket}/*"],
            )
        )

    # ── §C6 ECS cluster + worker service ──────────────────────────────────────
    def _build_compute(self) -> None:
        self.cluster = ecs.Cluster(
            self,
            "Cluster",
            cluster_name="starepods",
            vpc=self.vpc,
            # off for v1 cost; enable when tuning (C-7)
            container_insights_v2=ecs.ContainerInsights.DISABLED,
        )

        log_group = logs.LogGroup(
            self,
            "WorkerLogGroup",
            log_group_name="/ecs/starepods-worker",
            retention=logs.RetentionDays.ONE_MONTH
            if self.log_retention_days == 30
            else logs.RetentionDays.TWO_WEEKS,
            removal_policy=RemovalPolicy.DESTROY,
        )

        task_def = ecs.FargateTaskDefinition(
            self,
            "WorkerTaskDef",
            family="starepods-worker",
            cpu=self.task_cpu,
            memory_limit_mib=self.task_mem,
            task_role=self.worker_task_role,
            # execution_role is auto-created; grants below extend it.
        )

        repo = ecr.Repository.from_repository_name(
            self, "WorkerRepo", "starepods/worker"
        )

        task_def.add_container(
            "worker",
            container_name="worker",
            image=ecs.ContainerImage.from_ecr_repository(repo, self.worker_image_tag),
            logging=ecs.LogDriver.aws_logs(stream_prefix="worker", log_group=log_group),
            environment={
                "SQS_QUEUE_URL": self.tickets_queue.queue_url,
                "JOBS_TABLE_NAME": self.jobs_table.table_name,
                "FAILURES_TABLE_NAME": self.failures_table.table_name,
                "AWS_REGION": Aws.REGION,
                "IDLE_THRESHOLD": "3",
                "POLL_WAIT_SECONDS": "20",
                "WORK_DIR": "/tmp/starepods-worker",
                "STAREPANDAS_AWS_CONFIG": "/etc/starepods/.config",
            },
            # ECS injects the secret as an env var (native `secrets` block maps
            # to env, not a file). The worker image needs a tiny entrypoint shim
            # to materialise $STAREPANDAS_WORKER_SECRET → /etc/starepods/.config
            # before launch. Tracked as the C-3 "secret materialisation" item;
            # see docs/path_c_implementation.md §C-3.
            secrets={
                "STAREPANDAS_WORKER_SECRET": ecs.Secret.from_secrets_manager(
                    self.worker_secret
                ),
            },
        )

        self.worker_service = ecs.FargateService(
            self,
            "WorkerService",
            service_name="starepods-workers",
            cluster=self.cluster,
            task_definition=task_def,
            desired_count=0,  # scheduler scales up; watcher scales back to 0
            assign_public_ip=False,
            vpc_subnets=ec2.SubnetSelection(
                # Egress subnets: workers reach RDS + AWS APIs via the NAT.
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ),
            min_healthy_percent=0,  # allow full scale-to-zero
            max_healthy_percent=100,
            # Fail a bad rollout fast instead of waiting ~3h (workers that
            # crash-loop on a broken image should surface immediately).
            circuit_breaker=ecs.DeploymentCircuitBreaker(rollback=True),
        )

    # ── JobsControl singleton seed (Decision 3 scale-down gate) ────────────────
    def _seed_jobs_control(self) -> None:
        """Idempotently seed the ``JobsControl`` item with ``active_jobs=0``.

        CloudFormation has no native "DynamoDB item" resource, so this uses an
        AwsCustomResource PutItem guarded by ``attribute_not_exists(job_id)`` so
        re-deploys never clobber a live counter.
        """
        cr.AwsCustomResource(
            self,
            "SeedJobsControl",
            on_create=cr.AwsSdkCall(
                service="DynamoDB",
                action="putItem",
                parameters={
                    "TableName": self.jobs_table.table_name,
                    "Item": {
                        "job_id": {"S": "JobsControl"},
                        "active_jobs": {"N": "0"},
                    },
                    "ConditionExpression": "attribute_not_exists(job_id)",
                },
                physical_resource_id=cr.PhysicalResourceId.of("JobsControl"),
                # A conditional-check failure means the item already exists —
                # treat as success (idempotent seed).
                ignore_error_codes_matching="ConditionalCheckFailedException",
            ),
            policy=cr.AwsCustomResourcePolicy.from_statements(
                [
                    iam.PolicyStatement(
                        actions=["dynamodb:PutItem"],
                        resources=[self.jobs_table.table_arn],
                    )
                ]
            ),
            install_latest_aws_sdk=False,
        )

    # ── §C4 Part B — Scheduler + Status Lambdas + API Gateway ─────────────────
    def _build_api(self) -> None:
        """The C-4 control plane: two Lambdas behind a REST API.

        Both Lambdas ship from one asset (``infra/cdk/lambdas`` — common/ +
        scheduler/ + status/); the handler string selects which entrypoint
        runs. The functions reuse the §C6 scheduler/status roles created in
        ``_build_iam_roles``. Only added grant: the scheduler needs
        ``s3:GetObject`` on the ``_jobs/`` prefix for the >5 MB granule-list
        pointer escape hatch (§C10 Lambda 6 MB workaround).
        """
        # Extra grant for the scheduler's granule_uris_s3 pointer.
        self.scheduler_role.add_to_policy(
            iam.PolicyStatement(
                actions=["s3:GetObject"],
                resources=[f"arn:aws:s3:::{self.storage_bucket}/_jobs/*"],
            )
        )

        code = lambda_.Code.from_asset(_LAMBDAS_DIR)

        def _log_group(cid: str, fn_name: str) -> logs.LogGroup:
            return logs.LogGroup(
                self, cid,
                log_group_name=f"/aws/lambda/{fn_name}",
                retention=logs.RetentionDays.ONE_MONTH,  # cross-cutting: 30-day
                removal_policy=RemovalPolicy.DESTROY,
            )

        self.scheduler_fn = lambda_.Function(
            self, "SchedulerFn",
            function_name="starepods-scheduler",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="scheduler.handler.handler",
            code=code,
            role=self.scheduler_role,
            timeout=Duration.seconds(30),
            memory_size=256,
            log_group=_log_group("SchedulerLogs", "starepods-scheduler"),
            environment={
                "TICKETS_QUEUE_URL": self.tickets_queue.queue_url,
                "JOBS_TABLE_NAME": self.jobs_table.table_name,
                "ECS_CLUSTER": self.cluster.cluster_name,
                "ECS_SERVICE": self.worker_service.service_name,
                "WORKER_CAP": str(self.worker_cap),
                "MAX_TICKET_SIZE": str(self.max_ticket_size),
                "TTL_DAYS": str(self.ddb_ttl_days),
            },
        )

        self.status_fn = lambda_.Function(
            self, "StatusFn",
            function_name="starepods-status",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="status.handler.handler",
            code=code,
            role=self.status_role,
            timeout=Duration.seconds(15),
            memory_size=128,
            log_group=_log_group("StatusLogs", "starepods-status"),
            environment={
                "JOBS_TABLE_NAME": self.jobs_table.table_name,
                "FAILURES_TABLE_NAME": self.failures_table.table_name,
            },
        )

        # REST API with a single 'v1' stage; per-key throttling is the
        # defence-in-depth knob (not a cost control — §C9).
        self.api = apigw.RestApi(
            self, "Api",
            rest_api_name="starepods-api",
            description="STARE-PODS cloud ingest control plane (C-4).",
            deploy_options=apigw.StageOptions(stage_name="v1"),
            cloud_watch_role=False,
        )

        sched_int = apigw.LambdaIntegration(self.scheduler_fn)
        status_int = apigw.LambdaIntegration(self.status_fn)

        # POST /ingest → scheduler
        ingest = self.api.root.add_resource("ingest")
        ingest.add_method("POST", sched_int, api_key_required=True)

        # /jobs/{id} → status (GET) + 501 (DELETE); /jobs/{id}/failures → status
        jobs = self.api.root.add_resource("jobs")
        job = jobs.add_resource("{id}")
        job.add_method("GET", status_int, api_key_required=True)
        job.add_method("DELETE", status_int, api_key_required=True)  # handler returns 501
        failures = job.add_resource("failures")
        failures.add_method("GET", status_int, api_key_required=True)

        # API key + usage plan (x-api-key required on every method).
        self.api_key = self.api.add_api_key(
            "ApiKey", api_key_name="starepods-default-key"
        )
        plan = self.api.add_usage_plan(
            "UsagePlan",
            name="starepods-usage-plan",
            throttle=apigw.ThrottleSettings(
                rate_limit=self.api_rate_limit,
                burst_limit=self.api_burst_limit,
            ),
            quota=apigw.QuotaSettings(
                limit=self.api_daily_quota, period=apigw.Period.DAY
            ),
        )
        plan.add_api_key(self.api_key)
        plan.add_api_stage(stage=self.api.deployment_stage)

    # ── §C9 Budgets cost alarm ────────────────────────────────────────────────
    def _build_budget(self) -> None:
        """Monthly cost budget scoped to ``Project=starepods`` (§C9).

        Filters spend by the ``Project`` cost-allocation tag, so it tracks only
        the resources this stack creates+tags (NAT, Fargate, Lambda, SQS, DDB,
        Secrets, logs, API Gateway) — NOT the pre-existing, untagged ``zarrpods``
        S3 bucket or the external RDS. That's the right thing to watch: those
        were already paid for; the new runaway risk is workers that don't scale
        back down, which IS tagged.

        IMPORTANT: tag-filtered budgets only populate once the ``Project``
        cost-allocation tag is ACTIVATED in Billing → Cost allocation tags
        (account-level, ~24h lag, not retroactive). Until then this budget
        reads $0. Notifications are delivered straight to email (no SNS) at
        80% forecast + 100% actual.
        """
        if not self.budget_email:
            return  # no email configured → skip (e.g. offline synth in CI)

        def _notify(ntype: str, threshold: int):
            return budgets.CfnBudget.NotificationWithSubscribersProperty(
                notification=budgets.CfnBudget.NotificationProperty(
                    notification_type=ntype,            # ACTUAL | FORECASTED
                    comparison_operator="GREATER_THAN",
                    threshold=threshold,
                    threshold_type="PERCENTAGE",
                ),
                subscribers=[
                    budgets.CfnBudget.SubscriberProperty(
                        subscription_type="EMAIL", address=self.budget_email
                    )
                ],
            )

        budgets.CfnBudget(
            self, "MonthlyBudget",
            budget=budgets.CfnBudget.BudgetDataProperty(
                budget_name="starepods-monthly",
                budget_type="COST",
                time_unit="MONTHLY",
                budget_limit=budgets.CfnBudget.SpendProperty(
                    amount=self.budget_monthly_usd, unit="USD"
                ),
                # Format: "user:<TagKey>$<TagValue>".
                cost_filters={"TagKeyValue": ["user:Project$starepods"]},
            ),
            notifications_with_subscribers=[
                _notify("FORECASTED", 80),   # heads-up before we get there
                _notify("ACTUAL", 100),      # we've actually hit the cap
            ],
        )

    # ── Stack outputs (consumed by C-4/C-5/C-6 + the runbook) ─────────────────
    def _outputs(self) -> None:
        CfnOutput(self, "VpcId", value=self.vpc.vpc_id)
        CfnOutput(self, "ClusterName", value=self.cluster.cluster_name)
        CfnOutput(self, "WorkerServiceName", value=self.worker_service.service_name)
        CfnOutput(self, "TicketsQueueUrl", value=self.tickets_queue.queue_url)
        CfnOutput(self, "TicketsDlqUrl", value=self.tickets_dlq.queue_url)
        CfnOutput(self, "CallbacksDlqUrl", value=self.callbacks_dlq.queue_url)
        CfnOutput(self, "JobsTableName", value=self.jobs_table.table_name)
        CfnOutput(self, "FailuresTableName", value=self.failures_table.table_name)
        CfnOutput(self, "WorkerSecretArn", value=self.worker_secret.secret_arn)
        CfnOutput(self, "SchedulerRoleArn", value=self.scheduler_role.role_arn)
        CfnOutput(self, "StatusRoleArn", value=self.status_role.role_arn)
        CfnOutput(self, "CompletionWatcherRoleArn", value=self.watcher_role.role_arn)
        CfnOutput(self, "WorkerTaskRoleArn", value=self.worker_task_role.role_arn)
        # C-4 Part B control plane.
        CfnOutput(self, "ApiEndpoint", value=self.api.url)
        CfnOutput(self, "ApiKeyId", value=self.api_key.key_id,
                  description="Fetch the value with: aws apigateway get-api-key "
                              "--api-key <id> --include-value --query value")
        CfnOutput(self, "SchedulerFnName", value=self.scheduler_fn.function_name)
        CfnOutput(self, "StatusFnName", value=self.status_fn.function_name)
