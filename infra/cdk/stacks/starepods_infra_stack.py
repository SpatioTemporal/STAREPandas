"""Path C C-3 — STARE-PODS base infrastructure stack.

One CloudFormation stack holding every §C6 resource that C-3 owns. Kept as a
single stack (not split) so the whole base layer deploys and tears down with
one ``cdk deploy`` / ``cdk destroy`` and there are no cross-stack exports to
juggle for v1. Code is organised into ``_build_*`` methods, one per §C6 group.

Design notes baked in here (see docs/path_c_implementation.md §C-3):

* **Shared-workers model** (Decision 3): one SQS queue + one ECS service.
  The ``JobsControl`` counter item gates scale-down (decremented by the
  C-5 completion watcher).
* **No NAT** (§C10 cost trap): workers run in PRIVATE_ISOLATED subnets and
  reach AWS services through VPC endpoints only. ``natGateways`` context key
  defaults to 0; bump it only to unblock RDS reachability (see the RDS note
  on ``_build_network``).
* **Visibility timeout 3600 s** (C-2 §C10 #5 Option A) — overrides the
  "15 min" figure in the §C6 table; the worker has no heartbeat, so a long
  visibility window plus the idempotent counter absorbs overruns.
* **desiredCount=0** default — the scheduler (C-4) scales the service up;
  the completion watcher (C-5) scales it back to 0.
"""
from __future__ import annotations

from aws_cdk import (
    Aws,
    CfnOutput,
    Duration,
    RemovalPolicy,
    Stack,
    aws_dynamodb as dynamodb,
    aws_ec2 as ec2,
    aws_ecr as ecr,
    aws_ecs as ecs,
    aws_iam as iam,
    aws_logs as logs,
    aws_secretsmanager as secretsmanager,
    aws_sqs as sqs,
    custom_resources as cr,
)
from constructs import Construct


class StarePodsInfraStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ── Context-driven knobs (overridable via cdk.json / -c flags) ──────
        ctx = self.node.try_get_context
        self.worker_image_tag: str = ctx("starepods:workerImageTag") or "dev"
        self.nat_gateways: int = int(ctx("starepods:natGateways") or 0)
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

        # Build order matters: network → data → roles → compute → seed.
        self._build_network()
        self._build_messaging()
        self._build_tables()
        self._build_secret()
        self._build_iam_roles()
        self._build_compute()
        self._seed_jobs_control()
        self._outputs()

    # ── §C6 Network ─────────────────────────────────────────────────────────
    def _build_network(self) -> None:
        """VPC with two PRIVATE_ISOLATED subnets and the endpoints workers need.

        RDS reachability caveat: the existing ``zarrpods`` RDS Postgres lives
        outside this VPC and is reached over its wire protocol (not an AWS API),
        so it cannot be fronted by an interface endpoint. With ``natGateways=0``
        an isolated worker cannot reach a public RDS endpoint. Resolve before
        the first live ECS run by ONE of: (a) set ``-c starepods:natGateways=1``
        (single NAT, ~\\$32/mo — simplest), (b) VPC-peer this VPC to the RDS VPC,
        or (c) move the RDS into a private subnet of this VPC. Tracked as the
        C-3 "RDS connectivity" open item.
        """
        self.vpc = ec2.Vpc(
            self,
            "Vpc",
            vpc_name="starepods-vpc",
            ip_addresses=ec2.IpAddresses.cidr(self.vpc_cidr),
            max_azs=2,
            nat_gateways=self.nat_gateways,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="private",
                    subnet_type=ec2.SubnetType.PRIVATE_ISOLATED,
                    cidr_mask=24,
                ),
            ],
        )

        # Gateway endpoints (free) — S3 (also carries ECR image layers) + DDB.
        self.vpc.add_gateway_endpoint(
            "S3Endpoint", service=ec2.GatewayVpcEndpointAwsService.S3
        )
        self.vpc.add_gateway_endpoint(
            "DynamoDbEndpoint", service=ec2.GatewayVpcEndpointAwsService.DYNAMODB
        )

        # Interface endpoints (hourly + data) — required for image pull, secret
        # injection, logs, and SQS from no-NAT isolated subnets.
        interface_services = {
            "EcrApiEndpoint": ec2.InterfaceVpcEndpointAwsService.ECR,
            "EcrDockerEndpoint": ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER,
            "LogsEndpoint": ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS,
            "SecretsManagerEndpoint": ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
            "SqsEndpoint": ec2.InterfaceVpcEndpointAwsService.SQS,
        }
        for cid, svc in interface_services.items():
            self.vpc.add_interface_endpoint(cid, service=svc, private_dns_enabled=True)

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
                subnet_type=ec2.SubnetType.PRIVATE_ISOLATED
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
