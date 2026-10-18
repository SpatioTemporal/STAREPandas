"""Admin-side steps of the 2026-10-18 cost review (docs/aws_cost_actions.md).

Two deletions outside the project's own stack. ``zarpodder`` may run them while
the temporary managed policy ``infra/iam/zarpodder-cost-actions-2026-10-18.json``
is attached (detach it afterwards); admin credentials work too:

    delete-rds-starepods    RDS instance ``starepods`` (us-west-2) — empty,
                            unused since creation; final snapshot taken first.
    delete-eks-vpc-stack    CloudFormation ``EKS-VPC-Stack`` (us-east-2) — the
                            VPC + 2 NAT gateways of a long-deleted EKS cluster,
                            plus the orphaned eksctl IAM-role stack.

Credentials come from the normal boto3 chain (``AWS_PROFILE`` or the
``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` env vars). Every action is a
**dry run** unless ``--execute`` is given; each re-checks the evidence from the
review before deleting and refuses if anything looks different.

    AWS_PROFILE=<admin> conda run -n starepandas_3.12_v3 \
        python infra/scripts/aws_cost_actions_2026_10_18.py delete-rds-starepods
    AWS_PROFILE=<admin> conda run -n starepandas_3.12_v3 \
        python infra/scripts/aws_cost_actions_2026_10_18.py delete-rds-starepods --execute
"""
from __future__ import annotations

import argparse
import sys

import boto3
from botocore.exceptions import ClientError

ACCOUNT = "637388276731"
RDS_ID = "starepods"
RDS_REGION = "us-west-2"
FINAL_SNAPSHOT = "starepods-final-2026-10-18"
EKS_REGION = "us-east-2"
EKS_VPC_STACK = "EKS-VPC-Stack"
EKS_VPC_ID = "vpc-0bf0a15aae1483c16"
EKS_ROLE_STACK = (
    "eksctl-EKS-Cluster-addon-iamserviceaccount-kube-system-ebs-csi-controller-sa"
)


def _whoami(session: boto3.Session) -> str:
    ident = session.client("sts").get_caller_identity()
    if ident["Account"] != ACCOUNT:
        sys.exit(f"wrong account {ident['Account']} (expected {ACCOUNT})")
    print(f"acting as {ident['Arn']}")
    return ident["Arn"]


def delete_rds_starepods(session: boto3.Session, execute: bool) -> None:
    rds = session.client("rds", region_name=RDS_REGION)
    try:
        inst = rds.describe_db_instances(DBInstanceIdentifier=RDS_ID)["DBInstances"][0]
    except ClientError as e:
        if e.response["Error"]["Code"] == "DBInstanceNotFound":
            print(f"RDS instance {RDS_ID!r} does not exist — nothing to do")
            return
        raise
    created = inst["InstanceCreateTime"].date().isoformat()
    print(
        f"RDS {RDS_ID}: {inst['DBInstanceClass']} {inst['Engine']} "
        f"{inst['EngineVersion']} created {created} status={inst['DBInstanceStatus']} "
        f"storage={inst['AllocatedStorage']} GB replicas={inst.get('ReadReplicaDBInstanceIdentifiers')}"
    )
    # Evidence from the review — refuse if the instance is not the one examined.
    assert inst["DBInstanceClass"] == "db.t4g.micro", "class changed since the review"
    assert created == "2025-07-31", "creation date differs from the reviewed instance"
    assert not inst.get("ReadReplicaDBInstanceIdentifiers"), "has read replicas"
    assert not inst.get("DeletionProtection"), "deletion protection is on — review first"
    if inst["DBInstanceStatus"] != "available":
        sys.exit(f"status is {inst['DBInstanceStatus']!r}; wait until 'available'")
    if not execute:
        print(f"DRY RUN — would delete {RDS_ID} after final snapshot {FINAL_SNAPSHOT}")
        return
    rds.delete_db_instance(
        DBInstanceIdentifier=RDS_ID,
        SkipFinalSnapshot=False,
        FinalDBSnapshotIdentifier=FINAL_SNAPSHOT,
        DeleteAutomatedBackups=True,
    )
    print(f"delete requested; waiting for final snapshot {FINAL_SNAPSHOT} …")
    rds.get_waiter("db_snapshot_available").wait(
        DBSnapshotIdentifier=FINAL_SNAPSHOT, WaiterConfig={"Delay": 30, "MaxAttempts": 40}
    )
    print("snapshot available; waiting for the instance to disappear …")
    rds.get_waiter("db_instance_deleted").wait(
        DBInstanceIdentifier=RDS_ID, WaiterConfig={"Delay": 30, "MaxAttempts": 40}
    )
    print(f"DONE — {RDS_ID} deleted, snapshot {FINAL_SNAPSHOT} kept")


def delete_eks_vpc_stack(session: boto3.Session, execute: bool) -> None:
    cfn = session.client("cloudformation", region_name=EKS_REGION)
    ec2 = session.client("ec2", region_name=EKS_REGION)
    eks = session.client("eks", region_name=EKS_REGION)
    stacks = {
        s["StackName"]: s["StackStatus"]
        for s in cfn.list_stacks(
            StackStatusFilter=["CREATE_COMPLETE", "UPDATE_COMPLETE", "DELETE_FAILED"]
        )["StackSummaries"]
    }
    if EKS_VPC_STACK not in stacks and EKS_ROLE_STACK not in stacks:
        print("neither stack exists — nothing to do")
        return
    # Evidence from the review — refuse if anything moved into the VPC since.
    clusters = eks.list_clusters()["clusters"]
    assert not clusters, f"EKS clusters exist in {EKS_REGION}: {clusters}"
    enis = ec2.describe_network_interfaces(
        Filters=[{"Name": "vpc-id", "Values": [EKS_VPC_ID]}]
    )["NetworkInterfaces"]
    foreign = [n for n in enis if n["InterfaceType"] != "nat_gateway"]
    assert not foreign, f"non-NAT network interfaces in {EKS_VPC_ID}: " + str(
        [(n["NetworkInterfaceId"], n.get("Description")) for n in foreign]
    )
    instances = [
        i["InstanceId"]
        for r in ec2.describe_instances(Filters=[{"Name": "vpc-id", "Values": [EKS_VPC_ID]}])[
            "Reservations"
        ]
        for i in r["Instances"]
    ]
    assert not instances, f"EC2 instances in {EKS_VPC_ID}: {instances}"
    nats = [
        n["NatGatewayId"]
        for n in ec2.describe_nat_gateways(
            Filters=[{"Name": "vpc-id", "Values": [EKS_VPC_ID]}, {"Name": "state", "Values": ["available"]}]
        )["NatGateways"]
    ]
    print(f"{EKS_VPC_STACK}: status={stacks.get(EKS_VPC_STACK)} vpc={EKS_VPC_ID} NATs={nats} other ENIs=0 instances=0")
    print(f"{EKS_ROLE_STACK}: status={stacks.get(EKS_ROLE_STACK)} (one IAM role)")
    if not execute:
        print("DRY RUN — would delete both stacks")
        return
    for name in (EKS_VPC_STACK, EKS_ROLE_STACK):
        if name not in stacks:
            continue
        cfn.delete_stack(StackName=name)
        print(f"delete requested for {name}; waiting …")
        cfn.get_waiter("stack_delete_complete").wait(
            StackName=name, WaiterConfig={"Delay": 20, "MaxAttempts": 60}
        )
        print(f"DONE — {name} deleted")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("action", choices=["delete-rds-starepods", "delete-eks-vpc-stack"])
    p.add_argument("--execute", action="store_true", help="actually delete (default: dry run)")
    args = p.parse_args()
    session = boto3.Session()
    _whoami(session)  # any principal holding infra/iam/zarpodder-cost-actions-2026-10-18.json
    {"delete-rds-starepods": delete_rds_starepods, "delete-eks-vpc-stack": delete_eks_vpc_stack}[
        args.action
    ](session, args.execute)


if __name__ == "__main__":
    main()
