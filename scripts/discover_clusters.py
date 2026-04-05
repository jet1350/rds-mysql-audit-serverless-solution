#!/usr/bin/env python3
"""
Discover Aurora MySQL clusters and/or RDS MySQL instances with audit logging.

Scans the current AWS region, checks audit logging status, and generates
a cluster_config.json file suitable for the Dispatcher Lambda.

Usage:
    python discover_clusters.py [--region REGION] [--output PATH] [--upload S3_URI]
                                [--target-rds-type TYPE]

Examples:
    python discover_clusters.py
    python discover_clusters.py --target-rds-type both
    python discover_clusters.py --target-rds-type aurora-mysql --region us-west-2
    python discover_clusters.py --upload s3://my-bucket/config/cluster_config.json
"""

import argparse
import json
import sys
from datetime import datetime, timezone

VALID_RDS_TYPES = {"aurora-mysql", "rds-mysql", "both"}


def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="Discover Aurora MySQL clusters and RDS MySQL instances with audit logging."
    )
    parser.add_argument("--region", default=None, help="AWS region")
    parser.add_argument("--output", default="cluster_config.json", help="Output file path")
    parser.add_argument("--upload", default=None, help="S3 URI (s3://bucket/key)")
    parser.add_argument(
        "--target-rds-type", default="aurora-mysql",
        choices=sorted(VALID_RDS_TYPES),
        help="Target type: aurora-mysql, rds-mysql, or both (default: aurora-mysql)",
    )
    return parser.parse_args(argv)


def create_boto3_session(region=None):
    import boto3
    kwargs = {}
    if region:
        kwargs["region_name"] = region
    return boto3.Session(**kwargs)


# ── Aurora MySQL ──────────────────────────────────────────────────────

def list_aurora_mysql_clusters(rds_client):
    clusters = []
    paginator = rds_client.get_paginator("describe_db_clusters")
    for page in paginator.paginate():
        for cluster in page.get("DBClusters", []):
            if cluster.get("Engine") == "aurora-mysql":
                clusters.append(cluster)
    return clusters


def is_cluster_audit_enabled(rds_client, parameter_group_name):
    paginator = rds_client.get_paginator("describe_db_cluster_parameters")
    for page in paginator.paginate(DBClusterParameterGroupName=parameter_group_name):
        for param in page.get("Parameters", []):
            if param.get("ParameterName") == "server_audit_logging":
                value = param.get("ParameterValue", "")
                return value.upper() == "ON" or value == "1"
    return False


def discover_aurora_clusters(rds_client):
    all_clusters = list_aurora_mysql_clusters(rds_client)
    results = []
    for cluster in all_clusters:
        cluster_id = cluster["DBClusterIdentifier"]
        param_group = cluster.get("DBClusterParameterGroup", "")
        try:
            audit_on = is_cluster_audit_enabled(rds_client, param_group)
        except Exception as exc:
            print(f"  WARNING: Failed to check cluster '{cluster_id}': {exc}")
            continue
        if not audit_on:
            continue
        instance_ids = [m["DBInstanceIdentifier"] for m in cluster.get("DBClusterMembers", [])]
        results.append({
            "cluster_id": cluster_id, "instance_ids": instance_ids,
            "parameter_group": param_group, "enabled": True, "type": "aurora-mysql",
        })
    return results


# ── RDS MySQL ─────────────────────────────────────────────────────────

def list_rds_mysql_instances(rds_client):
    instances = []
    paginator = rds_client.get_paginator("describe_db_instances")
    for page in paginator.paginate():
        for inst in page.get("DBInstances", []):
            if inst.get("Engine") == "mysql" and not inst.get("DBClusterIdentifier"):
                instances.append(inst)
    return instances


def is_instance_audit_enabled(rds_client, option_group_name):
    try:
        response = rds_client.describe_option_groups(OptionGroupName=option_group_name)
        for og in response.get("OptionGroupsList", []):
            for option in og.get("Options", []):
                if option.get("OptionName") == "MARIADB_AUDIT_PLUGIN":
                    return True
    except Exception as exc:
        print(f"  WARNING: Failed to check option group '{option_group_name}': {exc}")
    return False


def discover_rds_mysql_instances(rds_client):
    all_instances = list_rds_mysql_instances(rds_client)
    results = []
    for inst in all_instances:
        instance_id = inst["DBInstanceIdentifier"]
        audit_on = False
        for og in inst.get("OptionGroupMemberships", []):
            og_name = og.get("OptionGroupName", "")
            if og_name and is_instance_audit_enabled(rds_client, og_name):
                audit_on = True
                break
        if not audit_on:
            continue
        results.append({
            "cluster_id": instance_id, "instance_ids": [instance_id],
            "parameter_group": "", "enabled": True, "type": "rds-mysql",
        })
    return results


def list_rds_mysql_multi_az_clusters(rds_client):
    clusters = []
    paginator = rds_client.get_paginator("describe_db_clusters")
    for page in paginator.paginate():
        for cluster in page.get("DBClusters", []):
            if cluster.get("Engine") == "mysql":
                clusters.append(cluster)
    return clusters


def discover_rds_mysql_multi_az_clusters(rds_client):
    all_clusters = list_rds_mysql_multi_az_clusters(rds_client)
    results = []
    for cluster in all_clusters:
        cluster_id = cluster["DBClusterIdentifier"]
        param_group = cluster.get("DBClusterParameterGroup", "")
        try:
            audit_on = is_cluster_audit_enabled(rds_client, param_group)
        except Exception as exc:
            print(f"  WARNING: Failed to check RDS cluster '{cluster_id}': {exc}")
            continue
        if not audit_on:
            continue
        instance_ids = [m["DBInstanceIdentifier"] for m in cluster.get("DBClusterMembers", [])]
        results.append({
            "cluster_id": cluster_id, "instance_ids": instance_ids,
            "parameter_group": param_group, "enabled": True, "type": "rds-mysql-multi-az-cluster",
        })
    return results


# ── Combined ──────────────────────────────────────────────────────────

def discover_all(rds_client, target_rds_type):
    results = []
    if target_rds_type in ("aurora-mysql", "both"):
        results.extend(discover_aurora_clusters(rds_client))
    if target_rds_type in ("rds-mysql", "both"):
        results.extend(discover_rds_mysql_instances(rds_client))
        results.extend(discover_rds_mysql_multi_az_clusters(rds_client))
    return results


def build_config(clusters, region):
    return {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "region": region,
        "clusters": [
            {"cluster_id": c["cluster_id"], "instance_ids": c["instance_ids"],
             "enabled": c["enabled"], "type": c.get("type", "aurora-mysql")}
            for c in clusters
        ],
    }


def write_config(config, output_path):
    with open(output_path, "w", encoding="utf-8") as fh:
        json.dump(config, fh, indent=2, ensure_ascii=False)
        fh.write("\n")


def parse_s3_uri(uri):
    if not uri.startswith("s3://"):
        raise ValueError(f"Invalid S3 URI: {uri}")
    path = uri[5:]
    if "/" not in path:
        raise ValueError(f"Invalid S3 URI (must include key): {uri}")
    bucket, key = path.split("/", 1)
    if not bucket or not key:
        raise ValueError(f"Invalid S3 URI: {uri}")
    return bucket, key


def upload_to_s3(session, s3_uri, config):
    bucket, key = parse_s3_uri(s3_uri)
    s3_client = session.client("s3")
    body = json.dumps(config, indent=2, ensure_ascii=False) + "\n"
    s3_client.put_object(Bucket=bucket, Key=key, Body=body.encode("utf-8"), ContentType="application/json")
    print(f"\nUploaded config to {s3_uri}")


def print_summary(clusters):
    if not clusters:
        print("\nNo targets with audit logging enabled found.")
        return
    print(f"\nDiscovered {len(clusters)} target(s) with audit logging enabled:\n")
    print(f"  {'ID':<40} {'Type':<15} {'Instances':<10} {'Param/Option Group'}")
    print(f"  {'-'*40} {'-'*15} {'-'*10} {'-'*30}")
    for c in clusters:
        print(f"  {c['cluster_id']:<40} {c.get('type',''):<15} "
              f"{len(c['instance_ids']):<10} {c.get('parameter_group','')}")


def main(argv=None):
    args = parse_args(argv)
    try:
        session = create_boto3_session(args.region)
    except Exception as exc:
        print(f"ERROR: Failed to create AWS session: {exc}", file=sys.stderr)
        sys.exit(1)

    region = session.region_name or "unknown"
    print(f"Scanning region: {region} (target: {args.target_rds_type})")

    rds_client = session.client("rds")
    try:
        clusters = discover_all(rds_client, args.target_rds_type)
    except Exception as exc:
        print(f"ERROR: Failed to discover targets: {exc}", file=sys.stderr)
        sys.exit(1)

    print_summary(clusters)
    config = build_config(clusters, region)

    try:
        write_config(config, args.output)
        print(f"\nConfig written to {args.output}")
    except Exception as exc:
        print(f"ERROR: Failed to write config: {exc}", file=sys.stderr)
        sys.exit(1)

    if args.upload:
        try:
            upload_to_s3(session, args.upload, config)
        except Exception as exc:
            print(f"ERROR: Failed to upload to S3: {exc}", file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
