"""
Cluster/Instance Discovery Lambda — auto-discover Aurora MySQL clusters
and RDS MySQL instances with audit logging enabled.

Scans the current region based on TARGET_RDS_TYPE:
  - "aurora-mysql": Aurora MySQL clusters only (default)
  - "rds-mysql": RDS MySQL clusters and instances only
  - "both": Aurora MySQL clusters + RDS MySQL clusters and instances

Generates a cluster_config.json and uploads it to S3.
Triggered daily by EventBridge.

Environment variables
---------------------
CONFIG_BUCKET : str
    S3 bucket to store the cluster configuration file.
CONFIG_S3_KEY : str
    S3 object key for ``cluster_config.json``.
TARGET_RDS_TYPE : str
    One of "aurora-mysql", "rds-mysql", "both". Defaults to "aurora-mysql".
"""

import boto3
import json
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

VALID_RDS_TYPES = {"aurora-mysql", "rds-mysql", "both"}


# ── Aurora MySQL cluster discovery ────────────────────────────────────

def list_aurora_mysql_clusters(rds_client):
    """List all Aurora MySQL clusters in the region."""
    clusters = []
    paginator = rds_client.get_paginator("describe_db_clusters")
    for page in paginator.paginate():
        for cluster in page.get("DBClusters", []):
            if cluster.get("Engine") == "aurora-mysql":
                clusters.append(cluster)
    return clusters


def list_rds_mysql_multi_az_clusters(rds_client):
    """List RDS MySQL Multi-AZ DB clusters (engine='mysql', 3 instances)."""
    clusters = []
    paginator = rds_client.get_paginator("describe_db_clusters")
    for page in paginator.paginate():
        for cluster in page.get("DBClusters", []):
            if cluster.get("Engine") == "mysql":
                clusters.append(cluster)
    return clusters


def is_cluster_audit_logging_enabled(rds_client, parameter_group_name):
    """Check if server_audit_logging is ON in a cluster parameter group."""
    paginator = rds_client.get_paginator("describe_db_cluster_parameters")
    for page in paginator.paginate(DBClusterParameterGroupName=parameter_group_name):
        for param in page.get("Parameters", []):
            if param.get("ParameterName") == "server_audit_logging":
                value = param.get("ParameterValue", "")
                return value.upper() == "ON" or value == "1"
    return False


def discover_aurora_clusters(rds_client):
    """Discover Aurora MySQL clusters with audit logging enabled."""
    all_clusters = list_aurora_mysql_clusters(rds_client)
    results = []

    for cluster in all_clusters:
        cluster_id = cluster["DBClusterIdentifier"]
        param_group = cluster.get("DBClusterParameterGroup", "")

        try:
            audit_on = is_cluster_audit_logging_enabled(rds_client, param_group)
        except Exception as exc:
            logger.warning(
                "Failed to check parameters for cluster '%s' (group: %s): %s",
                cluster_id, param_group, exc,
            )
            continue

        if not audit_on:
            logger.info("Cluster '%s' audit logging is OFF, skipping", cluster_id)
            continue

        instance_ids = [
            m["DBInstanceIdentifier"]
            for m in cluster.get("DBClusterMembers", [])
        ]

        results.append({
            "cluster_id": cluster_id,
            "instance_ids": instance_ids,
            "enabled": True,
            "type": "aurora-mysql",
        })

    return results


# ── RDS MySQL instance discovery ──────────────────────────────────────

def list_rds_mysql_instances(rds_client):
    """List all standalone RDS MySQL instances (not Aurora members)."""
    instances = []
    paginator = rds_client.get_paginator("describe_db_instances")
    for page in paginator.paginate():
        for inst in page.get("DBInstances", []):
            engine = inst.get("Engine", "")
            # Only standalone MySQL, not Aurora members
            if engine == "mysql" and not inst.get("DBClusterIdentifier"):
                instances.append(inst)
    return instances


def is_instance_audit_enabled_via_option_group(rds_client, option_group_name):
    """Check if MARIADB_AUDIT_PLUGIN is present in the option group.

    RDS MySQL uses the MariaDB Audit Plugin via option groups for audit logging.
    """
    try:
        response = rds_client.describe_option_groups(
            OptionGroupName=option_group_name,
        )
        for og in response.get("OptionGroupsList", []):
            for option in og.get("Options", []):
                if option.get("OptionName") == "MARIADB_AUDIT_PLUGIN":
                    return True
    except Exception as exc:
        logger.warning(
            "Failed to check option group '%s': %s", option_group_name, exc,
        )
    return False


def discover_rds_mysql_instances(rds_client):
    """Discover standalone RDS MySQL instances with audit logging enabled."""
    all_instances = list_rds_mysql_instances(rds_client)
    results = []

    for inst in all_instances:
        instance_id = inst["DBInstanceIdentifier"]

        # Check option groups for MariaDB Audit Plugin
        option_groups = inst.get("OptionGroupMemberships", [])
        audit_on = False
        for og in option_groups:
            og_name = og.get("OptionGroupName", "")
            if og_name and is_instance_audit_enabled_via_option_group(rds_client, og_name):
                audit_on = True
                break

        if not audit_on:
            logger.info("RDS instance '%s' audit plugin not found, skipping", instance_id)
            continue

        # For RDS MySQL, use instance_id as both cluster_id and instance_ids
        results.append({
            "cluster_id": instance_id,
            "instance_ids": [instance_id],
            "enabled": True,
            "type": "rds-mysql",
        })

    return results


def discover_rds_mysql_multi_az_clusters(rds_client):
    """Discover RDS MySQL Multi-AZ DB clusters with audit logging enabled.

    Multi-AZ DB clusters (engine='mysql') appear in describe_db_clusters
    with 1 writer + 2 readable standbys. Audit logging is checked via
    the cluster parameter group (same as Aurora).
    """
    all_clusters = list_rds_mysql_multi_az_clusters(rds_client)
    results = []

    for cluster in all_clusters:
        cluster_id = cluster["DBClusterIdentifier"]
        param_group = cluster.get("DBClusterParameterGroup", "")

        try:
            audit_on = is_cluster_audit_logging_enabled(rds_client, param_group)
        except Exception as exc:
            logger.warning(
                "Failed to check parameters for RDS cluster '%s': %s",
                cluster_id, exc,
            )
            continue

        if not audit_on:
            logger.info("RDS cluster '%s' audit logging is OFF, skipping", cluster_id)
            continue

        instance_ids = [
            m["DBInstanceIdentifier"]
            for m in cluster.get("DBClusterMembers", [])
        ]

        results.append({
            "cluster_id": cluster_id,
            "instance_ids": instance_ids,
            "enabled": True,
            "type": "rds-mysql-multi-az-cluster",
        })

    return results


# ── Combined discovery ────────────────────────────────────────────────

def discover_all(rds_client, target_rds_type):
    """Discover targets based on target_rds_type setting."""
    results = []

    if target_rds_type in ("aurora-mysql", "both"):
        aurora = discover_aurora_clusters(rds_client)
        logger.info("Found %d Aurora MySQL cluster(s)", len(aurora))
        results.extend(aurora)

    if target_rds_type in ("rds-mysql", "both"):
        rds_instances = discover_rds_mysql_instances(rds_client)
        logger.info("Found %d RDS MySQL standalone instance(s)", len(rds_instances))
        results.extend(rds_instances)

        rds_clusters = discover_rds_mysql_multi_az_clusters(rds_client)
        logger.info("Found %d RDS MySQL Multi-AZ DB cluster(s)", len(rds_clusters))
        results.extend(rds_clusters)

    return results


# ── Config helpers ────────────────────────────────────────────────────

def build_config(clusters, region):
    """Build a cluster_config dict from discovered targets."""
    return {
        "version": "1.0",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "region": region,
        "clusters": clusters,
    }


def read_existing_config(s3_client, bucket, key):
    """Read existing config from S3, return None if not found."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except s3_client.exceptions.NoSuchKey:
        return None
    except Exception as exc:
        logger.warning("Failed to read existing config: %s", exc)
        return None


def merge_enabled_flags(new_clusters, existing_config):
    """Preserve enabled=false flags from existing config."""
    if not existing_config:
        return new_clusters

    disabled = {
        c["cluster_id"]
        for c in existing_config.get("clusters", [])
        if not c.get("enabled", True)
    }

    for cluster in new_clusters:
        if cluster["cluster_id"] in disabled:
            cluster["enabled"] = False

    return new_clusters


# ── Lambda handler ────────────────────────────────────────────────────

def lambda_handler(event, context):
    """Entry point for the Cluster/Instance Discovery Lambda."""
    config_bucket = os.environ["CONFIG_BUCKET"]
    config_s3_key = os.environ["CONFIG_S3_KEY"]
    target_rds_type = os.environ.get("TARGET_RDS_TYPE", "aurora-mysql")

    if target_rds_type not in VALID_RDS_TYPES:
        logger.error("Invalid TARGET_RDS_TYPE: %s, must be one of %s",
                      target_rds_type, VALID_RDS_TYPES)
        return {"statusCode": 400, "body": {"error": f"Invalid TARGET_RDS_TYPE: {target_rds_type}"}}

    region = os.environ.get("AWS_REGION", "unknown")
    rds_client = boto3.client("rds")
    s3_client = boto3.client("s3")

    # 1. Discover targets
    try:
        clusters = discover_all(rds_client, target_rds_type)
    except Exception as exc:
        logger.error("Failed to discover targets: %s", exc)
        return {"statusCode": 500, "body": {"error": str(exc)}}

    logger.info("Discovered %d target(s) with audit logging enabled", len(clusters))

    # 2. Read existing config to preserve enabled flags
    existing_config = read_existing_config(s3_client, config_bucket, config_s3_key)
    clusters = merge_enabled_flags(clusters, existing_config)

    # 3. Build and upload config
    config = build_config(clusters, region)
    body = json.dumps(config, indent=2, ensure_ascii=False) + "\n"

    try:
        s3_client.put_object(
            Bucket=config_bucket,
            Key=config_s3_key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )
    except Exception as exc:
        logger.error("Failed to upload config to S3: %s", exc)
        return {"statusCode": 500, "body": {"error": str(exc)}}

    s3_uri = f"s3://{config_bucket}/{config_s3_key}"
    logger.info("Config uploaded to %s with %d target(s)", s3_uri, len(clusters))

    return {
        "statusCode": 200,
        "body": {
            "target_rds_type": target_rds_type,
            "targets_found": len(clusters),
            "config_s3_uri": s3_uri,
            "targets": [
                {"cluster_id": c["cluster_id"], "type": c.get("type", "aurora-mysql"), "enabled": c["enabled"]}
                for c in clusters
            ],
        },
    }
