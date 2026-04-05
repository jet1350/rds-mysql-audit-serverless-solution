"""
Cluster Discovery Lambda — auto-discover Aurora MySQL clusters.

Scans the current region for Aurora MySQL clusters with audit logging
enabled, generates a cluster_config.json, and uploads it to S3.
Triggered daily by EventBridge.

Environment variables
---------------------
CONFIG_BUCKET : str
    S3 bucket to store the cluster configuration file.
CONFIG_S3_KEY : str
    S3 object key for ``cluster_config.json``.
"""

import boto3
import json
import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def list_aurora_mysql_clusters(rds_client):
    """List all Aurora MySQL clusters in the region."""
    clusters = []
    paginator = rds_client.get_paginator("describe_db_clusters")
    for page in paginator.paginate():
        for cluster in page.get("DBClusters", []):
            if cluster.get("Engine") == "aurora-mysql":
                clusters.append(cluster)
    return clusters


def is_audit_logging_enabled(rds_client, parameter_group_name):
    """Check if server_audit_logging is ON in the parameter group."""
    paginator = rds_client.get_paginator("describe_db_cluster_parameters")
    for page in paginator.paginate(DBClusterParameterGroupName=parameter_group_name):
        for param in page.get("Parameters", []):
            if param.get("ParameterName") == "server_audit_logging":
                value = param.get("ParameterValue", "")
                return value.upper() == "ON" or value == "1"
    return False


def discover_clusters(rds_client):
    """Discover Aurora MySQL clusters with audit logging enabled."""
    all_clusters = list_aurora_mysql_clusters(rds_client)
    results = []

    for cluster in all_clusters:
        cluster_id = cluster["DBClusterIdentifier"]
        param_group = cluster.get("DBClusterParameterGroup", "")

        try:
            audit_on = is_audit_logging_enabled(rds_client, param_group)
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
        })

    return results


def build_config(clusters, region):
    """Build a cluster_config dict from discovered clusters."""
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
    """Preserve enabled=false flags from existing config.

    If a cluster was manually disabled (enabled=false) in the existing
    config, keep that setting. New clusters default to enabled=true.
    """
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


def lambda_handler(event, context):
    """Entry point for the Cluster Discovery Lambda."""
    config_bucket = os.environ["CONFIG_BUCKET"]
    config_s3_key = os.environ["CONFIG_S3_KEY"]

    region = os.environ.get("AWS_REGION", "unknown")
    rds_client = boto3.client("rds")
    s3_client = boto3.client("s3")

    # 1. Discover clusters
    try:
        clusters = discover_clusters(rds_client)
    except Exception as exc:
        logger.error("Failed to discover clusters: %s", exc)
        return {"statusCode": 500, "body": {"error": str(exc)}}

    logger.info("Discovered %d cluster(s) with audit logging enabled", len(clusters))

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
    logger.info("Config uploaded to %s with %d cluster(s)", s3_uri, len(clusters))

    return {
        "statusCode": 200,
        "body": {
            "clusters_found": len(clusters),
            "config_s3_uri": s3_uri,
            "clusters": [
                {"cluster_id": c["cluster_id"], "enabled": c["enabled"]}
                for c in clusters
            ],
        },
    }
