"""
Dispatcher Lambda for multi-cluster Aurora audit log collection.

Reads cluster configuration from S3, then asynchronously invokes the
Worker Lambda for each enabled cluster.  Disabled clusters are skipped
with a log message.  If a single Worker invocation fails the error is
logged and processing continues with the remaining clusters.

Environment variables
---------------------
CONFIG_BUCKET : str
    S3 bucket containing the cluster configuration file.
CONFIG_S3_KEY : str
    S3 object key for ``cluster_config.json``.
WORKER_FUNCTION_NAME : str
    Name (or ARN) of the Worker Lambda function to invoke.
"""

import boto3
import json
import os
import logging

from config_model import parse_config

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """Entry point for the Dispatcher Lambda.

    Returns a dispatch summary with counts and per-cluster details.
    """
    config_bucket = os.environ["CONFIG_BUCKET"]
    config_s3_key = os.environ["CONFIG_S3_KEY"]
    worker_function_name = os.environ["WORKER_FUNCTION_NAME"]

    # ------------------------------------------------------------------
    # 1. Read cluster config from S3
    # ------------------------------------------------------------------
    s3_client = boto3.client("s3")
    try:
        response = s3_client.get_object(Bucket=config_bucket, Key=config_s3_key)
        config_json = response["Body"].read().decode("utf-8")
    except Exception as exc:
        logger.error("Failed to read config from s3://%s/%s: %s",
                      config_bucket, config_s3_key, exc)
        return {
            "statusCode": 500,
            "body": {"error": f"Failed to read config from S3: {exc}"},
        }

    # ------------------------------------------------------------------
    # 2. Parse and validate config
    # ------------------------------------------------------------------
    try:
        config = parse_config(config_json)
    except ValueError as exc:
        logger.error("Invalid cluster config: %s", exc)
        return {
            "statusCode": 500,
            "body": {"error": f"Invalid cluster config: {exc}"},
        }

    # ------------------------------------------------------------------
    # 3. Iterate clusters and dispatch
    # ------------------------------------------------------------------
    lambda_client = boto3.client("lambda")
    clusters = config.get("clusters", [])

    triggered = 0
    skipped = 0
    failed = 0
    details = []

    for cluster in clusters:
        cluster_id = cluster["cluster_id"]

        if not cluster.get("enabled", True):
            logger.info("Skipping disabled cluster: %s", cluster_id)
            skipped += 1
            details.append({
                "cluster_id": cluster_id,
                "status": "skipped",
                "reason": "disabled",
            })
            continue

        payload = {
            "cluster_id": cluster_id,
            "instance_ids": cluster["instance_ids"],
        }

        try:
            lambda_client.invoke(
                FunctionName=worker_function_name,
                InvocationType="Event",
                Payload=json.dumps(payload),
            )
            logger.info("Triggered Worker for cluster: %s", cluster_id)
            triggered += 1
            details.append({
                "cluster_id": cluster_id,
                "status": "triggered",
            })
        except Exception as exc:
            logger.error("Failed to invoke Worker for cluster %s: %s",
                          cluster_id, exc)
            failed += 1
            details.append({
                "cluster_id": cluster_id,
                "status": "failed",
                "reason": str(exc),
            })

    total_clusters = len(clusters)
    summary = {
        "total_clusters": total_clusters,
        "triggered": triggered,
        "skipped": skipped,
        "failed": failed,
        "details": details,
    }

    logger.info("Dispatch summary: %s", json.dumps(summary))

    return {
        "statusCode": 200,
        "body": summary,
    }
