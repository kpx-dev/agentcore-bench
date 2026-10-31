#!/usr/bin/env python3
"""Enable (or remove) per-second USAGE_LOGS vended-log delivery for an AgentCore Browser tool.

AgentCore's CloudWatch *metrics* for browser CPU/memory are vended per browser tool at 1-minute
resolution, so they aggregate every concurrent session. USAGE_LOGS are per **session** at 1-second
granularity (fields: `session.id`, `elapsed_time_seconds`, `browser.memory.gb_hours.used`,
`browser.vcpu.hours.used`), which is what you want for a memory-ceiling benchmark and for checking
the peak-memory billing claim.

This creates, idempotently: a CloudWatch log group, a delivery source (logType USAGE_LOGS), a
delivery destination, and the delivery joining them. `--teardown` removes the delivery, source and
destination this script created, and only deletes the log group with `--delete-log-group`.

    python setup_usage_logs.py                 # enable (prints the log group to pass to benchmark.py)
    python setup_usage_logs.py --status        # show current delivery config, change nothing
    python setup_usage_logs.py --teardown      # remove the delivery wiring
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger("setup-usage-logs")
LOG_TYPE = "USAGE_LOGS"


def paginate(fn, key: str) -> list[dict]:
    out, token = [], None
    while True:
        resp = fn(**({"nextToken": token} if token else {}))
        out.extend(resp.get(key, []))
        token = resp.get("nextToken")
        if not token:
            return out


def names(browser_id: str) -> tuple[str, str, str]:
    slug = browser_id.replace(".", "-")
    return f"{slug}-usage-source", f"{slug}-usage-destination", f"/aws/vendedlogs/bedrock-agentcore/browser/{LOG_TYPE}/{browser_id}"


def browser_resource_arn(region: str, account: str, browser_id: str) -> str:
    """ARN of the browser tool. The AWS-managed browsers (`aws.browser.*`) are owned by the service,
    so their ARN carries the literal account ID `aws`, not the caller's account."""
    owner = "aws" if browser_id.startswith("aws.") else account
    return f"arn:aws:bedrock-agentcore:{region}:{owner}:browser/{browser_id}"


def status(logs, browser_arn: str, browser_id: str) -> dict:
    src_name, dest_name, _ = names(browser_id)
    sources = [s for s in paginate(logs.describe_delivery_sources, "deliverySources")
               if s.get("logType") == LOG_TYPE and browser_arn in (s.get("resourceArns") or [])]
    deliveries = [d for d in paginate(logs.describe_deliveries, "deliveries")
                  if d.get("deliverySourceName") in {s["name"] for s in sources}]
    dests = {d["arn"]: d for d in paginate(logs.describe_delivery_destinations, "deliveryDestinations")}
    out = {"browser_arn": browser_arn, "sources": [s["name"] for s in sources], "log_groups": [],
           "deliveries": [d["id"] for d in deliveries], "ours": {"source": src_name, "destination": dest_name}}
    for d in deliveries:
        target = (dests.get(d["deliveryDestinationArn"], {})
                  .get("deliveryDestinationConfiguration", {}).get("destinationResourceArn", ""))
        if ":log-group:" in target:
            out["log_groups"].append(target.split(":log-group:", 1)[1].removesuffix(":*"))
    return out


def enable(logs, account: str, region: str, browser_arn: str, browser_id: str, retention_days: int) -> str:
    src_name, dest_name, group = names(browser_id)
    try:
        logs.create_log_group(logGroupName=group)
        log.info("Created log group %s", group)
    except logs.exceptions.ResourceAlreadyExistsException:
        log.info("Log group %s already exists", group)
    logs.put_retention_policy(logGroupName=group, retentionInDays=retention_days)
    group_arn = f"arn:aws:logs:{region}:{account}:log-group:{group}"

    src = logs.put_delivery_source(name=src_name, logType=LOG_TYPE, resourceArn=browser_arn)["deliverySource"]
    log.info("Delivery source %s -> %s (%s)", src["name"], browser_arn, LOG_TYPE)
    dest = logs.put_delivery_destination(
        name=dest_name, deliveryDestinationType="CWL", outputFormat="json",
        deliveryDestinationConfiguration={"destinationResourceArn": group_arn})["deliveryDestination"]
    try:
        delivery = logs.create_delivery(deliverySourceName=src["name"],
                                        deliveryDestinationArn=dest["arn"])["delivery"]
        log.info("Created delivery %s", delivery["id"])
    except logs.exceptions.ConflictException:
        log.info("Delivery already exists for %s -> %s", src["name"], dest["name"])
    return group


def teardown(logs, browser_arn: str, browser_id: str, delete_group: bool) -> None:
    src_name, dest_name, group = names(browser_id)
    for d in paginate(logs.describe_deliveries, "deliveries"):
        if d.get("deliverySourceName") == src_name:
            logs.delete_delivery(id=d["id"])
            log.info("Deleted delivery %s", d["id"])
    for call, kwargs, what in ((logs.delete_delivery_source, {"name": src_name}, f"source {src_name}"),
                               (logs.delete_delivery_destination, {"name": dest_name}, f"destination {dest_name}")):
        try:
            call(**kwargs)
            log.info("Deleted %s", what)
        except ClientError as exc:
            log.info("Skipped %s: %s", what, exc.response["Error"]["Code"])
    if delete_group:
        try:
            logs.delete_log_group(logGroupName=group)
            log.info("Deleted log group %s", group)
        except ClientError as exc:
            log.info("Skipped log group: %s", exc.response["Error"]["Code"])
    else:
        log.info("Left log group %s in place (pass --delete-log-group to remove it)", group)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--region", default=os.environ.get("AGENTCORE_REGION", "us-west-2"))
    p.add_argument("--browser-id", default=os.environ.get("AGENTCORE_BROWSER_ID", "aws.browser.v1"))
    p.add_argument("--retention-days", type=int, default=7)
    p.add_argument("--status", action="store_true", help="print the current config and exit")
    p.add_argument("--teardown", action="store_true", help="remove the delivery wiring this script created")
    p.add_argument("--delete-log-group", action="store_true", help="with --teardown, also delete the log group")
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    session = boto3.Session(region_name=args.region)
    account = session.client("sts").get_caller_identity()["Account"]
    logs = session.client("logs", region_name=args.region)
    browser_arn = browser_resource_arn(args.region, account, args.browser_id)

    if args.status:
        print(json.dumps(status(logs, browser_arn, args.browser_id), indent=1))
        return 0
    if args.teardown:
        teardown(logs, browser_arn, args.browser_id, args.delete_log_group)
        return 0
    try:
        group = enable(logs, account, args.region, browser_arn, args.browser_id, args.retention_days)
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        log.error("Could not enable USAGE_LOGS: %s: %s", code, exc.response["Error"].get("Message"))
        if code in ("ValidationException", "ResourceNotFoundException"):
            log.error("The AWS-managed browser (%s) may not accept vended-log delivery in this region. "
                      "Create a custom browser (bedrock-agentcore-control:CreateBrowser) and point "
                      "--browser-id at it, or run the benchmark without USAGE_LOGS (it falls back to the "
                      "CDP and guest-VM signals).", args.browser_id)
        return 1
    print(json.dumps(status(logs, browser_arn, args.browser_id) | {"log_group": group}, indent=1))
    log.info("Records appear ~1-2 min after a session ends. Run: python benchmark.py --usage-log-group %s", group)
    return 0


if __name__ == "__main__":
    sys.exit(main())
