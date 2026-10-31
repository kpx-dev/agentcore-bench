"""Evidence that arrives after the fact, pulled from the account into the run directory.

  logs.jsonl        the probe's own BOOT / RESTORE / REQUEST lines from each runtime's log groups:
                    which processes started, and when, including any started before a session existed
  cloudtrail.jsonl  ECR image pulls (BatchGetImage, GetDownloadUrlForLayer) on the run's repository:
                    who pulled, when, and by tag or by digest. CloudTrail lags by up to ~15 minutes.
  usage.jsonl       USAGE_LOGS records: AgentCore's per-session, per-second CPU and memory metering
  metrics.json      AWS/Bedrock-AgentCore CPUUsed-vCPUHours / MemoryUsed-GBHours per runtime, 1-minute
  costs.json        Cost Explorer usage types for AgentCore on the run's dates (`poc.py pricing`, a day later)

Each collector is independent and best-effort: a missing permission or not-yet-delivered data is
recorded as such, and `poc.py report <run>` can pull everything again later.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import time
from pathlib import Path

from botocore.exceptions import BotoCoreError, ClientError

log = logging.getLogger("poc.evidence")

METRIC_NAMESPACE = "AWS/Bedrock-AgentCore"


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w") as f:
        for row in rows:
            f.write(json.dumps(row, default=str) + "\n")


def _rows(path: Path) -> int:
    if not path.exists():
        return 0
    if path.suffix == ".jsonl":
        return sum(1 for line in path.read_text().splitlines() if line.strip())
    return len(json.loads(path.read_text()))


def _window(events: list[dict]) -> tuple[dt.datetime, dt.datetime]:
    times = [e["t"] for e in events if "t" in e]
    start = dt.datetime.fromtimestamp(min(times) - 300, dt.timezone.utc)
    end = dt.datetime.fromtimestamp(max(max(times), time.time()) + 60, dt.timezone.utc)
    return start, end


def runtime_log_groups(logs, runtime_ids: list[str]) -> list[str]:
    """AgentCore writes a runtime's container output to /aws/bedrock-agentcore/runtimes/<id>-<endpoint>."""
    paginator = logs.get_paginator("describe_log_groups")
    return [g["logGroupName"] for rid in runtime_ids
            for page in paginator.paginate(logGroupNamePrefix=f"/aws/bedrock-agentcore/runtimes/{rid}")
            for g in page["logGroups"]]


def runtime_logs(logs, state: dict, start: dt.datetime, end: dt.datetime) -> list[dict]:
    rows = []
    groups = set(state.get("runtime_log_groups", []))
    for key, rt in state.get("runtimes", {}).items():
        found = runtime_log_groups(logs, [rt["id"]])
        groups.update(found)
        for group in found:
            kw = {"logGroupName": group, "startTime": int(start.timestamp() * 1000),
                  "endTime": int(end.timestamp() * 1000), "filterPattern": '"poc"'}
            for page in logs.get_paginator("filter_log_events").paginate(**kw):
                for ev in page.get("events", []):
                    try:
                        msg = json.loads(ev["message"])
                    except ValueError:
                        continue
                    if isinstance(msg, dict) and "poc" in msg:
                        rows.append({"rt": key, "group": group, "stream": ev.get("logStreamName"),
                                     "timestamp": ev["timestamp"] / 1000, "ingested": ev.get("ingestionTime", 0) / 1000,
                                     **msg})
    state["runtime_log_groups"] = sorted(groups)
    for group in groups:
        try:
            logs.put_retention_policy(logGroupName=group, retentionInDays=30)   # AgentCore creates them without one
        except ClientError:
            pass
    return sorted(rows, key=lambda r: r.get("wall", r["timestamp"]))


def ecr_pulls(cloudtrail, repo_name: str, start: dt.datetime, end: dt.datetime) -> list[dict]:
    rows = []
    for name in ("BatchGetImage", "GetDownloadUrlForLayer"):
        kw = {"LookupAttributes": [{"AttributeKey": "EventName", "AttributeValue": name}],
              "StartTime": start, "EndTime": end}
        for page in cloudtrail.get_paginator("lookup_events").paginate(**kw):
            for ev in page.get("Events", []):
                detail = json.loads(ev.get("CloudTrailEvent") or "{}")
                params = detail.get("requestParameters") or {}
                if params.get("repositoryName") != repo_name:
                    continue
                ident = detail.get("userIdentity") or {}
                rows.append({"event": name, "time": detail.get("eventTime"), "t": ev["EventTime"].timestamp(),
                             "identity_type": ident.get("type"), "principal": ident.get("arn") or ident.get("invokedBy"),
                             "invoked_by": ident.get("invokedBy"), "source_ip": detail.get("sourceIPAddress"),
                             "user_agent": detail.get("userAgent"), "image_ids": params.get("imageIds"),
                             "layer_digest": params.get("layerDigest"), "error": detail.get("errorCode")})
            time.sleep(0.6)  # LookupEvents allows 2 requests per second per account
    return sorted(rows, key=lambda r: r["t"])


def usage_records(logs, state: dict, start: dt.datetime, end: dt.datetime) -> list[dict]:
    group = state.get("usage", {}).get("log_group")
    if not group:
        return []
    rows = []
    kw = {"logGroupName": group, "startTime": int(start.timestamp() * 1000), "endTime": int(end.timestamp() * 1000)}
    for page in logs.get_paginator("filter_log_events").paginate(**kw):
        for ev in page.get("events", []):
            try:
                rows.append({"timestamp": ev["timestamp"] / 1000, **json.loads(ev["message"])})
            except ValueError:
                rows.append({"timestamp": ev["timestamp"] / 1000, "raw": ev["message"]})
    return rows


def runtime_metrics(cw, state: dict, start: dt.datetime, end: dt.datetime) -> dict:
    out: dict = {}
    for key, rt in state.get("runtimes", {}).items():
        metrics = [m for page in cw.get_paginator("list_metrics").paginate(
                       Namespace=METRIC_NAMESPACE, Dimensions=[{"Name": "Resource", "Value": rt["arn"]}])
                   for m in page["Metrics"] if m["MetricName"] in ("CPUUsed-vCPUHours", "MemoryUsed-GBHours")]
        if not metrics:
            out[key] = {"note": "no AWS/Bedrock-AgentCore usage metrics listed for this runtime yet"}
            continue
        queries = [{"Id": f"m{i}", "Label": json.dumps({"metric": m["MetricName"], "dims": m["Dimensions"]}),
                    "MetricStat": {"Metric": m, "Period": 60, "Stat": "Sum"}} for i, m in enumerate(metrics)]
        resp = cw.get_metric_data(MetricDataQueries=queries, StartTime=start, EndTime=end)
        out[key] = [{**json.loads(r["Label"]), "points": sorted(
            ([t.timestamp(), v] for t, v in zip(r["Timestamps"], r["Values"], strict=True)), key=lambda p: p[0])}
            for r in resp["MetricDataResults"]]
    return out


def collect(session, region: str, run_dir: Path, state: dict, save) -> dict:
    events = [json.loads(line) for line in (run_dir / "events.jsonl").read_text().splitlines() if line.strip()]
    start, end = _window(events)
    logs = session.client("logs", region_name=region)
    summary: dict = {"window": [start.isoformat(), end.isoformat()], "collected_at": time.time()}
    collectors = {
        "logs.jsonl": lambda: runtime_logs(logs, state, start, end),
        "cloudtrail.jsonl": lambda: ecr_pulls(session.client("cloudtrail", region_name=region),
                                              state.get("ecr_repo", {}).get("name", ""), start, end),
        "usage.jsonl": lambda: usage_records(logs, state, start, end),
        "metrics.json": lambda: runtime_metrics(session.client("cloudwatch", region_name=region), state, start, end),
    }
    for name, fn in collectors.items():
        try:
            data = fn()
        except (ClientError, BotoCoreError) as exc:
            log.warning("Collecting %s failed: %s", name, exc)
            summary[name] = {"error": str(exc)}
            continue
        path = run_dir / name
        before = _rows(path)
        if len(data) < before:
            # After teardown the log groups are gone; never replace saved evidence with less of it.
            log.info("Kept the saved %s (%d rows; the account now returns %d)", name, before, len(data))
            summary[name] = {"rows": before, "kept_saved": True}
            continue
        if name.endswith(".jsonl"):
            _write_jsonl(run_dir / name, data)
        else:
            (run_dir / name).write_text(json.dumps(data, indent=1, default=str))
        summary[name] = {"rows": len(data)}
        log.info("Collected %s: %d rows", name, len(data))
    save()
    (run_dir / "evidence.json").write_text(json.dumps(summary, indent=1))
    return summary


def costs(session, run_dir: Path) -> dict:
    """AgentCore line items for the run's days, by usage type. Cost Explorer data lags by ~24 hours."""
    events = [json.loads(line) for line in (run_dir / "events.jsonl").read_text().splitlines() if line.strip()]
    start, _ = _window(events)
    first = start.date()
    last = dt.date.today() + dt.timedelta(days=1)
    ce = session.client("ce", region_name="us-east-1")
    rows, token = [], None
    while True:
        resp = ce.get_cost_and_usage(TimePeriod={"Start": first.isoformat(), "End": last.isoformat()}, Granularity="DAILY",
                                     Metrics=["UsageQuantity", "UnblendedCost"],
                                     GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}, {"Type": "DIMENSION", "Key": "USAGE_TYPE"}],
                                     **({"NextPageToken": token} if token else {}))
        for day in resp["ResultsByTime"]:
            for g in day["Groups"]:
                service, usage_type = g["Keys"]
                if "agentcore" not in (service + usage_type).lower():
                    continue
                rows.append({"date": day["TimePeriod"]["Start"], "service": service, "usage_type": usage_type,
                             "quantity": float(g["Metrics"]["UsageQuantity"]["Amount"]),
                             "unit": g["Metrics"]["UsageQuantity"].get("Unit"),
                             "cost_usd": float(g["Metrics"]["UnblendedCost"]["Amount"])})
        token = resp.get("NextPageToken")
        if not token:
            break
    out = {"period": [first.isoformat(), last.isoformat()], "rows": rows, "pulled_at": time.time(),
           "note": "Account-wide: includes any other AgentCore usage in this account on these days."}
    (run_dir / "costs.json").write_text(json.dumps(out, indent=1))
    return out
