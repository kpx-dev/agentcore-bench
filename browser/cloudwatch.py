"""CloudWatch metric discovery/sampling and USAGE_LOGS retrieval for AgentCore Browser.

Nothing here hardcodes metric names. Namespaces are found by listing every namespace
visible to ListMetrics and keeping the ones that look like AgentCore; metrics inside
them are classified as memory/CPU by name and scoped to this browser/session by their
dimension values. Whatever is found is logged and saved so a run documents itself.
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import re
from dataclasses import asdict, dataclass

import boto3
from botocore.exceptions import BotoCoreError, ClientError

log = logging.getLogger("bench.cloudwatch")

NAMESPACE_RE = re.compile(r"agent.?core", re.I)
SIGNAL_RES = {"memory": re.compile(r"mem", re.I), "cpu": re.compile(r"cpu", re.I),
              # Tool-level session count: tells us whether the memory/CPU series above is ours alone.
              "sessions": re.compile(r"session.*count|count.*session", re.I)}
SCOPE_RANK = {"session": 3, "resource": 2, "service": 1, "account": 0}


@dataclass
class SelectedMetric:
    signal: str          # "memory" | "cpu"
    namespace: str
    name: str
    dimensions: list[dict]
    scope: str           # session | resource | service | account (how specific to our session)
    stat: str
    unit_kind: str       # gb_hours | vcpu_hours | bytes | megabytes | percent | raw

    @property
    def key(self) -> str:
        dims = ",".join(f"{d['Name']}={d['Value']}" for d in self.dimensions)
        return f"{self.namespace}:{self.name}[{dims}]"

    def convert(self, value: float, period_s: int) -> float:
        """Convert a datapoint into MB (memory) or vCPUs (cpu) averaged over the period."""
        hours = period_s / 3600.0
        if self.unit_kind == "gb_hours":
            return value / hours * 1024.0
        if self.unit_kind == "vcpu_hours":
            return value / hours
        if self.unit_kind == "bytes":
            return value / 2**20
        return value


def infer_unit(name: str) -> tuple[str, str]:
    """(unit_kind, statistic) inferred from a discovered metric name."""
    if re.search(r"gb.?hours?", name, re.I):
        return "gb_hours", "Sum"
    if re.search(r"vcpu.?hours?", name, re.I):
        return "vcpu_hours", "Sum"
    if re.search(r"bytes", name, re.I):
        return "bytes", "Maximum"
    if re.search(r"(^|[^a-z])mb([^a-z]|$)|megabytes", name, re.I):
        return "megabytes", "Maximum"
    if re.search(r"percent|utili[sz]ation", name, re.I):
        return "percent", "Average"
    if re.search(r"count", name, re.I):
        return "count", "Maximum"
    return "raw", "Average"


def classify_scope(dims: list[dict], browser_id: str, session_id: str | None) -> str | None:
    """How specific a metric's dimensions are to our session; None if it belongs to something else."""
    values = {d["Name"]: d["Value"] for d in dims}
    if session_id and session_id in values.values():
        return "session"
    if any(v == browser_id for v in values.values()):
        return "resource"
    if "Resource" in values:
        return None  # another tool/resource
    service = values.get("Service")
    if service is not None:
        return "service" if re.search(r"browser", service, re.I) else None
    return "account" if not values else None


class CloudWatchMonitor:
    def __init__(self, boto_session: boto3.Session, region: str, browser_id: str,
                 namespace_scan_pages: int = 20, period_s: int = 60):
        self.cw = boto_session.client("cloudwatch", region_name=region)
        self.logs = boto_session.client("logs", region_name=region)
        self.region = region
        self.browser_id = browser_id
        self.scan_pages = namespace_scan_pages
        self.period_s = period_s
        self.namespaces: list[str] = []
        self.all_namespaces_scanned: list[str] = []
        self.namespace_scan_truncated = False
        self.metrics: list[dict] = []          # everything listed in the AgentCore namespaces
        self.selected: dict[str, SelectedMetric] = {}  # signal -> metric
        self.datapoints: dict[str, dict[str, float]] = {}  # metric key -> {iso ts: converted value}
        self.raw_datapoints: dict[str, dict[str, float]] = {}
        self.errors: list[str] = []

    # ---------------------------------------------------------------- discovery
    def discover_namespaces(self) -> list[str]:
        seen: set[str] = set()
        pages = 0
        token = None
        try:
            while True:
                kw = {"NextToken": token} if token else {}
                resp = self.cw.list_metrics(**kw)
                pages += 1
                seen.update(m["Namespace"] for m in resp.get("Metrics", []))
                token = resp.get("NextToken")
                if not token:
                    break
                if pages >= self.scan_pages:
                    self.namespace_scan_truncated = True
                    break
        except (ClientError, BotoCoreError) as exc:
            self._error(f"ListMetrics (namespace scan) failed: {exc}")
        self.all_namespaces_scanned = sorted(seen)
        found = sorted(n for n in seen if NAMESPACE_RE.search(n))
        log.info("CloudWatch namespace scan (%s): %d namespaces over %d page(s)%s; AgentCore-like: %s",
                 self.region, len(seen), pages, " (truncated)" if self.namespace_scan_truncated else "",
                 found or "NONE")
        if self.namespace_scan_truncated and not found:
            log.warning("Namespace scan truncated before finding an AgentCore namespace; "
                        "set AGENTCORE_CW_NAMESPACE to name it explicitly.")
        self.namespaces = sorted(set(found) | set(self.namespaces))
        return self.namespaces

    def discover_metrics(self, session_id: str | None, recently_active: bool = False) -> dict[str, SelectedMetric]:
        """List metrics in the AgentCore namespaces and (re)select memory/CPU series for this session."""
        listed: list[dict] = []
        for ns in self.namespaces:
            token = None
            try:
                while True:
                    kw: dict = {"Namespace": ns}
                    if token:
                        kw["NextToken"] = token
                    if recently_active:
                        kw["RecentlyActive"] = "PT3H"
                    resp = self.cw.list_metrics(**kw)
                    listed.extend(resp.get("Metrics", []))
                    token = resp.get("NextToken")
                    if not token:
                        break
            except (ClientError, BotoCoreError) as exc:
                self._error(f"ListMetrics {ns} failed: {exc}")
        if not recently_active or listed:
            known = {json.dumps(m, sort_keys=True) for m in self.metrics}
            new = [m for m in listed if json.dumps(m, sort_keys=True) not in known]
            if new:
                for m in new:
                    dims = ", ".join(f"{d['Name']}={d['Value']}" for d in m.get("Dimensions", []))
                    log.info("  discovered metric %s :: %s {%s}", m["Namespace"], m["MetricName"], dims)
                self.metrics.extend(new)

        changed = False
        for signal, pattern in SIGNAL_RES.items():
            best: SelectedMetric | None = None
            for m in self.metrics:
                if not pattern.search(m["MetricName"]):
                    continue
                scope = classify_scope(m.get("Dimensions", []), self.browser_id, session_id)
                if scope is None:
                    continue
                unit_kind, stat = infer_unit(m["MetricName"])
                cand = SelectedMetric(signal, m["Namespace"], m["MetricName"], m.get("Dimensions", []),
                                      scope, stat, unit_kind)
                if best is None or self._prefer(cand, best):
                    best = cand
            prev = self.selected.get(signal)
            if best is not None and (prev is None or best.key != prev.key):
                self.selected[signal] = best
                changed = True
                log.info("Selected CloudWatch %s metric: %s (scope=%s, stat=%s, unit=%s)",
                         signal, best.key, best.scope, best.stat, best.unit_kind)
                if best.scope in ("service", "resource", "account") and signal != "sessions":
                    log.warning("  %s metric is %s-level: AgentCore vends resource usage per browser tool, not "
                                "per session, so it covers every concurrent session on %s.",
                                signal, best.scope, self.browser_id)
        if not changed and not self.selected:
            log.info("No AgentCore Browser memory/CPU metric is listed yet (%d metric(s) in %s). "
                     "ListMetrics only shows series with recent data and new series can take ~15 min to "
                     "appear; will retry.", len(self.metrics), self.namespaces or "no AgentCore namespace")
        return self.selected

    @staticmethod
    def _prefer(a: SelectedMetric, b: SelectedMetric) -> bool:
        if SCOPE_RANK[a.scope] != SCOPE_RANK[b.scope]:
            return SCOPE_RANK[a.scope] > SCOPE_RANK[b.scope]
        a_used, b_used = "used" in a.name.lower(), "used" in b.name.lower()
        if a_used != b_used:
            return a_used
        return a.key < b.key

    # ---------------------------------------------------------------- sampling
    def poll(self, start: dt.datetime, end: dt.datetime) -> list[tuple[str, dt.datetime, float]]:
        """GetMetricData for the selected series over [start, end]; returns datapoints not seen before."""
        if not self.selected:
            return []
        metrics = list(self.selected.values())
        queries = [{
            "Id": f"m{i}",
            "MetricStat": {
                "Metric": {"Namespace": m.namespace, "MetricName": m.name, "Dimensions": m.dimensions},
                "Period": self.period_s,
                "Stat": m.stat,
            },
            "ReturnData": True,
        } for i, m in enumerate(metrics)]
        new: list[tuple[str, dt.datetime, float]] = []
        token = None
        try:
            while True:
                kw = {"MetricDataQueries": queries, "StartTime": start, "EndTime": end,
                      "ScanBy": "TimestampAscending"}
                if token:
                    kw["NextToken"] = token
                resp = self.cw.get_metric_data(**kw)
                for result in resp.get("MetricDataResults", []):
                    m = metrics[int(result["Id"][1:])]
                    store = self.datapoints.setdefault(m.key, {})
                    raw_store = self.raw_datapoints.setdefault(m.key, {})
                    for ts, value in zip(result.get("Timestamps", []), result.get("Values", [])):
                        iso = ts.astimezone(dt.timezone.utc).isoformat()
                        converted = m.convert(value, self.period_s)
                        if store.get(iso) != converted:
                            new.append((m.signal, ts, converted))
                        store[iso] = converted
                        raw_store[iso] = value
                token = resp.get("NextToken")
                if not token:
                    break
        except (ClientError, BotoCoreError) as exc:
            self._error(f"GetMetricData failed: {exc}")
        return new

    def series(self, signal: str) -> dict[str, float]:
        m = self.selected.get(signal)
        return dict(sorted(self.datapoints.get(m.key, {}).items())) if m else {}

    # ---------------------------------------------------------------- USAGE_LOGS
    def find_usage_log_groups(self) -> list[str]:
        """Log groups receiving USAGE_LOGS vended-log delivery for this browser (empty if not enabled)."""
        try:
            sources = self._paginate(self.logs.describe_delivery_sources, "deliverySources")
            ours = [s["name"] for s in sources
                    if s.get("logType") == "USAGE_LOGS"
                    and any(arn.endswith(f"/{self.browser_id}") for arn in s.get("resourceArns", []))]
            if not ours:
                log.info("No USAGE_LOGS delivery source configured for browser %s (per-second session "
                         "usage logs unavailable; see setup_usage_logs.py).", self.browser_id)
                return []
            deliveries = self._paginate(self.logs.describe_deliveries, "deliveries")
            dest_arns = {d["deliveryDestinationArn"] for d in deliveries if d.get("deliverySourceName") in ours}
            groups = []
            for dest in self._paginate(self.logs.describe_delivery_destinations, "deliveryDestinations"):
                if dest.get("arn") in dest_arns:
                    target = (dest.get("deliveryDestinationConfiguration") or {}).get("destinationResourceArn", "")
                    if ":log-group:" in target:
                        groups.append(target.split(":log-group:", 1)[1].removesuffix(":*"))
            log.info("USAGE_LOGS delivery for %s -> %s", self.browser_id, groups or "non-CloudWatch destination")
            return groups
        except (ClientError, BotoCoreError) as exc:
            self._error(f"USAGE_LOGS delivery discovery failed: {exc}")
            return []

    def fetch_usage_logs(self, group: str, session_id: str, start: dt.datetime, end: dt.datetime) -> list[dict]:
        records: list[dict] = []
        kw = {"logGroupName": group, "startTime": int(start.timestamp() * 1000),
              "endTime": int(end.timestamp() * 1000), "filterPattern": f'"{session_id}"'}
        try:
            for page in self.logs.get_paginator("filter_log_events").paginate(**kw):
                for ev in page.get("events", []):
                    rec = parse_usage_record(ev.get("message", ""), ev.get("timestamp"))
                    if rec:
                        records.append(rec)
        except (ClientError, BotoCoreError) as exc:
            self._error(f"FilterLogEvents {group} failed: {exc}")
        if records:
            log.info("USAGE_LOGS: %d per-second record(s) for session %s in %s; sample fields: %s",
                     len(records), session_id, group, sorted(records[0].get("fields", []))[:20])
        return records

    # ---------------------------------------------------------------- helpers
    def discovery_report(self) -> dict:
        return {
            "region": self.region,
            "browser_id": self.browser_id,
            "namespace_scan_truncated": self.namespace_scan_truncated,
            "all_namespaces_scanned": self.all_namespaces_scanned,
            "agentcore_namespaces": self.namespaces,
            "metrics_listed": self.metrics,
            "selected": {k: asdict(v) | {"key": v.key} for k, v in self.selected.items()},
            "period_s": self.period_s,
            "errors": self.errors,
        }

    def _paginate(self, fn, key: str) -> list[dict]:
        out, token = [], None
        while True:
            resp = fn(**({"nextToken": token} if token else {}))
            out.extend(resp.get(key, []))
            token = resp.get("nextToken")
            if not token:
                return out

    def _error(self, msg: str) -> None:
        log.warning(msg)
        self.errors.append(msg)


def _flatten(obj, prefix: str = "") -> dict[str, object]:
    out: dict[str, object] = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            out.update(_flatten(v, f"{prefix}{k}."))
    elif isinstance(obj, list):
        for item in obj:
            # [{"name": "...", "value": ...}] style metric lists
            if isinstance(item, dict) and "name" in item and "value" in item:
                out[f"{prefix}{item['name']}"] = item["value"]
            else:
                out.update(_flatten(item, prefix))
    else:
        out[prefix.rstrip(".")] = obj
    return out


def parse_usage_record(message: str, event_ts_ms: int | None) -> dict | None:
    """Extract per-second memory/CPU from a USAGE_LOGS record without assuming exact nesting."""
    try:
        flat = _flatten(json.loads(message))
    except (ValueError, TypeError):
        return None
    mem = next((v for k, v in flat.items() if re.search(r"memory.*gb.?hours", k, re.I)), None)
    cpu = next((v for k, v in flat.items() if re.search(r"vcpu.*hours", k, re.I)), None)
    if mem is None and cpu is None:
        return None
    ts = next((v for k, v in flat.items() if k.endswith("event_timestamp")), None) or event_ts_ms
    if isinstance(ts, str):
        try:
            ts = dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000
        except ValueError:
            ts = event_ts_ms
    # Each record covers a short interval (normally 1 s); convert the GB-hours/vCPU-hours it reports
    # back into the instantaneous GB / vCPU that were billed over that interval.
    elapsed = next((v for k, v in flat.items()
                    if re.search(r"(time_elapsed|elapsed_time)_seconds", k, re.I)), 1.0) or 1.0
    return {
        "t": float(ts) / 1000.0 if ts else None,
        "session_id": next((v for k, v in flat.items() if re.search(r"session.?id", k, re.I)), None),
        "elapsed_s": float(elapsed),
        "usage_log_mem_mb": float(mem) * 3600.0 / float(elapsed) * 1024.0 if mem is not None else None,
        "usage_log_vcpu": float(cpu) * 3600.0 / float(elapsed) if cpu is not None else None,
        "fields": list(flat),
    }
