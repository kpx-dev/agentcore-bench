"""Turn a run directory into answers: results.json (machine-readable) and FINDINGS.md (shareable).

Everything is derived from files in the run directory, so `poc.py report <run>` can be re-run after
late evidence (CloudTrail, usage logs, Cost Explorer) arrives, without touching AWS resources.

Three independent signals identify a snapshot, and the report cross-checks them:
  * the agent's own BOOT lines in a runtime's `build-logs-*` stream: a process started to be
    snapshotted (its boot token is what every restored session of that snapshot reports);
  * CloudTrail image pulls under the runtime's execution role, whose session name says what pulled
    (`snapstart-build-<endpoint>` for a snapshot build) and whether it asked for a tag or a digest;
  * each session's `started` (restored/booted), `boot_token` and POC_MARKER (= runtime version).
Each BOOT line and each pull is attributed to the latest lifecycle step (create, update, endpoint
change) that started before it on the same runtime.
"""

from __future__ import annotations

import datetime as dt
import json
import math
from collections import Counter, defaultdict
from pathlib import Path

PRESTART_S = 10.0       # a process (V1) or restored instance (V2) this old at its first request was started ahead of time
ON_DEMAND_S = 2.0       # restored at most this long before its first request = restored for that request
PRICES = {"V1": {"vcpu_h": 0.0895, "gb_h": 0.00945}, "V2": {"vcpu_h": 0.1276, "gb_h": 0.0169}}
PRICING_URL = "https://aws.amazon.com/bedrock/agentcore/pricing/"
LABELS = {"v1_public": "V1 · PUBLIC", "v1_vpc": "V1 · VPC", "v2_public": "V2 · PUBLIC", "v2_vpc": "V2 · VPC",
          "v2_endpoints": "V2 · PUBLIC (endpoints/rollback)"}
MATRIX = ("v1_public", "v1_vpc", "v2_public", "v2_vpc")
QUESTIONS = [
    ("q1_vpc", "VPC mode: is V2 supported, and does it change restore/start time vs PUBLIC?"),
    ("q2_prestart", "Pre-started instances: does V2 keep a pool, or does every new session restore on demand?"),
    ("q3_rollback", "Rollback: can a runtime created on V2 be switched back to V1?"),
    ("q4_same_tag", "Same-tag updates: does an update to the same tag re-pull the image and rebuild the snapshot?"),
    ("q5_endpoints", "Endpoints: is the snapshot per version, and does each endpoint restore its own version?"),
    ("q6_pricing", "Pricing: is snapshot creation, storage or restore charged?"),
]


# --------------------------------------------------------------------------- loading
def _jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def _json(path: Path, default=None):
    return json.loads(path.read_text()) if path.exists() else default


class RunData:
    def __init__(self, run_dir: Path):
        self.dir = run_dir
        self.state = _json(run_dir / "state.json", {})
        self.events = _jsonl(run_dir / "events.jsonl")
        self.logs = _jsonl(run_dir / "logs.jsonl")
        self.pulls = _jsonl(run_dir / "cloudtrail.jsonl")
        self.usage = _jsonl(run_dir / "usage.jsonl")
        self.metrics = _json(run_dir / "metrics.json", {})
        self.costs = _json(run_dir / "costs.json")
        self.invokes = [e for e in self.events if e["kind"] == "invoke"]
        images = (self.kind("images") or [{}])[-1]
        self.builds = {img["digest"]: img["build_id"] for img in (images.get("a"), images.get("b")) if img}
        role_to_key = {r["name"]: k for k, r in self.state.get("iam_roles", {}).items()}
        role_prefix = f"agentcore-v2-poc-{self.state.get('run')}-"   # infra.ensure_role's naming (roles are gone after teardown)
        for p in self.pulls:
            parts = (p.get("principal") or "").split("/")
            role = parts[1] if len(parts) >= 3 and parts[0].endswith("assumed-role") else ""
            p["rt"] = role_to_key.get(role) or (role[len(role_prefix):] if role.startswith(role_prefix) else None)
            p["session_name"] = parts[2] if len(parts) >= 3 else (p.get("principal") or "?")
        self.ops = self._ops()
        self.snapshot_tokens: dict[str, set] = defaultdict(set)
        for x in self.logs:
            if x["poc"] == "BOOT" and x["stream"].startswith("build-logs"):
                self.snapshot_tokens[x["rt"]].add(x["boot"])
        self.token_sessions: Counter = Counter(
            (s["rt"], (s.get("data") or {}).get("boot_token")) for s in self.invokes if s["idx"] == 0 and s["ok"])
        for item in self.logs:
            item["op"] = self._attribute(item["rt"], item["wall"])
        for p in self.pulls:
            p["op"] = self._attribute(p["rt"], p["t"] + 2) if p["rt"] else None   # CloudTrail times are whole seconds

    def kind(self, kind: str, **match) -> list[dict]:
        return [e for e in self.events if e["kind"] == kind and all(e.get(k) == v for k, v in match.items())]

    def _ops(self) -> dict[str, list[dict]]:
        """Every lifecycle step per runtime, with its outcome: create, updates, endpoint changes."""
        out: dict[str, list[dict]] = defaultdict(list)
        finished = self.kind("op_done") + self.kind("op_rejected")
        for e in self.kind("op"):
            end = next((x for x in finished if x["action"] == e["action"] and x["rt"] == e["rt"]
                        and abs((x.get("t_start") or 0) - e["t_start"]) < 1e-3), {})
            out[e["rt"]].append({**e, "id": f"{e['rt']}#{len(out[e['rt']])}", "t_done": end.get("t_done"),
                                 "seconds": end.get("seconds"), "outcome": end.get("status") or end.get("error_code"),
                                 "platform_after": end.get("platform_version"), "failure": end.get("failure_reason") or end.get("error")})
        for e in self.kind("op_rejected"):
            if not any(o["action"] == e["action"] and abs(o["t_start"] - e["t_start"]) < 1e-3 for o in out[e["rt"]]):
                out[e["rt"]].append({**e, "id": f"{e['rt']}#{len(out[e['rt']])}", "outcome": e.get("error_code"),
                                     "failure": e.get("error"), "seconds": None, "t_done": None})
        for ops in out.values():
            ops.sort(key=lambda o: o["t_start"])
        return out

    def _attribute(self, rt: str | None, t: float) -> str | None:
        prior = [o for o in self.ops.get(rt, []) if o["t_start"] <= t]
        return prior[-1]["id"] if prior else None

    def op(self, rt: str, action: str) -> dict | None:
        rows = [o for o in self.ops.get(rt, []) if o["action"] == action]
        return rows[-1] if rows else None

    def snapshot_builds(self, op: dict | None) -> dict:
        """What one lifecycle step built: processes booted in the build stream, validation restores, image pulls."""
        if not op:
            return {}
        logs = [x for x in self.logs if x["op"] == op["id"] and x["stream"].startswith("build-logs")]
        boots = [x for x in logs if x["poc"] == "BOOT"]
        pulls = [p for p in self.pulls if p["op"] == op["id"] and p["event"] == "BatchGetImage"]
        refs = Counter()
        for p in pulls:
            for img in p.get("image_ids") or []:
                if img.get("imageTag"):
                    refs[f"tag {img['imageTag']}"] += 1
                else:
                    dg = img.get("imageDigest") or "?"
                    refs[f"digest {self.builds.get(dg, dg[:19])}"] += 1
        return {"boots": len(boots), "boot_tokens": sorted({b["boot"] for b in boots}),
                "validation_restores": sum(1 for x in logs if x["poc"] == "RESTORE"),
                "builder_sessions": sorted({p["session_name"] for p in pulls if p["session_name"].startswith("snapstart")}),
                "pull_sessions": sorted({p["session_name"].split("-")[0] for p in pulls}),
                "pulled": dict(refs), "logs_collected": bool(self.logs), "trail_collected": bool(self.pulls)}

    def samples(self, step: str | None = None, rt: str | None = None, idx: int | None = None,
                qualifier: str | None = None, prefix: bool = False) -> list[dict]:
        return [s for s in self.invokes
                if (step is None or (s["step"].startswith(step) if prefix else s["step"] == step))
                and (rt is None or s["rt"] == rt) and (idx is None or s["idx"] == idx)
                and (qualifier is None or s["qualifier"] == qualifier)]

    def cold(self, step: str, rt: str, qualifier: str | None = None) -> list[dict]:
        """The first, successful invocation of each session: what a new session landed on."""
        return [s for s in self.samples(step=step, rt=rt, idx=0, qualifier=qualifier) if s["ok"]]


# --------------------------------------------------------------------------- helpers
def pct(values, q: float) -> float | None:
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return None
    k = (len(vals) - 1) * q
    lo, hi = math.floor(k), math.ceil(k)
    return vals[lo] + (vals[hi] - vals[lo]) * (k - lo)


def ms(v) -> str:
    if v is None:
        return "–"
    return f"{v / 1000:.2f} s" if abs(v) >= 1000 else f"{v:.0f} ms"


def secs(v) -> str:
    if v is None:
        return "–"
    return f"{v / 60:.1f} min" if v >= 120 else f"{v:.0f} s"


def d(sample: dict) -> dict:
    return sample.get("data") or {}


def builds_of(samples: list[dict]) -> list[str]:
    return sorted({d(s).get("build_on_disk") or "?" for s in samples})


def tokens(samples: list[dict]) -> set[str]:
    return {d(s)["boot_token"] for s in samples if d(s).get("boot_token")}


def markers(samples: list[dict]) -> list[str]:
    return sorted({(d(s).get("env") or {}).get("POC_MARKER") or "?" for s in samples})


KINDS = {"snapshot": "snapshot restore", "pool": "pre-started instance", "fresh": "fresh boot"}


def classify(data: "RunData", s: dict) -> str:
    """How the instance behind a session's first request came to exist.

    snapshot: restored from a snapshot other sessions also restore (its startup token is shared, or was
              booted in the runtime's snapshot build): V2.
    pool:     its own process, booted ahead of time and frozen until this session arrived (V1's warm pool:
              booted at create or as the pool refills, paused once healthy, resumed for the session).
    fresh:    booted for this session.
    """
    tok = d(s).get("boot_token")
    if tok and (tok in data.snapshot_tokens.get(s["rt"], set()) or data.token_sessions[(s["rt"], tok)] > 1):
        return "snapshot"
    if (d(s).get("frozen_s") or 0) < 2:
        return "fresh"
    if not data.snapshot_tokens.get(s["rt"]):
        # No build logs to consult: fall back to how long the process ran before it was frozen. V2 snapshot builds
        # froze the agent ~70 s after boot in every run measured; V1 pool instances were paused ~3 s after boot.
        ran = (d(s).get("prestart_lead_s") or 0) - (d(s).get("first_request_since_restore_s") or 0)
        if ran >= 30:
            return "snapshot"
    return "pool"


def started(samples: list[dict], data: "RunData | None" = None) -> dict:
    data = data or _CURRENT[0]
    return dict(Counter(KINDS[classify(data, s)] for s in samples))


_CURRENT: list = [None]   # the RunData being analysed (set by analyze), so helpers can classify


def short(tok: str) -> str:
    return tok[:8]


def _get(rec: dict, dotted: str):
    """A USAGE_LOGS field, wherever the record keeps it (top level, `attributes`, `metrics` or `resource`)."""
    for scope in (rec, rec.get("attributes") or {}, rec.get("metrics") or {}, rec.get("resource") or {}):
        if isinstance(scope, dict) and dotted in scope:
            return scope[dotted]
    cur = rec
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def _plural(n: int, word: str) -> str:
    return f"{n} {word}{'' if n == 1 else 's'}"


# --------------------------------------------------------------------------- Q1
def q1_vpc(data: RunData) -> dict:
    rows = {}
    for key in MATRIX:
        sessions = defaultdict(list)
        for s in data.samples(step="q1", rt=key):
            sessions[s["session"]].append(s)
        create = data.op(key, "create")
        if not sessions and not create:
            continue
        firsts = [ss[0] for ss in sessions.values()]
        ok = [c for c in firsts if c["ok"]]
        warm = {s["session"]: s for s in data.samples(step="q1", rt=key, idx=1) if s["ok"]}
        overhead = [c["ms"] - warm[c["session"]]["ms"] for c in ok if c["session"] in warm]
        reads = defaultdict(list)
        for s in data.samples(step="q1", rt=key):
            if s["ok"] and s["op"] == "ballast" and "ballast_read_ms" in d(s):
                reads[d(s)["ballast_read_index"]].append(d(s)["ballast_read_ms"])
        egress = next((d(s).get("egress") for s in data.samples(step="q1", rt=key) if s["op"] == "egress" and s["ok"]), None)
        size = next((d(s).get("ballast_bytes") for s in data.samples(step="q1", rt=key) if d(s).get("ballast_bytes")), 0)
        cold_ms = [c["ms"] for c in ok]
        rows[key] = {
            "create": (create or {}).get("outcome"), "create_s": (create or {}).get("seconds"),
            "create_failure": (create or {}).get("failure"),
            "sessions": len(firsts), "ok": len(ok), "errors": [c.get("error") for c in firsts if not c["ok"]][:3],
            "cold_ms": {"p50": pct(cold_ms, .5), "p75": pct(cold_ms, .75), "p90": pct(cold_ms, .9),
                        "max": max(cold_ms, default=None)},
            "cold_ms_all": cold_ms, "warm_p50_ms": pct([w["ms"] for w in warm.values()], .5),
            "startup_overhead_p50_ms": pct(overhead, .5),
            "first_read_p50_ms": pct(reads.get(0, []), .5), "second_read_p50_ms": pct(reads.get(1, []), .5),
            "first_read_ms": sorted(reads.get(0, [])),
            "file_mb": round(size / 1048576), "egress": egress,
        }
    out: dict = {"runtimes": rows}

    def delta(a: str, b: str, q: str) -> float | None:
        if rows.get(a, {}).get("cold_ms", {}).get(q) is not None and rows.get(b, {}).get("cold_ms", {}).get(q) is not None:
            return rows[a]["cold_ms"][q] - rows[b]["cold_ms"][q]
        return None

    out["v2_vpc_minus_public_ms"] = {q: delta("v2_vpc", "v2_public", q) for q in ("p50", "p75", "p90")}
    out["v1_vpc_minus_public_ms"] = {q: delta("v1_vpc", "v1_public", q) for q in ("p50", "p75", "p90")}
    v = rows.get("v2_vpc")
    if not v:
        out["answer"] = "Not tested in this run."
    elif v["create"] != "READY":
        out["answer"] = f"**No**: the V2 runtime in VPC mode ended {v['create']}: {v['create_failure']}"
    elif not v["ok"]:
        out["answer"] = f"The V2 VPC runtime reached READY but no session succeeded: {v['errors']}"
    else:
        dv = out["v2_vpc_minus_public_ms"]
        parts = [f"**Yes.** V2 in VPC mode (private subnets, no internet{_isolation(v['egress'])}) reached READY in "
                 f"{secs(v['create_s'])} and served {v['ok']}/{v['sessions']} new sessions"]
        if dv["p50"] is not None:
            p = rows["v2_public"]
            parts.append(f"cold-start invoke p50 {ms(v['cold_ms']['p50'])} vs {ms(p['cold_ms']['p50'])} in PUBLIC "
                         f"(Δ p50 {dv['p50']:+.0f} ms, Δ p90 {dv['p90']:+.0f} ms); V2 PUBLIC reached READY in "
                         f"{secs(p['create_s'])}")
        dv1 = out["v1_vpc_minus_public_ms"]
        if dv1["p50"] is not None:
            parts.append(f"for comparison V1 VPC vs PUBLIC: {ms(rows['v1_vpc']['cold_ms']['p50'])} vs "
                         f"{ms(rows['v1_public']['cold_ms']['p50'])} (Δ p50 {dv1['p50']:+.0f} ms)")
        out["answer"] = "; ".join(parts) + "."
    return out


def _isolation(egress: list[dict] | None) -> str:
    if not egress:
        return ""
    by = {e["target"]: e for e in egress}
    parts = []
    if "internet" in by:
        parts.append("internet unreachable" if not by["internet"].get("reachable") else "INTERNET REACHABLE")
    if by.get("ecr_api", {}).get("private"):
        parts.append("ECR via its interface endpoint")
    if by.get("s3", {}).get("reachable"):
        parts.append("S3 via the gateway endpoint")
    return f"; checked from inside the session: {', '.join(parts)}" if parts else ""


# --------------------------------------------------------------------------- Q2
def q2_prestart(data: RunData) -> dict:
    rows = {}
    for key in MATRIX:
        mine = [s for s in data.invokes if s["rt"] == key]
        if not mine:
            continue
        first_invoke = min(s["client_send_wall"] for s in mine)
        logs = [x for x in data.logs if x["rt"] == key and x["wall"] < first_invoke - 1]
        build = [x for x in logs if x["stream"].startswith("build-logs")]
        live = [x for x in logs if not x["stream"].startswith("build-logs")]
        cold = data.cold("q1", key)
        platform = cold[0]["platform"] if cold else key[:2].upper()
        age = [d(s)["request_received_wall"] - d(s)["process_started_wall"] for s in cold]
        since = [d(s).get("first_request_since_restore_s") for s in cold]
        ran = [d(s)["prestart_lead_s"] - (d(s).get("first_request_since_restore_s") or 0) for s in cold]
        rows[key] = {
            "platform": platform, "sessions": len(cold), "started": started(cold, data),
            "distinct_startup_tokens": len(tokens(cold)),
            "logs_collected": any(x["rt"] == key for x in data.logs),
            "snapshot_build_boots": sum(1 for x in build if x["poc"] == "BOOT"),
            "snapshot_build_test_restores": sum(1 for x in build if x["poc"] == "RESTORE"),
            "boots_before_first_session": sum(1 for x in live if x["poc"] == "BOOT"),
            "restores_before_first_session": sum(1 for x in live if x["poc"] == "RESTORE"),
            "process_age_at_first_request_s": {"p50": pct(age, .5), "min": min(age, default=None), "max": max(age, default=None)},
            "ran_before_freeze_s": {"p50": pct(ran, .5)},
            "frozen_s": {"p50": pct([d(s).get("frozen_s") for s in cold], .5)},
            "resumed_before_first_request_s": {"p50": pct(since, .5), "max": max((x for x in since if x is not None), default=None),
                                               "n_on_demand": sum(1 for x in since if x is not None and x <= ON_DEMAND_S)},
            "cold_p50_ms": pct([s["ms"] for s in cold], .5),
            "quiet_window_s": (data.kind("q2_quiet") or [{}])[-1].get("seconds"),
        }
    out: dict = {"runtimes": rows}
    v2 = {k: r for k, r in rows.items() if r["platform"] == "V2" and r["sessions"]}
    v1 = {k: r for k, r in rows.items() if r["platform"] == "V1" and r["sessions"]}
    parts = []
    if v2:
        n = sum(r["sessions"] for r in v2.values())
        snap = sum(r["started"].get(KINDS["snapshot"], 0) for r in v2.values())
        on_demand = sum(r["resumed_before_first_request_s"]["n_on_demand"] for r in v2.values())
        worst = max((r["resumed_before_first_request_s"]["max"] or 0) for r in v2.values())
        early = sum(r["boots_before_first_session"] + r["restores_before_first_session"] for r in v2.values())
        copies = sorted({r["distinct_startup_tokens"] for r in v2.values()})
        logs_ok = all(r["logs_collected"] for r in v2.values())
        lead = ("**Every new session restores on demand; no pre-started or pre-restored V2 instances were seen.** "
                if snap == n and on_demand == n and (early == 0 or not logs_ok) else "")
        s = (f"{lead}{snap}/{n} new V2 sessions restored the version's snapshot (sessions shared "
             f"{'/'.join(map(str, copies))} snapshot cop{'y' if copies == [1] else 'ies'} per runtime), each restored at most "
             f"{worst:.2f} s before its first request arrived ({on_demand}/{n} ≤{ON_DEMAND_S:.0f} s)")
        if logs_ok:
            s += (", and before the first session " + ("nothing was booted or restored outside the snapshot build" if early == 0
                                                      else f"{early} instance(s) were booted or restored outside the build"))
        parts.append(s + ".")
    if v1:
        n = sum(r["sessions"] for r in v1.values())
        pool = sum(r["started"].get(KINDS["pool"], 0) for r in v1.values())
        ages = [r["process_age_at_first_request_s"]["p50"] for r in v1.values() if r["process_age_at_first_request_s"]["p50"]]
        mins = [r["process_age_at_first_request_s"]["min"] for r in v1.values() if r["process_age_at_first_request_s"]["min"] is not None]
        s = (f"V1, for comparison, does keep a warm pool: {pool}/{n} V1 sessions landed on their own instance that had "
             f"been booted ahead of time (median {secs(pct(ages, .5))} before the session, newest {secs(min(mins, default=None))}), "
             f"ran ~{pct([r['ran_before_freeze_s']['p50'] for r in v1.values()], .5):.1f} s, was frozen, and was resumed "
             f"~{pct([r['resumed_before_first_request_s']['p50'] for r in v1.values()], .5):.2f} s before the first request")
        if all(r["logs_collected"] for r in v1.values()):
            s += (f"; {' and '.join(str(r['boots_before_first_session']) for r in v1.values())} V1 instances had booted before "
                  f"the first session was even sent")
        v1c = pct([r["cold_p50_ms"] for r in v1.values()], .5)
        v2c = pct([r["cold_p50_ms"] for r in v2.values()], .5) if v2 else None
        if v1c and v2c:
            s += f". At this low, sequential load that pool made V1's cold invoke (p50 {ms(v1c)}) faster than V2's (p50 {ms(v2c)})"
        parts.append(s + ".")
    out["answer"] = " ".join(parts) or "Not tested in this run."
    return out


def build_timeline(data: RunData) -> list[dict]:
    """For each V2 create: when the image was resolved, when the agent booted and was pinged, when the
    snapshot was taken (restore wall-clock minus the jump), validation restores, READY, first session."""
    out = []
    for key, ops in data.ops.items():
        op = next((o for o in ops if o["action"] == "create" and (o.get("platform") == "V2")), None)
        if not op or not op.get("t_done"):
            continue
        t0 = op["t_start"]
        logs = [x for x in data.logs if x["op"] == op["id"] and x["stream"].startswith("build-logs")]
        boots = sorted(x["wall"] for x in logs if x["poc"] == "BOOT")
        pings = [x for x in logs if x["poc"] == "PING" and x.get("n") == 1]
        restores = sorted((x["wall"], x["wall"] - x["jump_s"]) for x in logs if x["poc"] == "RESTORE")
        pulls = sorted(p["t"] for p in data.pulls if p["op"] == op["id"] and p["event"] == "BatchGetImage")
        first = min((s["client_send_wall"] for s in data.invokes if s["rt"] == key), default=None)
        out.append({"rt": key, "image_pulls_s": [round(t - t0) for t in pulls][:6],
                    "boots_s": [round(t - t0) for t in boots],
                    "first_ping_after_boot_s": [round(x["since_import_s"], 1) for x in pings],
                    "snapshot_taken_s": [round(snap - t0) for _, snap in restores],
                    "validation_restores_s": [round(w - t0) for w, _ in restores],
                    "ready_s": round(op["t_done"] - t0), "first_session_s": round(first - t0) if first else None})
    return out


# --------------------------------------------------------------------------- Q3
def q3_rollback(data: RunData) -> dict:
    key = "v2_endpoints"
    back, fwd = data.op(key, "q3.rollback_to_v1"), data.op(key, "q3.rollforward_to_v2")
    if not back:
        return {"answer": "Not tested in this run."}
    plat = data.kind("q3_platform")
    reported = {"before_rollback": plat[0].get("before") if plat else None,
                "after_rollback": next((e["after_rollback"] for e in plat if "after_rollback" in e), None),
                "after_rollforward": next((e["after_rollforward"] for e in plat if "after_rollforward" in e), None)}
    steps = []
    for label, step, qualifier in (("Before: DEFAULT on V2", "q3.inflight_before", "DEFAULT"),
                                   ("After V2→V1: new sessions on DEFAULT", "q3.after_rollback", "DEFAULT"),
                                   ("After V2→V1: LIVE (pinned to an older V2 version)", "q3.pinned_after_rollback", "LIVE"),
                                   ("After V1→V2: new sessions on DEFAULT", "q3.after_rollforward", "DEFAULT")):
        ss = data.cold(step, key, qualifier)
        if ss:
            steps.append({"step": label, "sessions": len(ss), "started": started(ss, data), "markers": markers(ss),
                          "distinct_startup_tokens": len(tokens(ss)), "cold_ms_p50": pct([s["ms"] for s in ss], .5),
                          "frozen_s_p50": pct([d(s).get("frozen_s") for s in ss], .5)})
    before = data.samples(step="q3.inflight_before", rt=key)
    later = data.samples(step="q3.inflight_after_rollback", rt=key) + data.samples(step="q3.inflight_after_rollforward", rt=key)
    kept = (bool(before) and before[0]["ok"] and bool(later) and all(s["ok"] for s in later)
            and {d(s).get("instance_token") for s in later} == {d(before[0]).get("instance_token")})
    versions = (data.kind("versions", rt=key) or [{}])[-1].get("versions", [])
    out = {"rollback": _op_summary(data, back), "rollforward": _op_summary(data, fwd), "platform_reported": reported,
           "steps": steps, "inflight_session_kept_instance": kept, "inflight_calls": len(later), "versions": versions}
    if back.get("outcome") != "READY":
        out["answer"] = f"**No**: UpdateAgentRuntime(platformVersion=V1) was {back.get('outcome')}: {back.get('failure')}"
        return out
    after = data.cold("q3.after_rollback", key, "DEFAULT")
    again = data.cold("q3.after_rollforward", key, "DEFAULT")
    pinned = data.cold("q3.pinned_after_rollback", key, "LIVE")
    parts = [f"**Not fixed at create.** UpdateAgentRuntime(platformVersion=V1) on the V2 runtime created version "
             f"{back.get('version')} and reached READY in {secs(back.get('seconds'))}"
             + (f"; GetAgentRuntime then reported {reported['after_rollback']}" if reported.get("after_rollback") else "")]
    if after:
        v1_like = len(tokens(after)) == len(after) and not started(after, data).get(KINDS["snapshot"])
        parts.append(f"new sessions {'behaved as V1' if v1_like else 'did NOT behave as V1'} (each on its own instance: "
                     f"{len(tokens(after))} distinct startup tokens in {len(after)} sessions)")
    if fwd and fwd.get("outcome") == "READY":
        parts.append(f"switching back to V2 worked too (READY in {secs(fwd.get('seconds'))}; "
                     f"{started(again, data).get(KINDS['snapshot'], 0)}/{len(again)} sessions restored a snapshot)")
    if later:
        parts.append("a session opened before the switch " + ("kept its instance and answered through both switches"
                                                             if kept else "did NOT keep its instance"))
    if pinned:
        parts.append(f"an endpoint pinned to the older V2 version kept serving snapshot restores "
                     f"({started(pinned, data).get(KINDS['snapshot'], 0)}/{len(pinned)})")
    out["answer"] = "; ".join(parts) + "."
    return out


def _op_summary(data: RunData, op: dict | None) -> dict | None:
    if not op:
        return None
    return {"action": op["action"], "version": op.get("version"), "platform": op.get("platform"), "outcome": op.get("outcome"),
            "seconds": op.get("seconds"), "failure": op.get("failure"), **data.snapshot_builds(op)}


# --------------------------------------------------------------------------- Q4
def q4_same_tag(data: RunData) -> dict:
    moved = (data.kind("q4_tag_moved") or [None])[-1]
    if not moved:
        return {"answer": "Not tested in this run."}
    images = (data.kind("images") or [{}])[-1]
    a, b = images.get("a", {}).get("build_id"), images.get("b", {}).get("build_id")
    rows = {}
    for key in MATRIX:
        if not data.samples(step="q4", rt=key, prefix=True) and not data.op(key, "q4.same_tag_update"):
            continue
        upd, noop = data.op(key, "q4.same_tag_update"), data.op(key, "q4.noop_update")
        base, moved_s = data.cold("q1", key), data.cold("q4.after_move", key)
        after_u, after_n = data.cold("q4.after_update", key), data.cold("q4.after_noop", key)
        bu, bn = data.snapshot_builds(upd), data.snapshot_builds(noop)
        logs_ok = any(x["rt"] == key for x in data.logs)
        rows[key] = {
            "before": builds_of(base), "tag_moved_no_update": builds_of(moved_s),
            "same_tag_update": _op_summary(data, upd), "after_update": builds_of(after_u),
            "after_update_restored_new_snapshot": _new_snapshot(after_u, bu, base + moved_s, logs_ok),
            "noop_update": _op_summary(data, noop), "after_noop": builds_of(after_n),
            "after_noop_restored_new_snapshot": _new_snapshot(after_n, bn, after_u, logs_ok),
            "after_update_started": started(after_u), "after_noop_started": started(after_n),
        }
    out: dict = {"tag": moved.get("tag"), "build_a": a, "build_b": b, "runtimes": rows}
    parts = []
    for key, r in rows.items():
        u, n = r["same_tag_update"] or {}, r["noop_update"] or {}
        s = f"{LABELS[key]}: "
        if key.startswith("v2"):
            s += f"with the tag moved but no update, new sessions still restored build {'/'.join(r['tag_moved_no_update']) or '?'}; "
        else:
            s += f"tag moved, no update: new sessions ran build {'/'.join(r['tag_moved_no_update']) or '?'}; "
        if u.get("outcome") == "READY":
            s += f"the same-tag update (v{u['version']}, READY in {secs(u['seconds'])}) "
            if key.startswith("v2"):
                snap = r["after_update_restored_new_snapshot"]
                s += (f"resolved the tag again{_pull_note(u)}; new sessions ran build {'/'.join(r['after_update'])} from "
                      + ("a newly built snapshot" if snap else "the old snapshot" if snap is False else "a snapshot"))
            else:
                s += f"→ new sessions ran build {'/'.join(r['after_update'])}"
        elif u:
            s += f"the same-tag update was {u.get('outcome')}: {u.get('failure')}"
        if n.get("outcome") == "READY":
            snap = r["after_noop_restored_new_snapshot"]
            s += (f"; an update with nothing changed still created v{n['version']} (READY in {secs(n['seconds'])})"
                  + ((" and rebuilt the snapshot" if snap else " and kept the snapshot" if snap is False else "")
                     if key.startswith("v2") else ""))
        elif n:
            s += f"; an update with nothing changed was {n.get('outcome')}: {n.get('failure')}"
        parts.append(s)
    v2_ok = [r for k, r in rows.items() if k.startswith("v2") and b in r["after_update"]]
    lead = ("**Yes: an update naming the same tag re-pulls it and rebuilds the snapshot.** " if v2_ok else "")
    out["answer"] = (lead + ". ".join(parts) + f". (Build A `{a}`, build B `{b}`.)") if parts else "Not tested in this run."
    return out


def _new_snapshot(after: list[dict], build: dict, before: list[dict], logs_ok: bool) -> bool | None:
    """Did sessions after a step restore a snapshot that step built? From the build logs when collected, otherwise
    from startup tokens never seen before the step."""
    if not after:
        return None
    if logs_ok:
        return bool(build.get("boots")) and tokens(after) <= set(build.get("boot_tokens", []))
    return not (tokens(after) & tokens(before))


def _pull_note(op: dict) -> str:
    pulled = op.get("pulled") or {}
    return f" (ECR pulls: {', '.join(f'{k} ×{v}' for k, v in pulled.items())})" if pulled else ""


# --------------------------------------------------------------------------- Q5
def q5_endpoints(data: RunData) -> dict:
    """Which snapshot each endpoint restores, traced back to the lifecycle step whose build booted it."""
    key = "v2_endpoints"
    if not data.samples(step="q5", rt=key, prefix=True):
        return {"answer": "Not tested in this run."}
    versions = (data.kind("q5_versions") or [{}])[-1]
    v1, v2 = versions.get("v1"), versions.get("v2")
    ops = {o["id"]: o for o in data.ops.get(key, [])}
    origin = {x["boot"]: ops[x["op"]]["action"] for x in data.logs
              if x["rt"] == key and x["poc"] == "BOOT" and x["stream"].startswith("build-logs") and x.get("op") in ops}
    steps = {}
    for step, label in (("q5.all_on_v1", f"DEFAULT, LIVE, test → v{v1}"),
                        ("q5.split", f"DEFAULT, test → v{v2}; LIVE → v{v1}"),
                        ("q5.repoint_unreferenced", f"LIVE → v{v2} again, after no endpoint pointed at it")):
        per = {}
        for q in ("DEFAULT", "LIVE", "test"):
            ss = data.cold(step, key, q)
            if ss:
                toks = tokens(ss)
                per[q] = {"sessions": len(ss), "markers": markers(ss), "builds": builds_of(ss),
                          "snapshots": sorted(short(t) for t in toks),
                          "built_by": sorted({origin.get(t, "?") for t in toks}), "started": started(ss, data),
                          "_tokens": toks}
        steps[step] = {"label": label, "endpoints": per}
    op_rows = [_op_summary(data, o) | {"endpoint": o.get("endpoint")}
               for o in data.ops.get(key, []) if o["action"].startswith(("create", "endpoint_", "q5."))]
    out = {"versions": versions, "ops": op_rows,
           "steps": {k: {"label": v["label"], "endpoints": {q: {kk: vv for kk, vv in r.items() if not kk.startswith("_")}
                                                            for q, r in v["endpoints"].items()}} for k, v in steps.items()}}

    one = steps["q5.all_on_v1"]["endpoints"]
    same_version = [(a, b) for a in one for b in one if a < b]
    shared = [(a, b) for a, b in same_version if one[a]["_tokens"] & one[b]["_tokens"]]
    ep_builds = [r for r in op_rows if r["action"].startswith("endpoint_") and r.get("boots")]
    ep_ops = [r for r in op_rows if r["action"].startswith("endpoint_")]
    logs_ok = bool(origin)
    per_endpoint = bool(same_version) and not shared and (
        not logs_ok or any(any(b.startswith("endpoint_") for b in r["built_by"]) for r in one.values()))
    parts = []
    if one:
        parts.append(f"with DEFAULT, LIVE and test all on v{v1}, " + "; ".join(
            f"{q} restored snapshot {'/'.join(r['snapshots'])}" + (f" (built by {'/'.join(r['built_by'])})" if logs_ok else "")
            for q, r in one.items()))
    split = steps["q5.split"]["endpoints"]
    if split:
        parts.append(f"after the runtime moved to v{v2}, every endpoint restored the version it pointed at: " + "; ".join(
            f"{q} → marker {'/'.join(r['markers'])}" for q, r in split.items()))
    if ep_ops:
        parts.append((f"{len(ep_builds)} of {len(ep_ops)} endpoint creates/updates built a snapshot of their own"
                      if logs_ok else "endpoint creates/updates took") + f" ({', '.join(secs(r['seconds']) for r in ep_ops)})")
    rep = steps["q5.repoint_unreferenced"]["endpoints"].get("LIVE")
    if rep:
        earlier = steps["q5.split"]["endpoints"]
        reused = any(rep["_tokens"] & r["_tokens"] for r in earlier.values() if r["markers"] == rep["markers"])
        parts.append(f"re-pointing LIVE at v{v2} once nothing referenced it "
                     + (f"restored a snapshot built by {'/'.join(rep['built_by'])}" if logs_ok
                        else ("reused an earlier v" + str(v2) + " snapshot" if reused else "restored a snapshot not seen before")))
    # The same version on two endpoints running different images: each snapshot build resolves the image tag again.
    first_build: dict[str, set] = {}
    drift = []
    for st in steps.values():
        for q, r in st["endpoints"].items():
            for m in r["markers"]:
                first_build.setdefault(m, set(r["builds"]))
                if set(r["builds"]) != first_build[m]:
                    drift.append(f"{q} on the version with marker {m} ran {'/'.join(r['builds'])} "
                                 f"(first seen with {'/'.join(sorted(first_build[m]))})")
    out["same_version_different_image"] = drift
    if drift:
        parts.append("**the same runtime version ran different images on different endpoints**: " + "; ".join(drift)
                     + ". The version stores the image tag, and every snapshot build (a new endpoint, an endpoint update) "
                     "resolves that tag again, so with a mutable tag re-pointing an endpoint at an old version does not "
                     "bring back the old image")
    if per_endpoint:
        lead = ("**The snapshot is per endpoint and version, not just per version.** Each endpoint restores the version it "
                "points to, but an endpoint gets its own snapshot build (minutes) even when another endpoint already has "
                "that version: ")
    elif same_version and len(shared) == len(same_version):
        lead = "**One snapshot per version, shared by the endpoints that point at it.** "
    else:
        lead = ""
    out["snapshot_scope"] = "endpoint+version" if per_endpoint else ("version" if shared else "unclear")
    out["answer"] = (lead + "; ".join(parts) + ".") if parts else "Not enough data."
    return out


# --------------------------------------------------------------------------- Q6
def q6_pricing(data: RunData) -> dict:
    runtimes = data.state.get("runtimes", {})
    arn_to_key = {rt["arn"]: k for k, rt in runtimes.items() if rt.get("arn")}
    name_to_key = {rt["name"]: k for k, rt in runtimes.items() if rt.get("name")}
    windows = []
    for key, ops in data.ops.items():
        for o in ops:
            if o.get("outcome") != "READY" or not o.get("t_done"):
                continue
            boots = data.snapshot_builds(o).get("boots")
            is_v2_runtime_op = (o.get("platform_after") or o.get("platform")) == "V2" and not o["action"].startswith("endpoint_")
            if not (boots or is_v2_runtime_op):
                continue
            busy = any(s["rt"] == key and s["client_send_wall"] <= o["t_done"] and s["client_recv_wall"] >= o["t_start"] - 600
                       for s in data.invokes)
            windows.append({"rt": key, "action": o["action"], "start": o["t_start"], "end": o["t_done"],
                            "minutes": round((o["t_done"] - o["t_start"]) / 60, 1), "sessions_nearby": busy,
                            "boots": data.snapshot_builds(o).get("boots")})
    last_record = max((_usage_time(r) for r in data.usage), default=0)
    for w in windows:
        w["usage_delivered"] = w["end"] <= last_record   # USAGE_LOGS arrive 10-25 min late
    quiet = [w for w in windows if not w["sessions_nearby"]]

    ours = {s["session"] for s in data.invokes}
    last_reply, first_call = defaultdict(float), {}
    for s in data.invokes:
        last_reply[s["session"]] = max(last_reply[s["session"]], s["client_recv_wall"])
        first_call[s["session"]] = min(first_call.get(s["session"], s["client_send_wall"]), s["client_send_wall"])
    per = defaultdict(lambda: {"records": 0, "sessions": set(), "vcpu_h": 0.0, "gb_h": 0.0, "no_session": 0,
                               "not_our_session": 0, "in_builds": 0})
    by_session: dict = defaultdict(lambda: {"seconds": 0.0, "gb_h": 0.0, "vcpu_h": 0.0, "peak_gb": 0.0, "first": None, "last": None,
                                            "samples": []})
    for rec in data.usage:
        key = arn_to_key.get(_get(rec, "resource_arn") or "") or name_to_key.get(_get(rec, "agent.name") or "") or "?"
        row = per[key]
        row["records"] += 1
        sid = _get(rec, "session.id")
        secs_ = float(_get(rec, "time_elapsed_seconds") or _get(rec, "elapsed_time_seconds") or 1)
        gb_h = float(_get(rec, "agent.runtime.memory.gb_hours.used") or 0)
        vcpu_h = float(_get(rec, "agent.runtime.vcpu.hours.used") or 0)
        ts = _usage_time(rec)
        if sid:
            row["sessions"].add(sid)
            row["not_our_session"] += sid not in ours
            b = by_session[sid]
            b["seconds"] += secs_
            b["gb_h"] += gb_h
            b["vcpu_h"] += vcpu_h
            b["peak_gb"] = max(b["peak_gb"], gb_h * 3600 / secs_)
            b["first"] = ts if b["first"] is None else min(b["first"], ts)
            b["last"] = ts if b["last"] is None else max(b["last"], ts)
            b["samples"].append((ts, gb_h * 3600 / secs_, vcpu_h * 3600 / secs_))
            b["platform"] = runtimes.get(key, {}).get("platform")
            # V1 meters a pool instance's boot under the session that later claims it, timestamped at boot.
            before = sid in first_call and ts < first_call[sid] - 2
            b["pre_session_seconds"] = b.get("pre_session_seconds", 0.0) + (secs_ if before else 0)
            b["in_session_seconds"] = b.get("in_session_seconds", 0.0) + (0 if before else secs_)
            b["pre_session_lead_s"] = max(b.get("pre_session_lead_s", 0.0), first_call[sid] - ts if before else 0)
        else:
            row["no_session"] += 1
        row["vcpu_h"] += vcpu_h
        row["gb_h"] += gb_h
        row["in_builds"] += any(w["rt"] == key and w["start"] <= ts <= w["end"] for w in quiet)
    usage = {}
    for key, row in per.items():
        p = PRICES.get(runtimes.get(key, {}).get("platform", "V2"), PRICES["V2"])
        usage[key] = {**row, "sessions": len(row["sessions"]), "vcpu_h": round(row["vcpu_h"], 6), "gb_h": round(row["gb_h"], 6),
                      "est_usd": round(row["vcpu_h"] * p["vcpu_h"] + row["gb_h"] * p["gb_h"], 6)}
    for sid, b in by_session.items():
        b["samples"].sort()
        b["first_second_gb"], b["first_second_vcpu"] = (b["samples"][0][1], b["samples"][0][2]) if b["samples"] else (None, None)
        b["steady_gb"] = pct([g for _, g, _ in b["samples"][1:]], .5)
        in_session = [t for t, _, _ in b["samples"] if sid in first_call and t >= first_call[sid] - 2]
        b["metered_after_last_reply_s"] = (max(in_session) - last_reply[sid]) if in_session and sid in last_reply else None
    per_platform = {}
    for plat in ("V1", "V2"):
        ss = [b for b in by_session.values() if b.get("platform") == plat]
        if ss:
            per_platform[plat] = {
                "sessions": len(ss), "metered_seconds_p50": pct([b["seconds"] for b in ss], .5),
                "first_second_gb_p50": pct([b["first_second_gb"] for b in ss], .5),
                "first_second_vcpu_p50": pct([b["first_second_vcpu"] for b in ss], .5),
                "steady_gb_p50": pct([b["steady_gb"] for b in ss], .5),
                "metered_after_last_reply_s_p50": pct([b["metered_after_last_reply_s"] for b in ss], .5),
                "pre_session_seconds_p50": pct([b.get("pre_session_seconds", 0) for b in ss], .5),
                "pre_session_lead_s_p50": pct([b.get("pre_session_lead_s", 0) for b in ss], .5),
                "in_session_seconds_p50": pct([b.get("in_session_seconds", 0) for b in ss], .5),
                "gb_h_p50": pct([b["gb_h"] for b in ss], .5), "vcpu_h_p50": pct([b["vcpu_h"] for b in ss], .5),
                "usd_p50": pct([b["gb_h"] * PRICES[plat]["gb_h"] + b["vcpu_h"] * PRICES[plat]["vcpu_h"] for b in ss], .5)}
    sessions = list(by_session.values())
    per_session = {"sessions": len(sessions),
                   "metered_seconds_p50": pct([b["seconds"] for b in sessions], .5),
                   "gb_h_p50": pct([b["gb_h"] for b in sessions], .5), "vcpu_h_p50": pct([b["vcpu_h"] for b in sessions], .5),
                   "peak_gb_p50": pct([b["peak_gb"] for b in sessions], .5), "peak_gb_max": max((b["peak_gb"] for b in sessions), default=None),
                   "usd_p50": pct([b["gb_h"] * PRICES["V2"]["gb_h"] + b["vcpu_h"] * PRICES["V2"]["vcpu_h"] for b in sessions], .5)}
    series_found = any(isinstance(v, list) and v for v in data.metrics.values())
    hits = []
    for w in quiet:
        series = data.metrics.get(w["rt"])
        for s in series if isinstance(series, list) else []:
            hits += [{"rt": w["rt"], "action": w["action"], "metric": s["metric"], "t": t, "value": val}
                     for t, val in s["points"] if w["start"] + 60 <= t <= w["end"] - 60 and val > 0]
    out = {"snapshot_builds": windows, "quiet_build_minutes": round(sum(w["minutes"] for w in quiet), 1),
           "usage_records": len(data.usage), "usage": usage, "per_session": per_session, "per_platform": per_platform,
           "last_usage_record": last_record, "metric_series_found": series_found,
           "metric_usage_during_builds": hits, "costs": data.costs, "prices": PRICES, "pricing_url": PRICING_URL}
    parts = []
    if data.usage:
        in_builds = sum(u["in_builds"] for u in usage.values())
        no_session = sum(u["no_session"] for u in usage.values())
        foreign = sum(u["not_our_session"] for u in usage.values())
        covered = sorted(LABELS.get(k, k) for k in usage)
        all_builds = [w for w in windows if w["rt"] in usage and w["usage_delivered"]]
        clean = in_builds == 0 and no_session == 0 and foreign == 0 and bool(all_builds) and "V2" in per_platform
        parts.append(f"{'**Snapshot creation and storage: nothing was metered. Restore: metered, as part of the session.** ' if clean else ''}"
                     f"All {len(data.usage)} per-second usage records AgentCore delivered ({', '.join(covered)}) belong to "
                     f"sessions this PoC opened ({foreign} for any other session, {no_session} with no session)"
                     + (f", although {len(all_builds)} V2 snapshot builds ({round(sum(w['minutes'] for w in all_builds), 1)} "
                        f"min of building) ran on those runtimes within the delivered period" if all_builds else
                        "; no V2 snapshot build is covered by the records delivered so far")
                     + (f"; {in_builds} records fell inside builds with no session nearby" if in_builds else ""))
        v2s = per_platform.get("V2")
        if v2s:
            parts.append(f"Each new V2 session's first metered second (the restore) showed {v2s['first_second_gb_p50']:.2f} GB and "
                         f"{v2s['first_second_vcpu_p50']:.2f} vCPU, then ~{v2s['steady_gb_p50']:.2f} GB while idle (reclaim starts "
                         f"after 120 s idle); a one-call session metered {v2s['metered_seconds_p50']:.0f} s, "
                         f"{v2s['metered_after_last_reply_s_p50']:.0f} s of it after the last reply (StopRuntimeSession was called "
                         f"immediately), ≈ ${v2s['usd_p50']:.6f} per session")
        v1s = per_platform.get("V1")
        if v1s:
            s1 = (f"On V1 the warm-pool instance's boot is metered and billed to the session that later claims it: each V1 "
                  f"session carried {v1s['pre_session_seconds_p50']:.0f} s of records timestamped "
                  f"{secs(v1s['pre_session_lead_s_p50'])} before its first call (~{v1s['steady_gb_p50']:.2f} GB, "
                  f"{v1s['first_second_vcpu_p50']:.1f}+ vCPU while booting), and nothing for the time it sat paused")
            if not v1s["in_session_seconds_p50"]:
                s1 += " (records for the sessions' own active seconds had not been delivered when the run collected them)"
            parts.append(s1)
    else:
        parts.append("USAGE_LOGS records not collected (yet)")
    if series_found:
        parts.append(f"CloudWatch MemoryUsed/CPUUsed showed {len(hits)} non-zero minutes inside those builds")
    if data.costs and data.costs.get("rows"):
        parts.append("Cost Explorer usage types on the run's days: "
                     + ", ".join(sorted({r["usage_type"] for r in data.costs["rows"]})))
    else:
        parts.append("Cost Explorer not checked yet (run `poc.py pricing` a day later)")
    parts.append(f"The pricing page has no snapshot line item, and V2 bills at its own rates (${PRICES['V2']['vcpu_h']}/vCPU-h, "
                 f"${PRICES['V2']['gb_h']}/GB-h vs V1 ${PRICES['V1']['vcpu_h']}, ${PRICES['V1']['gb_h']}). This is evidence of "
                 f"what was metered, not a pricing commitment")
    out["answer"] = ". ".join(parts) + "."
    return out


def _usage_time(rec: dict) -> float:
    ts = _get(rec, "event_timestamp")
    if isinstance(ts, (int, float)):
        return ts / 1000 if ts > 1e11 else float(ts)
    if isinstance(ts, str):
        try:
            return dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
        except ValueError:
            pass
    return rec.get("timestamp", 0)


# --------------------------------------------------------------------------- extras
def extras(data: RunData) -> dict:
    out: dict = {}
    env = data.kind("extra_env_limit")
    if env:
        out["env_limit"] = env
    anon = data.kind("extra_anonymous_invoke")
    if anon:
        out["unsigned_invoke"] = anon[-1]
    per_rt = {k: data.cold("q1", k) for k in ("v2_public", "v2_vpc") if data.cold("q1", k)}
    if per_rt:
        v2 = [s for ss in per_rt.values() for s in ss]
        out["shared_startup_state"] = {
            "sessions": len(v2), "distinct_import_time_uuids": {LABELS[k]: len(tokens(ss)) for k, ss in per_rt.items()},
            "hostnames": sorted({str(d(s).get("hostname")) for s in v2}), "pids": sorted({str(d(s).get("pid")) for s in v2}),
            "distinct_boot_ids": len({d(s).get("boot_id") for s in v2})}
    boots = {k: data.snapshot_builds(data.op(k, "create")).get("boots") for k in data.ops if data.op(k, "create")}
    out["startup_code_runs_per_v2_build"] = {k: v for k, v in boots.items() if k.startswith("v2")}
    out["ready_times"] = [{"rt": o["rt"], "action": o["action"], "platform": o.get("platform_after") or o.get("platform"),
                           "outcome": o.get("outcome"), "seconds": o.get("seconds")}
                          for ops in data.ops.values() for o in ops if not o["action"].startswith("endpoint_")]
    out["timeline"] = build_timeline(data)
    return out


# --------------------------------------------------------------------------- output
def analyze(run_dir: Path) -> dict:
    data = RunData(run_dir)
    _CURRENT[0] = data
    return {"run": data.state.get("run"), "region": data.state.get("region"), "account": data.state.get("account"),
            "started": data.state.get("started"), "samples": (data.state.get("args") or {}).get("samples"),
            "q1_vpc": q1_vpc(data), "q2_prestart": q2_prestart(data), "q3_rollback": q3_rollback(data),
            "q4_same_tag": q4_same_tag(data), "q5_endpoints": q5_endpoints(data), "q6_pricing": q6_pricing(data),
            "extras": extras(data), "evidence": _json(run_dir / "evidence.json", {})}


def write(run_dir: Path) -> dict:
    res = analyze(run_dir)
    (run_dir / "results.json").write_text(json.dumps(res, indent=1, default=str))
    (run_dir / "FINDINGS.md").write_text(markdown(res))
    return res


def _table(header: list[str], rows: list[list]) -> list[str]:
    return ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)] + \
           ["| " + " | ".join("–" if c is None or c == "" else str(c) for c in r) + " |" for r in rows]


def _fmt_started(c: dict) -> str:
    return ", ".join(f"{v} {k}" for k, v in c.items()) or "–"


def markdown(res: dict) -> str:
    when = (dt.datetime.fromtimestamp(res["started"], dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            if res.get("started") else "?")
    L = ["# AgentCore Runtime V2: six questions, measured", "",
         f"Run `{res['run']}` · Region `{res['region']}` · {when} · {res.get('samples')} new sessions per runtime for the "
         f"cold-start comparison. Every number below comes from this run's API calls, invocations, the agent's own "
         f"logs and CloudTrail (raw data in this directory).", "", "## Answers", ""]
    L += _table(["#", "Question", "Measured answer"],
                [[i, q, res[k].get("answer", "–")] for i, (k, q) in enumerate(QUESTIONS, 1)])

    q1 = res["q1_vpc"]["runtimes"]
    L += ["", "## 1. VPC mode", ""]
    if q1:
        L += _table(["Runtime", "Create → READY", "Sessions OK", "Cold invoke p50 / p75 / p90", "Warm p50",
                     "Start-up overhead p50", f"First read of a {next(iter(q1.values()))['file_mb']} MiB image file: min / p50 / max",
                     "Second read p50"],
                    [[LABELS[k], secs(r["create_s"]), f"{r['ok']}/{r['sessions']}",
                      f"{ms(r['cold_ms']['p50'])} / {ms(r['cold_ms']['p75'])} / {ms(r['cold_ms']['p90'])}",
                      ms(r["warm_p50_ms"]), ms(r["startup_overhead_p50_ms"]),
                      " / ".join(ms(v) for v in (min(r["first_read_ms"], default=None), r["first_read_p50_ms"],
                                                  max(r["first_read_ms"], default=None))),
                      ms(r["second_read_p50_ms"])] for k, r in q1.items()])
        L += ["", "*Cold invoke*: InvokeAgentRuntime with a new session id, timed by the client (includes the round trip "
              "to the Region). *Start-up overhead*: cold minus the next call on the same session. *File read*: a full read of "
              "a file baked into the image, on the session's third and fourth calls.", ""]
        for k, r in q1.items():
            if r.get("egress"):
                L.append(f"- Network seen from inside **{LABELS[k]}**: " + "; ".join(
                    f"{e['target']} {'reachable' if e.get('reachable') else 'unreachable'}"
                    + (f" (DNS: {'private' if e.get('private') else 'public'} IPs)" if e.get("ips") else "") for e in r["egress"]))
    q2 = res["q2_prestart"]["runtimes"]
    L += ["", "## 2. Pre-started instances", ""]
    if q2:
        L += _table(["Runtime", "Sessions", "How each session's instance started", "Distinct startup tokens",
                     "Instance age at 1st request p50 (min)", "Ran before freeze p50", "Resumed/restored before 1st request p50 / max",
                     "Before 1st session: boots / restores", "Snapshot build: boots / test restores"],
                    [[LABELS[k], r["sessions"], _fmt_started(r["started"]), r["distinct_startup_tokens"],
                      f"{secs(r['process_age_at_first_request_s']['p50'])} ({secs(r['process_age_at_first_request_s']['min'])})",
                      _s(r["ran_before_freeze_s"]["p50"]),
                      f"{_s(r['resumed_before_first_request_s']['p50'])} / {_s(r['resumed_before_first_request_s']['max'])}",
                      f"{r['boots_before_first_session']} / {r['restores_before_first_session']}" if r["logs_collected"] else "logs n/a",
                      f"{r['snapshot_build_boots']} / {r['snapshot_build_test_restores']}" if r["logs_collected"] else "logs n/a"]
                     for k, r in q2.items()])
        quiet = next((r["quiet_window_s"] for r in q2.values() if r.get("quiet_window_s")), None)
        L += ["", "*Instance age*: wall-clock time from the agent process starting to the session's first request. *Ran before "
              "freeze*: how long that process ran before it was frozen (V1: paused once healthy; V2: snapshotted). "
              "*Resumed/restored before 1st request*: how long the instance had been running again when the request arrived. "
              f"*Before 1st session*: processes booted or restored outside a snapshot build before the runtime's first "
              f"invocation (after READY there were {quiet} s with no traffic at all).", ""]
    tl = res["extras"].get("timeline")
    if tl:
        L += ["How a V2 create unfolded (seconds after CreateAgentRuntime):", ""]
        L += _table(["Runtime", "Image pulls", "Agent booted", "First ping after boot", "Snapshot taken", "Test restore",
                     "READY", "First session"],
                    [[LABELS.get(t["rt"], t["rt"]), _lst(t["image_pulls_s"]), _lst(t["boots_s"]), _lst(t["first_ping_after_boot_s"]),
                      _lst(t["snapshot_taken_s"]), _lst(t["validation_restores_s"]), t["ready_s"], t["first_session_s"]]
                     for t in tl])
        L.append("")

    q3 = res["q3_rollback"]
    L += ["## 3. Rollback", ""]
    if q3.get("steps"):
        L += _table(["Step", "Sessions", "Started as", "POC_MARKER", "Distinct startup tokens", "Cold invoke p50", "Frozen p50"],
                    [[s["step"], s["sessions"], _fmt_started(s["started"]), ", ".join(s["markers"]), s["distinct_startup_tokens"],
                      ms(s.get("cold_ms_p50")), _s(s["frozen_s_p50"])] for s in q3["steps"]])
        L += ["", f"- V2→V1 update: {_op_line(q3.get('rollback'))}", f"- V1→V2 update: {_op_line(q3.get('rollforward'))}",
              "- `GetAgentRuntime.platformVersion`: " + ", ".join(f"{k.replace('_', ' ')} = {v}" for k, v in
                                                                  (q3.get("platform_reported") or {}).items()),
              f"- Session opened before the first switch kept its instance across {q3.get('inflight_calls')} later calls: "
              f"{q3.get('inflight_session_kept_instance')}",
              "- Versions: " + ", ".join(f"v{v['version']} {v.get('platform')} {v.get('marker')}" for v in q3.get("versions", [])), ""]
    q4 = res["q4_same_tag"]
    L += ["## 4. Same-tag updates", ""]
    if q4.get("runtimes"):
        L += [f"All runtimes were created from tag `{q4['tag']}` → build `{q4['build_a']}`. The tag was then moved to build "
              f"`{q4['build_b']}`, and each runtime was updated twice with its configuration unchanged.", ""]
        L += _table(["Runtime", "Build before", "Tag moved, no update", "Same-tag update", "Build after", "No-op update", "Build after"],
                    [[LABELS[k], ", ".join(r["before"]), ", ".join(r["tag_moved_no_update"]), _op_line(r["same_tag_update"]),
                      ", ".join(r["after_update"]), _op_line(r["noop_update"]), ", ".join(r["after_noop"])]
                     for k, r in q4["runtimes"].items()])
        L.append("")
    q5 = res["q5_endpoints"]
    L += ["## 5. Endpoints", ""]
    if q5.get("steps"):
        rows = []
        for st in q5["steps"].values():
            for q, r in st["endpoints"].items():
                rows.append([st["label"], q, r["sessions"], ", ".join(r["markers"]), ", ".join(r.get("builds", [])),
                             ", ".join(r["snapshots"]), ", ".join(r["built_by"]), _fmt_started(r["started"])])
        L += _table(["Configuration", "Endpoint", "Sessions", "POC_MARKER (version)", "Image build", "Snapshot (startup token)",
                     "Snapshot built by", "Started as"], rows)
        L += ["", "Lifecycle steps on this runtime:", ""]
        L += _table(["Step", "Version", "Outcome", "Time", "Snapshot builds (boots)", "Builder sessions", "Pulled"],
                    [[r["action"], r.get("version"), r.get("outcome"), secs(r.get("seconds")), r.get("boots"),
                      ", ".join(r.get("builder_sessions", [])), ", ".join(f"{k} ×{v}" for k, v in (r.get("pulled") or {}).items())]
                     for r in q5["ops"]])
        L.append("")
    q6 = res["q6_pricing"]
    L += ["## 6. Pricing", ""]
    if q6.get("snapshot_builds"):
        L += _table(["Runtime", "Step", "Build minutes", "Snapshot boots", "Sessions nearby", "Usage delivered for it"],
                    [[LABELS.get(w["rt"], w["rt"]), w["action"], w["minutes"], w["boots"], w["sessions_nearby"], w["usage_delivered"]]
                     for w in q6["snapshot_builds"]])
        L.append("")
    if q6.get("usage"):
        L += _table(["Runtime", "Usage records", "Sessions", "Records for sessions not opened by the PoC", "Records with no session",
                     "Records inside quiet builds", "vCPU-h", "GB-h", "≈ USD"],
                    [[LABELS.get(k, k), u["records"], u["sessions"], u["not_our_session"], u["no_session"], u["in_builds"],
                      u["vcpu_h"], u["gb_h"], u["est_usd"]] for k, u in q6["usage"].items()])
        if q6.get("per_platform"):
            L += [""] + _table(["Platform", "Sessions", "Metered s p50", "…before the 1st call (lead)", "…from the 1st call",
                                "Metered after last reply", "1st metered second GB / vCPU", "Then GB p50", "GB-h p50",
                                "vCPU-h p50", "≈ USD p50"],
                               [[plat, v["sessions"], _n(v["metered_seconds_p50"], 0),
                                 f"{_n(v['pre_session_seconds_p50'], 0)} s ({secs(v['pre_session_lead_s_p50'])} earlier)",
                                 f"{_n(v['in_session_seconds_p50'], 0)} s",
                                 (_n(v["metered_after_last_reply_s_p50"], 0) + " s") if v["metered_after_last_reply_s_p50"] is not None else "–",
                                 f"{_n(v['first_second_gb_p50'])} / {_n(v['first_second_vcpu_p50'])}", _n(v["steady_gb_p50"]),
                                 _n(v["gb_h_p50"], 6), _n(v["vcpu_h_p50"], 6), _n(v["usd_p50"], 6)]
                                for plat, v in q6["per_platform"].items()])
        L.append("")
    if q6.get("costs") and q6["costs"].get("rows"):
        L += _table(["Date", "Usage type", "Quantity", "Unit", "USD"],
                    [[r["date"], r["usage_type"], round(r["quantity"], 6), r["unit"], round(r["cost_usd"], 4)] for r in q6["costs"]["rows"]])
        L.append("")
    ex = res["extras"]
    L += ["## Also measured", ""]
    for e in ex.get("env_limit", []):
        size = f"{e['bytes']} bytes" if e.get("bytes") else "~3 KB"
        if e.get("accepted") is False or e.get("rejected"):
            what = f"rejected by CreateAgentRuntime: {e.get('error')}"
        elif e.get("status"):
            what = f"accepted, and the runtime then ended {e['status']}" + (f" ({e['failure_reason']})" if e.get("failure_reason") else "")
        else:
            what = "accepted by CreateAgentRuntime (the runtime was deleted before it finished building)"
        L.append(f"- **V2 environment variables, {size}** (docs: 2.5 KB cap for container agents on V2, 4 KB on V1): {what}")
    if ex.get("shared_startup_state"):
        s = ex["shared_startup_state"]
        L.append(f"- **Startup state is shared**: across {s['sessions']} V2 sessions, a uuid4() drawn at import took only "
                 + ", ".join(f"{n} distinct values on {k}" for k, n in s["distinct_import_time_uuids"].items())
                 + f" (one per snapshot copy); every session reported hostname {', '.join(s['hostnames'])} and PID "
                 f"{', '.join(s['pids'])}")
    if ex.get("startup_code_runs_per_v2_build"):
        L.append(f"- **Startup code runs more than once per V2 version**: processes booted per create build: "
                 f"{ex['startup_code_runs_per_v2_build']} (plus a test restore of each)")
    if ex.get("unsigned_invoke"):
        u = ex["unsigned_invoke"]
        L.append(f"- **Inbound auth**: an unsigned InvokeAgentRuntime was refused with HTTP {u.get('status')} "
                 f"{(u.get('error_type') or '').split(':')[0]}")
    if ex.get("ready_times"):
        L.append("- **Create/update → READY**: " + "; ".join(
            f"{LABELS.get(r['rt'], r['rt'])} {r['action']} ({r['platform']}) {secs(r['seconds'])}" for r in ex["ready_times"]
            if r.get("seconds") is not None))
    L += ["", "## Method", "",
          "- One probe image: python:3.12-slim + a standard-library HTTP agent + a fixed 32 MiB file, assembled and pushed by "
          "`image.py` (no Docker). Builds A and B differ only in a build id.",
          "- The agent recognizes a V2 restore from inside the process. Monotonic time does not advance across a restore, so the "
          "gap between wall-clock and monotonic time jumps, and the agent records when. It also draws a random token at import, "
          "which a snapshot freezes: sessions restored from the same snapshot report the same token.",
          "- Each runtime has its own execution role, so CloudTrail's ECR pull events name the runtime and the step that pulled "
          "(`snapstart-build-<endpoint>` for snapshot builds).",
          "- Runtimes use IAM (SigV4) inbound auth. Sessions are stopped after use; idle timeout is 10 min.", ""]
    return "\n".join(L)


def _n(v, digits: int = 2) -> str:
    return "–" if v is None else f"{v:.{digits}f}"


def _s(v) -> str:
    return "–" if v is None else f"{v:.2f} s"


def _lst(v) -> str:
    return ", ".join(str(x) for x in v) if v else "–"


def _op_line(op: dict | None) -> str:
    if not op:
        return "–"
    s = f"{op.get('outcome')} v{op.get('version')} in {secs(op.get('seconds'))}"
    if op.get("boots"):
        s += f", {_plural(op['boots'], 'snapshot boot')}"
    if op.get("pulled"):
        s += f", pulled {', '.join(f'{k} ×{v}' for k, v in op['pulled'].items())}"
    if op.get("failure") and op.get("outcome") != "READY":
        s += f": {op['failure']}"
    return s
