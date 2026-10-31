"""Turn a run directory into timeseries.csv, chart.png, findings.json and FINDINGS.md.

Works purely from files in the run directory, so `benchmark.py --reanalyze` can rebuild
everything after late CloudWatch data lands.
"""

from __future__ import annotations

import datetime as dt
import json
import os
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter  # noqa: E402

from cdp_signals import decode_exit_status  # noqa: E402

PRICE_GB_HOUR = float(os.environ.get("AGENTCORE_BROWSER_GB_HOUR_USD", "0.00945"))
PRICE_VCPU_HOUR = float(os.environ.get("AGENTCORE_BROWSER_VCPU_HOUR_USD", "0.0895"))
MIN_BILLED_GB = 0.125  # 128 MB billing floor
NOMINAL_CEILING_MB = 4096.0

CSV_COLUMNS = [
    # requested columns
    "timestamp", "cumulative_alloc_mb", "cloudwatch_mem_mb", "cloudwatch_cpu", "cdp_heap_mb",
    "probe_latency_ms", "event",
    # context
    "elapsed_s", "phase", "event_detail", "alloc_step_ms",
    "cdp_js_heap_used_mb", "cdp_backing_store_mb",
    "browser_pmf_mb", "max_renderer_pmf_mb", "pmf_renderers_mb", "pmf_browser_process_mb",
    "host_mem_total_mb", "host_mem_inuse_excl_cache_mb", "host_mem_used_mb", "host_mem_free_mb",
    "host_mem_avail_est_mb", "host_anon_mb", "host_file_mb", "host_swap_cached_mb",
    "host_disk_read_mb_s", "host_disk_write_mb_s",
    "probe_browser_ms", "host_probe_ms", "probe_main_thread_busy_pct",
    "chrome_cpu_pct", "alloc_renderer_cpu_pct", "n_renderers",
    "usage_log_mem_mb", "usage_log_vcpu", "session_status",
]

MODE_TEXT = {
    "oom_kill_inferred": "OOM kill of the tab (inferred: the VM was out of memory and the tab's process died, "
                         "but the automation stream dropped too, so Chrome reported no exit code)",
    "oom_kill": "OOM kill: the tab's process was SIGKILLed while the VM was out of memory",
    "v8_heap_oom": "the tab crashed at Chrome's own JavaScript heap limit (V8 out of memory)",
    "browser_restart": "the VM stalled, the CDP connection dropped and Chrome came back as a new process with "
                       "every tab lost",
    "connection_drop_oom": "the VM stalled and the CDP connection dropped while out of memory (whether Chrome "
                           "itself restarted was not checked in this run)",
    "chrome_oom": "Chrome out-of-memory termination of the renderer",
    "renderer_crash": "renderer crash",
    "connection_drop": "CDP WebSocket connection dropped",
    "session_terminated": "session terminated by the service",
    "allocation_refused": "allocation refused (no process was killed)",
    "hang": "allocation step hung past its timeout",
    "page_closed": "page closed unexpectedly",
    "no_failure": "no failure up to the configured limit",
    "interrupted": "run interrupted before any failure",
    "runner_error": "benchmark runner error before any failure",
}

MODE_SHORT = {
    "oom_kill": "OOM kill of the tab", "chrome_oom": "Chrome OOM termination",
    "v8_heap_oom": "Tab crashed at Chrome's JS heap limit", "browser_restart": "Chrome restarted, all tabs lost",
    "connection_drop_oom": "Connection dropped at the ceiling (restart not checked)",
    "renderer_crash": "Renderer crash", "connection_drop": "CDP connection dropped",
    "session_terminated": "Session terminated by the service", "allocation_refused": "Allocation refused",
    "hang": "Allocation hung", "page_closed": "Page closed unexpectedly",
    "no_failure": "No failure up to the configured limit", "interrupted": "Run interrupted",
    "runner_error": "Runner error",
}

# Chart palette: validated categorical slots (light), fixed per signal so colors mean the same
# thing in every run. Status red is reserved for the failure marker.
SURFACE, INK, INK2, MUTED, GRID, AXIS, CRITICAL = (
    "#fcfcfb", "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7", "#d03b3b")
SLOT = {"alloc": "#2a78d6", "pmf": "#eb6834", "vm": "#1baf7a", "aws": "#eda100",
        "probe": "#e87ba4", "browser": "#008300", "step": "#6250d6"}


# --------------------------------------------------------------------------- loading
def load_rows(run_dir: Path) -> list[dict]:
    rows = []
    path = run_dir / "samples.jsonl"
    if path.exists():
        for line in path.read_text().splitlines():
            try:
                rows.append(json.loads(line))
            except ValueError:
                pass  # a killed run can leave a truncated last line
    return rows


def _frame(rows: list[dict], kind: str) -> pd.DataFrame:
    df = pd.DataFrame([r for r in rows if r.get("kind") == kind])
    if df.empty:
        return df
    df = df.sort_values("t").reset_index(drop=True)
    if kind == "host" and {"host_mem_total_mb", "host_mem_avail_est_mb"} <= set(df.columns):
        # What the VM cannot hand back without reclaim: total minus what is still available.
        df["host_mem_inuse_excl_cache_mb"] = df["host_mem_total_mb"] - df["host_mem_avail_est_mb"]
    if kind == "host" and "after_failure" not in df.columns:
        df["after_failure"] = 0
    return df


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


def _epoch(iso: str | None) -> float | None:
    return dt.datetime.fromisoformat(iso).timestamp() if iso else None


def _asof(base: pd.DataFrame, src: pd.DataFrame, cols: list[str], tol: float | None) -> None:
    """Attach the latest non-null value of each column at or before every base timestamp."""
    for col in cols:
        if src.empty or col not in src.columns:
            base[col] = np.nan
            continue
        s = src[["t", col]].dropna(subset=[col]).sort_values("t")
        if s.empty:
            base[col] = np.nan
            continue
        merged = pd.merge_asof(base[["t"]], s, on="t", direction="backward", tolerance=tol)
        base[col] = merged[col].to_numpy()


def _iso(t: float) -> str:
    return dt.datetime.fromtimestamp(t, dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# --------------------------------------------------------------------------- time series
def build_timeseries(rows, meta, cw_doc, usage) -> tuple[pd.DataFrame, pd.DataFrame]:
    args = meta.get("args", {})
    sample_s = float(args.get("sample_interval_s", 2.0))
    memdump_s = float(args.get("memdump_interval_s", 6.0)) or sample_s
    host_s = float(args.get("host_interval_s", 5.0)) or sample_s
    health_s = float(args.get("health_interval_s", 15.0))
    period_s = float(args.get("cw_period_s", 60))

    times = [r["t"] for r in rows if "t" in r]
    if not times:
        # A run that died before recording a single sample still has to produce a report.
        return pd.DataFrame(columns=["t"] + CSV_COLUMNS), pd.DataFrame()
    t_origin = _epoch(meta.get("session_created_at")) or min(times)
    t0, t1 = min(times), max(times)

    events = _frame(rows, "event")
    grid = pd.DataFrame({"t": np.arange(t0, t1 + sample_s / 2, sample_s)})
    grid["event"], grid["event_detail"] = "", ""
    if not events.empty:
        ev = pd.DataFrame({
            "t": events["t"],
            "event": events["event"],
            "event_detail": [json.dumps({k: v for k, v in (d or {}).items() if v is not None},
                                        default=str)[:300] for d in events.get("detail", [None] * len(events))],
        })
        base = pd.concat([grid, ev], ignore_index=True).sort_values("t", kind="stable").reset_index(drop=True)
    else:
        base = grid

    alloc = _frame(rows, "alloc")
    if not alloc.empty and "ok" in alloc.columns:
        alloc = alloc[alloc["ok"] == True]  # noqa: E712
    page = _frame(rows, "page")
    if not page.empty and "main_task_s" in page.columns:
        p = page.dropna(subset=["main_task_s"]).copy()
        same_step = p["steps_started"].eq(p["steps_started"].shift()) if "steps_started" in p else True
        busy = p["main_task_s"].diff() / p["t"].diff() * 100.0
        p["probe_main_thread_busy_pct"] = busy.where(same_step).clip(lower=0, upper=100)
        page = page.merge(p[["t", "probe_main_thread_busy_pct"]], on="t", how="left")
    browser = _frame(rows, "browser")
    host = _frame(rows, "host")
    if not host.empty:
        host = host[host["after_failure"] != 1]
    session = _frame(rows, "session")
    phase = _frame(rows, "phase")

    _asof(base, alloc, ["cumulative_alloc_mb"], None)
    _asof(base, alloc, ["alloc_step_ms"], sample_s)
    _asof(base, page, ["probe_latency_ms"], sample_s * 1.6)
    _asof(base, page, ["cdp_heap_mb", "cdp_js_heap_used_mb", "cdp_backing_store_mb"], sample_s * 3)
    _asof(base, page, ["probe_main_thread_busy_pct"], sample_s * 2)
    _asof(base, browser, ["probe_browser_ms"], sample_s * 1.6)
    _asof(base, browser, ["chrome_cpu_pct", "alloc_renderer_cpu_pct", "n_renderers"], sample_s * 2)
    _asof(base, browser, ["browser_pmf_mb", "max_renderer_pmf_mb", "pmf_renderers_mb",
                          "pmf_browser_process_mb"], memdump_s * 2.5)
    _asof(base, host, ["host_mem_total_mb", "host_mem_inuse_excl_cache_mb", "host_mem_used_mb",
                       "host_mem_free_mb", "host_mem_avail_est_mb", "host_anon_mb", "host_file_mb",
                       "host_swap_cached_mb", "host_disk_read_mb_s", "host_disk_write_mb_s"], host_s * 2.5)
    _asof(base, host, ["host_probe_ms"], host_s * 1.6)
    _asof(base, session, ["session_status"], health_s * 2.5)
    _asof(base, phase, ["phase"], None)

    cw_series = (cw_doc or {}).get("series", {})
    for signal, col in (("memory", "cloudwatch_mem_mb"), ("cpu", "cloudwatch_cpu")):
        pts = cw_series.get(signal) or {}
        src = pd.DataFrame({"t": [_epoch(k) for k in pts], col: list(pts.values())})
        _asof(base, src, [col], period_s - 1e-3)
    usage_df = pd.DataFrame(usage) if usage else pd.DataFrame()
    if not usage_df.empty:
        usage_df = usage_df.dropna(subset=["t"]).sort_values("t")
    _asof(base, usage_df, ["usage_log_mem_mb", "usage_log_vcpu"], 1.5)

    base["phase"] = base["phase"].fillna("setup")
    base["timestamp"] = [_iso(t) for t in base["t"]]
    base["elapsed_s"] = (base["t"] - t_origin).round(1)
    for col in CSV_COLUMNS:
        if col not in base.columns:
            base[col] = np.nan
    return base[["t"] + CSV_COLUMNS], events


# --------------------------------------------------------------------------- analysis helpers
def _vals(df: pd.DataFrame, col: str, t_from=None, t_to=None, mask=None) -> pd.Series:
    if df.empty or col not in df.columns:
        return pd.Series(dtype=float)
    sel = df
    if t_from is not None:
        sel = sel[sel["t"] >= t_from]
    if t_to is not None:
        sel = sel[sel["t"] < t_to]
    if mask is not None:
        sel = sel[mask.reindex(sel.index, fill_value=False)]
    return pd.to_numeric(sel[col], errors="coerce").dropna()


def _q(s: pd.Series, q: float) -> float | None:
    return float(s.quantile(q)) if len(s) else None


def _last(df: pd.DataFrame, col: str, t_to: float, window_s: float = 30.0):
    s = _vals(df, col, t_to - window_s, t_to + 1e-6)
    return float(s.iloc[-1]) if len(s) else None


def fmt(v, unit: str = "", digits: int = 0) -> str:
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "n/a"
    return f"{v:,.{digits}f}{(' ' + unit) if unit else ''}"


def _ratio(a, b):
    return a / b if a is not None and b not in (None, 0) else None


# --------------------------------------------------------------------------- analysis
def analyze(run_dir: Path, rows, ts, meta, cw_doc, usage) -> dict:
    args = meta.get("args", {})
    env = next((r for r in rows if r.get("kind") == "env"), {})
    caps = next((r for r in rows if r.get("kind") == "host_caps"), {})
    page, browser, host = _frame(rows, "page"), _frame(rows, "browser"), _frame(rows, "host")
    live_host = host[host["after_failure"] != 1] if not host.empty else host
    alloc_rows = _frame(rows, "alloc")
    phases = _frame(rows, "phase")
    ev_list = [r for r in rows if r.get("kind") == "event"]
    names = [e["event"] for e in ev_list]
    t_created = _epoch(meta.get("session_created_at"))
    t_stopped = _epoch(meta.get("session_stopped_at"))

    def phase_start(name):
        hit = phases[phases["phase"] == name]["t"] if not phases.empty else pd.Series(dtype=float)
        return float(hit.iloc[0]) if len(hit) else None

    t_ramp = phase_start("ramp")
    t_base = phase_start("baseline")
    terminal = next((e for e in ev_list if (e.get("detail") or {}).get("terminal")), None)
    t_fail = terminal["t"] if terminal else None
    ok_alloc_all = alloc_rows[alloc_rows["ok"] == True] if not alloc_rows.empty else alloc_rows  # noqa: E712
    # A step already in flight can finish after the terminal event; counting it as a successful
    # pre-failure step would overstate what the session held when it died.
    ok_alloc = ok_alloc_all[ok_alloc_all["t"] <= t_fail] if (t_fail and len(ok_alloc_all)) else ok_alloc_all
    t_ramp_end = t_fail or (float(ok_alloc_all["t"].iloc[-1]) if len(ok_alloc_all) else None)
    mem_total = caps.get("mem_total_mb") or _q(_vals(live_host, "host_mem_total_mb"), 0.5)

    # ---- failure mode
    mode, crash, vanished, symptom = "no_failure", None, [], None
    if terminal is None:
        if {"interrupted", "aborted"} & set(names):
            mode = "interrupted"
        elif "runner_error" in names:
            mode = "runner_error"
    else:
        d = terminal["detail"]
        page_label = d.get("page")
        near = [e for e in ev_list if e["event"] == "target_crashed" and abs(e["t"] - t_fail) <= 10
                and (page_label is None or e["detail"].get("page") == page_label)]
        crash = near[0]["detail"] if near else None
        status = d.get("status") or (crash or {}).get("status")
        code = d.get("error_code") if d.get("error_code") is not None else (crash or {}).get("error_code")
        exit_status = decode_exit_status(code)
        vanished = [e["detail"] for e in ev_list if e["event"] == "process_vanished" and abs(e["t"] - t_fail) <= 15]
        name = terminal["event"]
        if name == "page_crash":
            js_heap = _last(ts, "cdp_js_heap_used_mb", t_fail, 180.0)
            heap_limit = env.get("jsHeapSizeLimitMb")
            if (exit_status or "").startswith("SIGKILL") or status == "killed":
                mode = "oom_kill"
            elif status == "oom":
                mode = "chrome_oom"
            elif status == "crashed" and js_heap and heap_limit and js_heap >= 0.8 * heap_limit:
                # V8 aborts the renderer itself (an immediate crash, seen as SIGTRAP) once the JavaScript
                # heap cannot grow: Chrome's own per-tab limit, reached before the VM's.
                mode = "v8_heap_oom"
            else:
                mode = "renderer_crash"
        else:
            mode = {"cdp_disconnected": "connection_drop", "session_unhealthy": "session_terminated",
                    "alloc_failed": "allocation_refused", "alloc_step_timeout": "hang",
                    "page_closed": "page_closed"}.get(name, name)
        crash = (crash or {}) | {"status": status, "error_code": code, "exit_status": exit_status}

    snap = (terminal or {}).get("snapshot", {})

    def at(col):
        """Value at the failure: the event snapshot first (it was captured synchronously, with an age),
        then the last sample within 3 min — during a hang every sampler is blocked, so a 30 s window
        is routinely empty."""
        if col in snap:
            return (snap[col] or {}).get("value")
        return _last(ts, col, t_fail, 180.0) if t_fail else None

    alloc_at_fail = (snap.get("cumulative_alloc_mb") or {}).get("value") or at("cumulative_alloc_mb")
    if alloc_at_fail is None and len(ok_alloc):
        alloc_at_fail = float(ok_alloc["cumulative_alloc_mb"].iloc[-1])
    in_flight = (terminal or {}).get("detail", {}).get("in_flight_target_mb")
    readings = {k: at(k) for k in ("cdp_heap_mb", "browser_pmf_mb", "max_renderer_pmf_mb", "host_mem_free_mb",
                                    "host_mem_avail_est_mb", "host_mem_inuse_excl_cache_mb", "host_anon_mb",
                                    "host_file_mb", "cloudwatch_mem_mb", "usage_log_mem_mb")}
    recovery = next((e for e in ev_list if e["event"] in ("recovery_ok", "recovery_failed")), None)
    disconnected = "cdp_disconnected" in names
    session_after = None
    if t_fail is not None:
        st = _frame(rows, "session")
        if not st.empty and "session_status" in st.columns:
            after = st[st["t"] > t_fail].dropna(subset=["session_status"])
            session_after = after["session_status"].iloc[-1] if len(after) else None

    # ---- baseline vs pressure windows
    base_from = t_base if t_base is not None else (float(ts["t"].iloc[0]) if len(ts) else None)
    base_to = t_ramp or (base_from + 20 if base_from else None)
    end_ref = t_ramp_end
    pre_from = end_ref - 30 if end_ref else None
    idle_browser = (browser["during_step"] == 0) if "during_step" in browser else None

    def window_stats(df, col, mask=None):
        b = _vals(df, col, base_from, base_to, mask)
        p = _vals(df, col, pre_from, end_ref, mask)
        return {"baseline_p50": _q(b, 0.5), "pre_failure_p50": _q(p, 0.5), "pre_failure_p95": _q(p, 0.95),
                "pre_failure_max": float(p.max()) if len(p) else None, "n_baseline": len(b), "n_pre": len(p),
                "ratio_p50": _ratio(_q(p, 0.5), _q(b, 0.5))}

    def ramp_stats(df, col, mask=None):
        v = _vals(df, col, t_ramp, end_ref, mask)
        return {"p50": _q(v, 0.5), "p95": _q(v, 0.95), "max": float(v.max()) if len(v) else None, "n": len(v)}

    def tail_stats(df, col, n=5):
        """p50/max of the last n samples at or before the failure, however stale — a hung step blocks
        the samplers, so the final 30 s window is usually empty."""
        s_all = df[df["t"] <= (end_ref or np.inf)] if not df.empty and "t" in df else df
        vals = _vals(s_all, col).tail(n)
        return {"p50": _q(vals, 0.5), "max": float(vals.max()) if len(vals) else None, "n": len(vals)}

    probe = window_stats(page, "probe_latency_ms")
    probe_tail = tail_stats(page, "probe_latency_ms")
    bprobe_tail = tail_stats(browser, "probe_browser_ms")
    probe_ramp = ramp_stats(page, "probe_latency_ms")
    bprobe_ramp = ramp_stats(browser, "probe_browser_ms", idle_browser)
    bprobe = window_stats(browser, "probe_browser_ms", idle_browser)
    hprobe = window_stats(live_host, "host_probe_ms")
    cpu = window_stats(browser, "chrome_cpu_pct", idle_browser)
    steps = pd.to_numeric(ok_alloc["alloc_step_ms"], errors="coerce").dropna() if len(ok_alloc) else pd.Series(dtype=float)
    step_first, step_last = _q(steps.head(5), 0.5), _q(steps.tail(3), 0.5)
    failed_alloc = alloc_rows[alloc_rows["ok"] == False] if not alloc_rows.empty else alloc_rows  # noqa: E712
    failing_step_ms = float(failed_alloc["alloc_step_ms"].iloc[-1]) if len(failed_alloc) else None
    timeouts = [e for e in ev_list if e["event"] in ("probe_timeout", "browser_probe_timeout", "host_probe_timeout")
                and (t_fail is None or e["t"] <= t_fail)]

    onset_alloc = None
    if probe["baseline_p50"] is not None and len(page) and "probe_latency_ms" in page:
        pl = page.dropna(subset=["probe_latency_ms"])
        pl = pl[(pl["t"] >= (t_ramp or 0)) & (pl["t"] <= (end_ref or np.inf))]
        rolling = pl["probe_latency_ms"].rolling(5, min_periods=3).median()
        hit = pl[rolling > 2 * probe["baseline_p50"] + 20]
        if len(hit):
            onset_alloc = _last(ts, "cumulative_alloc_mb", float(hit["t"].iloc[0]), 1e9)

    base_p50 = probe["baseline_p50"]
    gradual_reasons, cliff_reasons = [], []
    ramp_ratio = _ratio(probe_ramp["p50"], base_p50)
    tail_ratio = _ratio(probe_tail["p50"], base_p50)
    max_ratio = _ratio(probe_ramp["max"], base_p50)
    bramp_ratio = _ratio(bprobe_ramp["p50"], bprobe["baseline_p50"])
    step_ratio = _ratio(step_last, step_first)
    fail_ratio = _ratio(failing_step_ms, step_first)
    # Sustained degradation (median moves) is GC thrash / reclaim pressure across the whole ramp.
    if ramp_ratio and ramp_ratio >= 2 and (probe_ramp["p50"] - base_p50) >= 30:
        gradual_reasons.append(f"renderer probe median {base_p50:.0f} → {probe_ramp['p50']:.0f} ms over the ramp "
                               f"({ramp_ratio:.1f}×)")
    if bramp_ratio and bramp_ratio >= 2 and (bprobe_ramp["p50"] - bprobe["baseline_p50"]) >= 30:
        gradual_reasons.append(f"browser-process probe median {bprobe['baseline_p50']:.0f} → "
                               f"{bprobe_ramp['p50']:.0f} ms ({bramp_ratio:.1f}×)")
    if step_ratio and step_ratio >= 1.4:
        gradual_reasons.append(f"allocation step {step_first:.0f} → {step_last:.0f} ms ({step_ratio:.1f}×) "
                               "while still succeeding")
    # Isolated spikes and a blown-up final step are a cliff, not a ramp.
    if max_ratio and max_ratio >= 3 and (probe_ramp["max"] - base_p50) >= 100:
        cliff_reasons.append(f"renderer probe spiked to {probe_ramp['max']:.0f} ms ({max_ratio:.0f}× baseline) "
                             "while its median stayed flat")
    if tail_ratio and tail_ratio >= 3 and (probe_tail["p50"] - base_p50) >= 50:
        cliff_reasons.append(f"renderer probe {base_p50:.0f} → {probe_tail['p50']:.0f} ms in the final samples")
    if fail_ratio and fail_ratio >= 3:
        cliff_reasons.append(f"the final step ran {failing_step_ms / 1000:.0f} s before the renderer died, vs "
                             f"{step_first / 1000:.1f} s for early steps ({fail_ratio:.0f}×)")
    if timeouts:
        cliff_reasons.append(f"{len(timeouts)} probe timeout(s) before the failure")
    slow_reasons = gradual_reasons + cliff_reasons
    if gradual_reasons and cliff_reasons:
        shape, slowdown = "gradual then cliff", "yes"
    elif gradual_reasons:
        shape, slowdown = "gradual", "yes"
    elif cliff_reasons:
        shape, slowdown = "cliff", "yes"
    elif (tail_ratio or 0) >= 1.5 or (step_ratio or 0) >= 1.2:
        shape, slowdown = "mild", "mild"
    else:
        shape, slowdown = "flat", "no"

    # ---- paging evidence
    swap_cached_max = float(_vals(live_host, "host_swap_cached_mb").max()) if len(_vals(live_host, "host_swap_cached_mb")) else None
    w_base = _q(_vals(live_host, "host_disk_write_mb_s", base_from, base_to), 0.5)
    w_ramp = _vals(live_host, "host_disk_write_mb_s", t_ramp, end_ref)
    w_pre = _q(w_ramp, 0.5)
    w_pre_max = float(w_ramp.max()) if len(w_ramp) else None
    r_base = _q(_vals(live_host, "host_disk_read_mb_s", base_from, base_to), 0.5)
    r_pre = _q(_vals(live_host, "host_disk_read_mb_s", t_ramp, end_ref), 0.5)
    avail_start = _q(_vals(live_host, "host_mem_avail_est_mb", base_from, base_to), 0.5)
    free_min = float(_vals(live_host, "host_mem_free_mb", t_ramp, end_ref).min()) \
        if len(_vals(live_host, "host_mem_free_mb", t_ramp, end_ref)) else None
    write_surge = w_pre is not None and w_pre >= 5 and w_pre >= 5 * max(w_base or 0, 0.5)
    alloc_max_ok = float(ok_alloc["cumulative_alloc_mb"].max()) if len(ok_alloc) else 0.0
    beyond_ram = mem_total is not None and alloc_max_ok > mem_total
    beyond_avail = avail_start is not None and alloc_max_ok > avail_start * 1.15
    # Did the guest VM actually run out of memory? Free pinned near zero while the kernel evicted page
    # cache to satisfy anonymous demand, with no swap in use, is memory exhaustion without paging.
    file_base = _q(_vals(live_host, "host_file_mb", base_from, base_to), 0.5)
    file_fail = at("host_file_mb") or _q(_vals(live_host, "host_file_mb", pre_from, end_ref), 0.5)
    anon_fail = at("host_anon_mb") or _q(_vals(live_host, "host_anon_mb", pre_from, end_ref), 0.5)
    cache_evicted_pct = (100 * (1 - file_fail / file_base)) if file_base and file_fail else None
    anon_share_pct = (100 * anon_fail / mem_total) if anon_fail and mem_total else None
    free_floor_pct = (100 * free_min / mem_total) if free_min is not None and mem_total else None
    avail_fail = at("host_mem_avail_est_mb")
    avail_fail_pct = (100 * avail_fail / mem_total) if avail_fail is not None and mem_total else None
    # Linux keeps "free" low by design, so also require little left that it could reclaim.
    memory_exhausted = bool(
        free_floor_pct is not None and free_floor_pct <= 6
        and (avail_fail_pct is None or avail_fail_pct <= 20)
        and ((cache_evicted_pct or 0) >= 40 or (anon_share_pct or 0) >= 70)
        and not (swap_cached_max or 0) > 16)

    killed = mode in ("oom_kill", "chrome_oom", "renderer_crash", "connection_drop", "session_terminated",
                      "allocation_refused", "page_closed", "hang", "v8_heap_oom", "browser_restart",
                      "connection_drop_oom")
    no_ceiling = mode in ("no_failure", "interrupted", "runner_error")
    # Chrome reports no exit code when the automation stream dies with the renderer, so when the VM was
    # demonstrably out of memory, attribute the kill rather than reporting the symptom.
    # When the automation stream drops, no exit code reaches us (every Playwright page reports "closed"),
    # so decide what died from what survived. An explicit crash status from Chrome is never overridden.
    oom_inferred = False
    survived = (recovery or {}).get("detail") or {}
    if memory_exhausted and mode in ("page_closed", "connection_drop", "hang"):
        symptom = mode
        if survived.get("same_browser_process") is False:
            mode = "browser_restart"
        elif (survived.get("same_browser_process") is True
              and str(survived.get("pressured_page", "")).startswith(("dead", "gone"))):
            oom_inferred, mode = True, "oom_kill"
        else:
            mode = "connection_drop_oom"
    if no_ceiling and (swap_cached_max or 0) <= 16 and not beyond_ram:
        paged = "not applicable (ceiling not reached)"
    elif (swap_cached_max or 0) > 16 or beyond_ram or (write_surge and beyond_avail):
        paged = "yes"
    elif killed and (swap_cached_max in (None, 0.0)) and not write_surge and not beyond_avail:
        paged = "no" if len(host) else "no (inferred without guest-VM data)"
    else:
        paged = "inconclusive"

    # ---- after the ceiling: plateau / drop / climb
    aftermath = {}
    post_host = host[host["after_failure"] == 1] if not host.empty else pd.DataFrame()
    if t_fail is not None:
        post_to = t_fail + float(args.get("post_failure_s", 60))
        for col in ("host_mem_inuse_excl_cache_mb", "host_mem_free_mb", "browser_pmf_mb"):
            before = at(col) if col in snap else _last(ts, col, t_fail, 180.0)
            after = None
            if not post_host.empty and col in post_host.columns:
                vals = _vals(post_host, col)          # read back over a fresh connection after the kill
                after = float(vals.iloc[-1]) if len(vals) else None
            if after is None:
                after = _q(_vals(ts, col, t_fail + 10, post_to), 0.5)
            aftermath[col] = {"before": before, "after": after, "source": "reconnect" if not post_host.empty else "samples",
                              "delta": (after - before) if None not in (before, after) else None}
    elif t_ramp_end is not None:
        tail = ts[(ts["t"] >= t_ramp_end - 60) & (ts["t"] <= t_ramp_end)]
        for col in ("cumulative_alloc_mb", "host_mem_inuse_excl_cache_mb", "browser_pmf_mb"):
            s = _vals(tail, col)
            aftermath[col] = {"growth_last_60s": float(s.iloc[-1] - s.iloc[0]) if len(s) > 1 else None}
    allocs_after_failure = int((ok_alloc_all["t"] > t_fail).sum()) if (t_fail and len(ok_alloc_all)) else 0
    drop = (aftermath.get("host_mem_inuse_excl_cache_mb") or {}).get("delta")
    if drop is None:
        drop = (aftermath.get("browser_pmf_mb") or {}).get("delta")

    # ---- CloudWatch / USAGE_LOGS
    disc = (cw_doc or {}).get("discovery") or _load_json(run_dir / "discovery.json")
    cw_mem = (cw_doc or {}).get("series", {}).get("memory") or {}
    cw_cpu = (cw_doc or {}).get("series", {}).get("cpu") or {}
    cw_in_window = {k: v for k, v in cw_mem.items()
                    if t_created and t_stopped and t_created - 60 <= _epoch(k) <= t_stopped}
    usage_peak = max((u["usage_log_mem_mb"] for u in usage if u.get("usage_log_mem_mb") is not None), default=None)

    # ---- billing
    billing = estimate_billing(ts, t_created, t_stopped, cw_doc, usage, mem_total, browser)

    findings = {
        "run_id": meta.get("run_id"), "strategy": meta.get("strategy"), "run_dir": run_dir.name,
        "session_id": meta.get("session_id"), "region": meta.get("region"), "browser_id": meta.get("browser_id"),
        "params": {k: args.get(k) for k in ("step_mb", "interval_s", "max_mb", "alloc_kind", "fill",
                                            "sample_interval_s", "memdump_interval_s", "host_interval_s")},
        "env": {k: env.get(k) for k in ("chrome_version", "hardwareConcurrency", "deviceMemoryGb",
                                        "jsHeapSizeLimitMb", "connect_ms")},
        "host_caps": {k: v for k, v in caps.items() if k not in ("t", "kind")},
        "valid": t_ramp is not None and mode not in ("interrupted", "runner_error"),
        "mode": mode, "mode_text": (MODE_TEXT["oom_kill_inferred"] if oom_inferred else MODE_TEXT.get(mode, mode)),
        "oom_inferred": oom_inferred, "symptom": symptom, "avail_at_failure_pct": avail_fail_pct,
        "memory_exhausted": memory_exhausted, "cache_evicted_pct": cache_evicted_pct,
        "anon_share_pct": anon_share_pct, "free_floor_pct": free_floor_pct,
        "probe_tail": probe_tail, "browser_probe_tail": bprobe_tail, "failing_step_ms": failing_step_ms,
        "probe_ramp": probe_ramp, "browser_probe_ramp": bprobe_ramp, "slowdown_shape": shape,
        "gradual_reasons": gradual_reasons, "cliff_reasons": cliff_reasons,
        "failure_event": terminal["event"] if terminal else None,
        "failure_elapsed_s": round(t_fail - t_created, 1) if t_fail and t_created else None,
        "failure_utc": _iso(t_fail) if t_fail else None,
        "crash": crash, "vanished_processes": vanished,
        "alloc_at_failure_mb": alloc_at_fail, "in_flight_target_mb": in_flight,
        "max_allocated_ok_mb": alloc_max_ok, "readings_at_failure": readings, "failure_snapshot": snap,
        "recovery": recovery["event"] if recovery else None,
        "recovery_detail": recovery["detail"] if recovery else None,
        "survivors": _survivors(recovery),
        "disconnected": disconnected, "session_status_after": session_after,
        "mem_total_mb": mem_total, "avail_est_at_start_mb": avail_start, "vm_free_min_mb": free_min,
        "slowdown": slowdown, "slow_reasons": slow_reasons, "slowdown_onset_alloc_mb": onset_alloc,
        "probe": probe, "browser_probe": bprobe, "host_probe": hprobe, "chrome_cpu_idle": cpu,
        "step_ms_first5_p50": step_first, "step_ms_last3_p50": step_last, "timeouts_before_failure": len(timeouts),
        "paged": paged, "swap_cached_max_mb": swap_cached_max,
        "disk_write_mb_s": {"baseline_p50": w_base, "pre_failure_p50": w_pre, "pre_failure_max": w_pre_max},
        "disk_read_mb_s": {"baseline_p50": r_base, "pre_failure_p50": r_pre},
        "beyond_ram": beyond_ram, "beyond_avail": beyond_avail, "write_surge": write_surge,
        "aftermath": aftermath, "memory_delta_after_failure_mb": drop,
        "allocs_after_failure": allocs_after_failure,
        "cloudwatch": {
            "namespaces": disc.get("agentcore_namespaces"), "selected": disc.get("selected"),
            "metrics_listed": len(disc.get("metrics_listed") or []),
            "memory_points_in_session": len(cw_in_window), "cpu_points": len(cw_cpu),
            "memory_peak_mb": max(cw_in_window.values()) if cw_in_window else None,
            "covers_session": (cw_doc or {}).get("covers_session"),
            "usage_log_groups": (cw_doc or {}).get("usage_log_groups"),
            "usage_log_records": len(usage), "usage_log_peak_mb": usage_peak,
            "other_ready_sessions_at_start": meta.get("other_ready_sessions_at_start"),
        },
        "billing": billing, "baseline_gb": _idle_gb(ts),
        "counts": {"samples": len(rows), "memdumps": sum(1 for r in rows if r.get("kind") == "memdump"),
                   "host_samples": len(live_host), "page_samples": len(page), "events": len(ev_list)},
        "events": [{"elapsed_s": round(e["t"] - t_created, 1) if t_created else None, "event": e["event"],
                    "detail": e.get("detail")} for e in ev_list],
    }
    findings["mode_short"] = MODE_SHORT.get(findings["mode"], findings["mode"])
    findings["summary_line"] = summary_line(findings)
    return findings


def _survivors(recovery: dict | None) -> dict | None:
    """Survivor-check fields from the recovery event; None for runs recorded before the check existed."""
    detail = (recovery or {}).get("detail") or {}
    keys = ("browser_pid_before", "browser_pid_after", "same_browser_process", "surviving_pages", "pressured_page")
    found = {k: detail.get(k) for k in keys}
    return found if found.get("same_browser_process") is not None or found.get("surviving_pages") is not None else None


def estimate_billing(ts, t_created, t_stopped, cw_doc, usage, vm_total_mb=None, browser=None) -> dict:
    """Per-second peak-memory and CPU cost estimates for the session from each available signal."""
    out: dict = {"price_gb_hour": PRICE_GB_HOUR, "price_vcpu_hour": PRICE_VCPU_HOUR,
                 "min_billed_gb": MIN_BILLED_GB}
    if not (t_created and t_stopped) or ts.empty:
        return out
    seconds = np.arange(t_created, t_stopped, 1.0)
    out["billed_seconds"] = len(seconds)
    grid = pd.DataFrame({"t": seconds})
    signals = {"chrome_pmf": "browser_pmf_mb", "vm_in_use_excl_cache": "host_mem_inuse_excl_cache_mb",
               "vm_used_incl_cache": "host_mem_used_mb"}
    src = ts[["t", *signals.values()]]
    _asof(grid, src, list(signals.values()), None)
    if usage:
        u = pd.DataFrame(usage).dropna(subset=["t", "usage_log_mem_mb"])
        if not u.empty:
            _asof(grid, u.sort_values("t"), ["usage_log_mem_mb"], 1.5)
            signals["aws_usage_logs"] = "usage_log_mem_mb"
    per_signal = {}
    for name, col in signals.items():
        mb = pd.to_numeric(grid[col], errors="coerce")
        if mb.notna().sum() == 0:
            continue
        gb = (mb.ffill().bfill() / 1024.0).clip(lower=MIN_BILLED_GB)
        # "Peak memory consumed up to that second": a running high-water mark, not the instant value.
        billed_gb = gb.cummax()
        per_signal[name] = {"peak_gb": round(float(gb.max()), 3), "mean_gb": round(float(gb.mean()), 3),
                            "memory_cost_usd": round(float(billed_gb.sum()) * PRICE_GB_HOUR / 3600.0, 6)}
    out["memory_by_signal"] = per_signal
    if browser is not None and not browser.empty and "chrome_cpu_pct" in browser.columns:
        b = browser[["t", "chrome_cpu_pct"]].copy()
        # Each percentage is the average since the previous getProcessInfo call, so pair it with the
        # gap to the previous browser sample — including the first row that carries a percentage.
        b["interval_s"] = b["t"].diff()
        cpu = b.dropna(subset=["chrome_cpu_pct", "interval_s"])
        if len(cpu):
            vcpu_s = float((pd.to_numeric(cpu["chrome_cpu_pct"]) / 100.0 * cpu["interval_s"]).sum())
            out["chrome_cpu_vcpu_seconds"] = round(vcpu_s, 1)
            out["chrome_cpu_measured_seconds"] = round(float(cpu["interval_s"].sum()), 1)
            out["cpu_cost_usd_from_chrome"] = round(vcpu_s * PRICE_VCPU_HOUR / 3600.0, 6)
    ordered = sorted((u for u in usage if u.get("usage_log_mem_mb") is not None and u.get("t")),
                     key=lambda u: u["t"]) if usage else []
    billed = pd.Series([u["usage_log_mem_mb"] for u in ordered], dtype=float)
    # The first record covers a partial second, and the last two fall in the session's shutdown, when
    # memory is being released; exclude them from minima.
    whole = billed.iloc[1:-2] if len(billed) > 3 else billed
    if len(billed) >= 5:
        lo, hi, mid = float(billed.min()), float(billed.max()), float(billed.median())
        full = 0.97 * (vm_total_mb or mid)
        share_full = float((billed >= full).mean())
        out["billed_memory_mb"] = {"min": round(lo, 1), "min_whole_seconds": round(float(whole.min()), 1),
                                   "p25": round(float(billed.quantile(0.25)), 1),
                                   "median": round(mid, 1), "max": round(hi, 1), "n_records": len(billed),
                                   "share_at_full_vm": round(share_full, 3)}
        # Billed at the VM's whole RAM size for nearly every second, regardless of what the browser
        # touched: AWS charges the session's allocation, not its working set. (The first and last
        # records of a session cover partial intervals, so min/max alone are not the test.)
        out["billed_memory_is_full_vm"] = bool(mid >= full and share_full >= 0.8)
        out["billed_memory_iqr_pct"] = round(
            100 * (float(billed.quantile(0.75)) - float(billed.quantile(0.25))) / mid, 2) if mid else None
    cpu_billed = pd.Series([u["usage_log_vcpu"] for u in usage if u.get("usage_log_vcpu") is not None],
                           dtype=float) if usage else pd.Series(dtype=float)
    if len(cpu_billed) >= 5:
        out["billed_vcpu"] = {"min": round(float(cpu_billed.min()), 3),
                              "median": round(float(cpu_billed.median()), 3),
                              "max": round(float(cpu_billed.max()), 3)}
    raw = (cw_doc or {}).get("raw_datapoints") or {}
    sel = ((cw_doc or {}).get("discovery") or {}).get("selected") or {}
    for signal, price in (("memory", PRICE_GB_HOUR), ("cpu", PRICE_VCPU_HOUR)):
        key = (sel.get(signal) or {}).get("key")
        unit = (sel.get(signal) or {}).get("unit_kind")
        if key in raw and unit in ("gb_hours", "vcpu_hours"):
            pts = {k: v for k, v in raw[key].items() if t_created - 60 <= _epoch(k) <= t_stopped + 60}
            if pts:
                out[f"cloudwatch_{signal}_{unit}"] = round(sum(pts.values()), 6)
                out[f"cloudwatch_{signal}_cost_usd"] = round(sum(pts.values()) * price, 6)
    return out


def summary_line(f: dict) -> str:
    mode = f["mode_text"]
    if f["mode"] not in ("no_failure", "interrupted", "runner_error"):
        mode += f" at {fmt(f['alloc_at_failure_mb'], 'MB')} allocated"
    return (f"[{f['strategy']}] failure mode: {mode} | paged to disk: {f['paged']} | "
            f"slowed before failing: {f['slowdown']}")


# --------------------------------------------------------------------------- report text
def render_findings(f: dict) -> str:
    b, cw, crash = f["billing"], f["cloudwatch"], f.get("crash") or {}
    probe, bprobe = f["probe"], f["browser_probe"]
    end_label = "before failure" if f.get("failure_event") else "of the ramp"
    r = f["readings_at_failure"]
    mem_total = f.get("mem_total_mb")
    lines = [f"# AgentCore Browser at the memory ceiling — `{f['strategy']}` strategy",
             "",
             f"_Auto-generated by `benchmark.py` from `runs/{f['run_dir']}/` (session `{f['session_id']}`, "
             f"{f['region']}, `{f['browser_id']}`). Regenerate with `python benchmark.py --reanalyze runs/{f['run_dir']}`._",
             ""]
    if not f["valid"]:
        lines += [f"> **Run did not complete a ramp** ({f['mode_text']}). Treat the answers below as unknown.", ""]

    # 1. paging
    caps = f.get("host_caps") or {}
    swap_bits = []
    if caps:
        swap_bits.append(f"zswap enabled: `{caps.get('zswap_enabled')}`, zram devices: "
                         f"{caps.get('zram_devices') or 'none'}, block devices: {caps.get('block_devices')}")
    swap_bits.append(f"`SwapCached` peak {fmt(f['swap_cached_max_mb'], 'MB')}")
    if f.get("free_floor_pct") is not None:
        swap_bits.append(f"VM free memory bottomed at {fmt(f['vm_free_min_mb'], 'MB')} "
                         f"({f['free_floor_pct']:.1f}% of RAM) and stayed there")
    if f.get("cache_evicted_pct") is not None:
        swap_bits.append(f"the kernel reclaimed page cache instead — `FilePages` fell "
                         f"{f['cache_evicted_pct']:.0f}% under pressure")
    if f.get("anon_share_pct") is not None:
        swap_bits.append(f"anonymous memory reached {f['anon_share_pct']:.0f}% of RAM")
    swap_bits.append(f"disk writes p50 {fmt(f['disk_write_mb_s']['baseline_p50'], 'MB/s', 1)} at baseline vs "
                     f"{fmt(f['disk_write_mb_s']['pre_failure_p50'], 'MB/s', 1)} across the ramp "
                     f"(max {fmt(f['disk_write_mb_s']['pre_failure_max'], 'MB/s', 1)}) — no write surge")
    headroom = ""
    if f.get("avail_est_at_start_mb"):
        headroom = (f" The last successful step left {fmt(f['max_allocated_ok_mb'], 'MB')} retained, "
                    f"{100 * f['max_allocated_ok_mb'] / f['avail_est_at_start_mb']:.0f}% of the "
                    f"~{fmt(f['avail_est_at_start_mb'], 'MB')} the VM could still hand out before the ramp "
                    f"(free + reclaimable cache; MemTotal {fmt(mem_total, 'MB')}).")
    paged_text = {
        "yes": "**Yes.** Memory beyond physical RAM was serviced:",
        "no": "**No — there is nowhere to page to.** The microVM has no swap device, no zram and zswap "
              "disabled, so when anonymous memory grows the kernel can only evict page cache and then "
              "kill. Evidence:",
        "no (inferred without guest-VM data)": "**No (inferred).** The failure was a hard stop; guest-VM data was unavailable:",
        "inconclusive": "**Inconclusive.**",
    }.get(f["paged"], "**Not applicable** — the ramp stopped before the ceiling.")
    q1 = f"{paged_text} " + "; ".join(swap_bits) + "." + headroom

    # 2. slowdown
    rtt = bprobe.get("baseline_p50")
    ptail, btail = f["probe_tail"], f["browser_probe_tail"]
    pramp = f["probe_ramp"]
    q2 = {"gradual": "**Yes — a sustained, progressive slowdown as memory filled.**",
          "gradual then cliff": "**Yes — it degraded progressively and then fell off a cliff.**",
          "cliff": "**Barely, and then all at once.** Throughput stayed flat almost to the end; this is a cliff, "
                   "not the long GC-thrash ramp you might expect.",
          "mild": "**Only mildly.**",
          "flat": "**No — it stayed flat until the cliff.**"}[f["slowdown_shape"]]
    probe_where = ("on the page under pressure" if f["strategy"] == "inpage"
                   else "on a dedicated light tab, separate from the heavy tabs")
    q2 += (f" Renderer round-trip (`evaluate(1+1)` {probe_where}): {fmt(probe['baseline_p50'], 'ms')} "
           f"at baseline → median {fmt(pramp['p50'], 'ms')} across the ramp (p95 {fmt(pramp['p95'], 'ms')}, "
           f"max {fmt(pramp['max'], 'ms')}), and {fmt(ptail['p50'], 'ms')} over the last {ptail['n']} samples "
           f"{end_label}. Browser-process probe (`Browser.getVersion`, whose baseline is essentially the runner's "
           f"network RTT): {fmt(rtt, 'ms')} → {fmt(btail['p50'], 'ms')}. "
           f"Allocation step time: {fmt(f['step_ms_first5_p50'], 'ms')} (first 5 steps) → "
           f"{fmt(f['step_ms_last3_p50'], 'ms')} (last 3 that completed)"
           + (f", and the step that killed the renderer ran {f['failing_step_ms'] / 1000:.0f} s before dying"
              if f.get("failing_step_ms") else "") + ".")
    if f["gradual_reasons"]:
        q2 += f" Progressive degradation: {'; '.join(f['gradual_reasons'])}."
    if f["cliff_reasons"]:
        q2 += f" The cliff: {'; '.join(f['cliff_reasons'])}."
    if f["slowdown_onset_alloc_mb"]:
        q2 += f" Latency first exceeded 2× baseline at ~{fmt(f['slowdown_onset_alloc_mb'], 'MB')} allocated."
    if probe["n_pre"] == 0 and f.get("failure_event"):
        q2 += (" (The fixed 30 s pre-failure window is empty: once the final allocation stalled, the probe loop "
               "deliberately skips rather than queueing behind it, so the figures above come from the last "
               "samples taken before the stall.)")

    # 3. failure mode
    if f["mode"] in ("no_failure", "interrupted", "runner_error"):
        q3 = f"**{f['mode_text'][0].upper() + f['mode_text'][1:]}** (max allocated {fmt(f['max_allocated_ok_mb'], 'MB')})."
    else:
        q3 = (f"**{f['mode_text'][0].upper() + f['mode_text'][1:]}** — `{f['failure_event']}` at "
              f"t+{fmt(f['failure_elapsed_s'], 's')} "
              f"({f['failure_utc']}), with **{fmt(f['alloc_at_failure_mb'], 'MB')}** of retained allocations"
              + (f" (step to {fmt(f['in_flight_target_mb'], 'MB')} in flight)" if f.get("in_flight_target_mb") else "") + ".")
        if crash.get("status") or crash.get("exit_status"):
            q3 += f" Chrome reported the target `{crash.get('status')}` with exit status `{crash.get('exit_status')}`."
        if f["mode"] in ("browser_restart", "connection_drop_oom"):
            stall = f.get("failing_step_ms")
            q3 += (f" Chrome reported no exit code because the automation connection itself dropped (the observable "
                   f"symptom was `{f.get('symptom')}`). At that point the guest VM was out of memory: free "
                   f"{fmt(f['vm_free_min_mb'], 'MB')} ({(f.get('free_floor_pct') or 0):.1f}% of RAM), page cache down "
                   f"{(f.get('cache_evicted_pct') or 0):.0f}%, swap unused"
                   + (f", and the final allocation had been stuck for {stall / 1000:.0f} s" if stall else "") + ".")
            if f["mode"] == "connection_drop_oom":
                q3 += " This run predates the check that tells a tab kill apart from a Chrome restart."
        if f.get("oom_inferred"):
            q3 += (f" Chrome reported no exit code — the observable symptom was `{f['symptom']}` — but the guest VM "
                   f"was out of memory at that moment (free {fmt(f['vm_free_min_mb'], 'MB')} = "
                   f"{f['free_floor_pct']:.1f}% of RAM, page cache down {f['cache_evicted_pct']:.0f}%, anonymous "
                   f"memory {f['anon_share_pct']:.0f}% of RAM, swap unused), which is an out-of-memory kill.")
        if f.get("vanished_processes"):
            q3 += (" Processes that disappeared at the failure: "
                   + ", ".join(f"pid {v['pid']} ({v['process_type']}, last {fmt(v.get('last_known_pmf_mb'), 'MB')})"
                               for v in f["vanished_processes"]) + ".")
        q3 += (f" Last readings before the failure: VM free {fmt(r['host_mem_free_mb'], 'MB')}, VM available "
               f"(est.) {fmt(r['host_mem_avail_est_mb'], 'MB')}, VM in use excl. cache {fmt(r['host_mem_inuse_excl_cache_mb'], 'MB')}, "
               f"Chrome private footprint {fmt(r['browser_pmf_mb'], 'MB')} (largest renderer {fmt(r['max_renderer_pmf_mb'], 'MB')}), "
               f"CDP heap+ArrayBuffers {fmt(r['cdp_heap_mb'], 'MB')}, CloudWatch {fmt(r['cloudwatch_mem_mb'], 'MB')}.")
        survived = f.get("recovery") == "recovery_ok"
        via = (f.get("recovery_detail") or {}).get("via")
        same = (f.get("survivors") or {}).get("same_browser_process")
        q3 += (f" **The session itself survived**: {'reconnecting to the same CDP endpoint' if via == 'reconnect' else 'a new tab'} "
               f"worked immediately afterwards, so the microVM was not torn down"
               + {True: ", and only the tab's process died", False: "", None: ""}[same]
               if survived else " Recovery afterwards: " + str(f.get("recovery") or "not attempted"))
        q3 += (f". CDP connection dropped: {'yes' if f['disconnected'] else 'no'}; "
               f"GetBrowserSession status after the failure: `{f.get('session_status_after')}`.")
        sv = f.get("survivors") or {}
        if sv.get("same_browser_process") is not None:
            q3 += (f" Chrome's browser process after recovery was **{'the same' if sv['same_browser_process'] else 'a different'}"
                   f" one** (PID {sv.get('browser_pid_before')} → {sv.get('browser_pid_after')})"
                   + (", so Chrome itself was not restarted" if sv["same_browser_process"]
                      else ", so Chrome itself was restarted and every tab was lost") + ".")
        if sv.get("pressured_page"):
            q3 += f" The page that held the memory: {sv['pressured_page']}."
        if sv.get("surviving_pages") is not None:
            q3 += f" Pages still open afterwards: {', '.join(f'`{t}`' for t in sv['surviving_pages']) or 'none'}."

    lines += ["## Answers", "", f"1. **Did it page to disk?** {q1}", "",
              f"2. **Did it slow down before failing?** {q2}", "",
              f"3. **What was the failure mode, and at what memory reading?** {q3}", ""]

    # aftermath
    lines += ["## After the ceiling: plateau, drop, or climb?", ""]
    am = f["aftermath"]
    if f.get("failure_event"):
        rows = []
        for col, label in (("host_mem_inuse_excl_cache_mb", "VM in use excl. cache"),
                           ("host_mem_free_mb", "VM free"), ("browser_pmf_mb", "Chrome private footprint")):
            a = am.get(col) or {}
            rows.append(f"| {label} | {fmt(a.get('before'), 'MB')} | {fmt(a.get('after'), 'MB')} "
                        f"| {fmt(a.get('delta'), 'MB')} |")
        delta = f.get("memory_delta_after_failure_mb")
        caveat = ""
        if f.get("allocs_after_failure"):
            caveat = (f" Note: {f['allocs_after_failure']} allocation step(s) that were already in flight completed "
                      "after the failure, so the post-failure figures include memory they added.")
        verdict = ("Memory **dropped** right after the failure — the killed process's memory went back to the VM. "
                   "That is the signature of a kill at a hard cap, not of paging (paging would keep memory pinned "
                   "near the ceiling while allocations continued and latency climbed)." + caveat
                   if delta is not None and delta < -0.3 * max(f["alloc_at_failure_mb"] or 0, 1)
                   else ("No post-failure reading is available — the CDP connection died with the renderer, so "
                         "in-browser sampling stopped at that instant. Re-run with `--post-failure-s` > 0 (the "
                         "default) so the recovery reconnect re-reads guest memory."
                         if all((v or {}).get("after") is None for v in am.values())
                         else "Memory did not drop much after the failure event; see the chart and CSV." + caveat))
        src = next((v.get("source") for v in am.values() if v.get("after") is not None), None)
        after_col = "after the kill (reconnect)" if src == "reconnect" else "median 10–60 s after"
        lines += [f"| signal | last before failure | {after_col} | change |", "|---|---:|---:|---:|",
                  *rows, "", verdict, ""]
    else:
        lines += ["No terminal failure; growth over the last 60 s of the ramp: " + ", ".join(
            f"{k} {fmt((v or {}).get('growth_last_60s'), 'MB')}" for k, v in am.items()), ""]

    # billing
    mem_sig = b.get("memory_by_signal") or {}
    lines += ["## Billing implication", "",
              f"AgentCore Browser is priced per second at ${PRICE_GB_HOUR}/GB-hour of memory and "
              f"${PRICE_VCPU_HOUR}/vCPU-hour of CPU, documented as *peak memory consumed up to that second* "
              f"with a 128 MB floor. This session ran {fmt(b.get('billed_seconds'), 's')}.", ""]
    bm, bv = b.get("billed_memory_mb"), b.get("billed_vcpu")
    if bm:
        in_use = (mem_sig.get("vm_in_use_excl_cache") or {}).get("mean_gb")
        ratio = (bm["median"] / 1024) / in_use if in_use else None
        lines += [
            f"**Measured, from AWS's own per-second `USAGE_LOGS` for this session** "
            f"({bm['n_records']} records): metered memory was **{bm['median'] / 1024:.2f} GB at the median** "
            f"(p25 {bm['p25'] / 1024:.2f} GB, max {bm['max'] / 1024:.2f} GB) and at the VM's full RAM size "
            f"for {bm['share_at_full_vm'] * 100:.0f}% of the session's seconds"
            + (f" — about **{ratio:.1f}×** the memory the session actually had in use (mean {in_use:.2f} GB, "
               "excluding reclaimable cache)" if ratio else "") + ".", ""]
        if not b.get("billed_memory_is_full_vm") and ratio and ratio >= 2:
            lines += [
                f"> **Billed memory did not track use.** It stayed around {bm['median'] / 1024:.1f} GB, roughly "
                f"{ratio:.0f}× what was actually in use, for the whole session. Trimming memory inside a session "
                "would not have reduced it.", ""]
        if b.get("billed_memory_is_full_vm"):
            observed = mem_sig.get("vm_in_use_excl_cache") or mem_sig.get("chrome_pmf") or {}
            span = (f"which over this run ranged from {f['baseline_gb']:.2f} GB at idle baseline to a peak of "
                    f"{observed['peak_gb']:.2f} GB" if f.get("baseline_gb") and observed.get("peak_gb")
                    else "which varied widely over this run")
            lines += [
                "> **This contradicts the intuition that staying well under the ceiling is cheaper.** Billed "
                f"memory did not track what the browser actually used ({span}) — it was the session's entire "
                f"{bm['median'] / 1024:.0f} GB allocation for virtually every second the session existed. "
                f"Memory therefore costs a flat **${bm['median'] / 1024 * PRICE_GB_HOUR:.4f}/session-hour** "
                "whether the agent idles on `about:blank` or rides the ceiling. The lever on memory cost is "
                "**session count × session lifetime**, not per-session frugality.", ""]
        if bv:
            lines += [f"CPU, by contrast, did vary with work: {bv['min']:.2f}–{bv['max']:.2f} vCPU "
                      f"(median {bv['median']:.2f}), consistent with the documented \"I/O wait and idle time is "
                      f"free\" behaviour. A fully busy vCPU adds ${PRICE_VCPU_HOUR:.4f}/hour.", ""]
    if mem_sig:
        lines += ["| memory signal | peak | mean | est. memory cost of this run |", "|---|---:|---:|---:|"]
        for name, v in mem_sig.items():
            lines.append(f"| {name} | {v['peak_gb']:.2f} GB | {v['mean_gb']:.2f} GB | ${v['memory_cost_usd']:.4f} |")
        lines.append("")
    if b.get("cpu_cost_usd_from_chrome") is not None:
        lines.append(f"CPU (Chrome processes, summed from CDP `SystemInfo.getProcessInfo`): "
                     f"{fmt(b['chrome_cpu_vcpu_seconds'], 'vCPU-s', 1)} over "
                     f"{fmt(b.get('chrome_cpu_measured_seconds'), 's', 1)} of sampled time ≈ "
                     f"${b['cpu_cost_usd_from_chrome']:.4f}. Sampling stops when the connection drops, so "
                     f"this covers only the sampled window, not necessarily the whole session.")
    for sig in ("memory", "cpu"):
        k = next((k for k in b if k.startswith(f"cloudwatch_{sig}_") and k.endswith("hours")), None)
        if k:
            lines.append(f"CloudWatch `{sig}`: {b[k]} {k.split('_', 2)[2].replace('_', '-')} ≈ ${b[f'cloudwatch_{sig}_cost_usd']:.4f}.")
    order = ["aws_usage_logs", "vm_in_use_excl_cache", "chrome_pmf", "vm_used_incl_cache"]
    head = next((n for n in order if n in mem_sig), None)
    peak_gb = mem_sig[head]["peak_gb"] if head else None
    base_gb = f.get("baseline_gb")
    if b.get("billed_memory_is_full_vm"):
        head, base_gb = head, None  # a per-session "ride lower to pay less" comparison would mislead
    if peak_gb:
        hourly_peak = peak_gb * PRICE_GB_HOUR
        verb = "Observed consumption peaked at" if b.get("billed_memory_is_full_vm") else "Holding a session at"
        lines += ["", f"{verb} {peak_gb:.2f} GB (`{head}`), which at list price is "
                  f"**${hourly_peak:.4f}/hour in memory**"
                  + (f" versus ${base_gb * PRICE_GB_HOUR:.4f}/hour at the idle baseline ({base_gb:.2f} GB) — "
                     f"**{peak_gb / base_gb:.1f}×**" if base_gb else "")
                  + f". At the full 4 GB that is ${4 * PRICE_GB_HOUR:.4f}/hour"
                  + (" — which is what was actually billed here, so this figure is the floor as well as the "
                     "ceiling for a live session." if b.get("billed_memory_is_full_vm")
                     else "; a fully busy vCPU adds a further "
                          f"${PRICE_VCPU_HOUR:.4f}/hour, but idle and I/O-wait CPU is free, so memory dominates "
                          "the bill of a mostly idle session and scales with how close to the ceiling it rides.")]
    lines += ["", "## Signals used", ""]
    sel = cw.get("selected") or {}
    if sel:
        for sig, m in sel.items():
            lines.append(f"- CloudWatch {sig}: `{m.get('key')}` (scope **{m.get('scope')}**, {m.get('stat')}, "
                         f"converted from {m.get('unit_kind')}).")
        lines.append(f"- CloudWatch memory datapoints inside the session window: {cw['memory_points_in_session']} "
                     f"(peak {fmt(cw['memory_peak_mb'], 'MB')}; covers whole session: {cw.get('covers_session')}).")
    else:
        lines.append(f"- CloudWatch: AgentCore namespaces found: {cw.get('namespaces') or 'none'}; "
                     f"{cw.get('metrics_listed')} metric(s) listed, **no Browser memory/CPU metric available** for this "
                     "session at collection time. AWS documents up to 60 min delay for resource-usage data — re-run "
                     "`--reanalyze` later. The answers above rely on the CDP and guest-VM signals.")
    if cw.get("other_ready_sessions_at_start"):
        lines.append(f"- ⚠ {cw['other_ready_sessions_at_start']} other READY session(s) shared this browser tool, so "
                     "tool-level CloudWatch metrics include them.")
    lines.append(f"- USAGE_LOGS (per-second session usage): {cw.get('usage_log_records')} record(s) from "
                 f"{cw.get('usage_log_groups') or 'no configured delivery'}"
                 + (f", peak {fmt(cw['usage_log_peak_mb'], 'MB')}" if cw.get("usage_log_peak_mb") else "") + ".")
    c = f["counts"]
    lines += [f"- CDP: `Runtime.getHeapUsage` (V8 heap + ArrayBuffer backing store), `Performance.getMetrics`, "
              f"`SystemInfo.getProcessInfo` (per-process CPU), memory-infra dumps ({c['memdumps']}) for per-process "
              "private footprint, `Target.targetCrashed` / `page.on('crash')` / WebSocket `disconnected` for failure detection.",
              f"- Guest VM: `/sys/devices/system/node/node0/meminfo` and `/sys/block/*/stat` read through a `file://` "
              f"tab ({c['host_samples']} samples). Capabilities: `{json.dumps(caps)}`.",
              "", "## Run", "",
              f"- Parameters: `{json.dumps(f['params'])}`",
              f"- Browser: Chrome {f['env'].get('chrome_version')}, hardwareConcurrency="
              f"{f['env'].get('hardwareConcurrency')} (the documented 1 vCPU), V8 jsHeapSizeLimit="
              f"{fmt(f['env'].get('jsHeapSizeLimitMb'), 'MB')}, VM MemTotal={fmt(mem_total, 'MB')} "
              f"(the documented 4 GB).",
              "- Events:", ""]
    for e in f["events"]:
        detail = {k: v for k, v in (e.get("detail") or {}).items() if v is not None and k != "terminal"}
        lines.append(f"  - t+{fmt(e['elapsed_s'], 's', 1)} `{e['event']}` {json.dumps(detail, default=str)[:240]}")
    lines += ["", "Artifacts: `timeseries.csv` (table view of the chart), `chart.png`, `samples.jsonl` (raw), "
              "`cloudwatch.json`, `discovery.json`, `benchmark.log`.", ""]
    return "\n".join(lines)


# --------------------------------------------------------------------------- chart
def plot(ts: pd.DataFrame, cw_doc: dict, f: dict, out: Path) -> None:
    plt.rcParams.update({"font.family": "sans-serif", "font.size": 9, "axes.titlesize": 10})
    fig, (ax_m, ax_l) = plt.subplots(2, 1, figsize=(11, 7.6), sharex=True,
                                     gridspec_kw={"height_ratios": [3, 2], "hspace": 0.12})
    fig.subplots_adjust(top=0.80, left=0.085, right=0.975, bottom=0.08)
    fig.patch.set_facecolor(SURFACE)
    if ts.empty:
        ax_m.text(0.5, 0.5, "no samples recorded", ha="center", va="center", color=INK2, transform=ax_m.transAxes)
        fig.savefig(out, dpi=150, facecolor=SURFACE)
        plt.close(fig)
        return
    x = ts["elapsed_s"] / 60.0
    t_fail_min = f["failure_elapsed_s"] / 60.0 if f.get("failure_elapsed_s") is not None else None
    pre = ts["elapsed_s"] <= (f["failure_elapsed_s"] if t_fail_min is not None else np.inf)

    def line(ax, col, label, color, scale=1.0, mask=None, **kw):
        s = pd.to_numeric(ts[col], errors="coerce") / scale
        if mask is not None:
            s = s.where(mask)
        if s.notna().sum() == 0:
            return None
        ok = s.notna()
        return ax.plot(x[ok], s[ok], color=color, lw=1.5, label=label, solid_joinstyle="round",
                       solid_capstyle="round", **kw)

    line(ax_m, "cumulative_alloc_mb", "Intended cumulative allocation", SLOT["alloc"], 1024, mask=pre,
         drawstyle="steps-post")
    line(ax_m, "browser_pmf_mb", "Chrome private footprint (all processes, memory-infra)", SLOT["pmf"], 1024)
    line(ax_m, "host_mem_inuse_excl_cache_mb", "VM memory in use, excl. reclaimable cache (sysfs)", SLOT["vm"], 1024)
    cw_mem = (cw_doc or {}).get("series", {}).get("memory") or {}
    t_origin = ts["t"].iloc[0] - ts["elapsed_s"].iloc[0]
    if cw_mem:
        # CloudWatch publishes a 1-minute bucket for every minute the browser tool had activity, so a
        # bucket at session_end+N minutes exists for other sessions on this tool, not for ours. Keep
        # only buckets whose timestamp falls inside the plot's own x-range (the full sample window).
        x_lo, x_hi = float(x.min()), float(x.max())
        points = sorted((((_epoch(k) - t_origin) / 60.0, v / 1024) for k, v in cw_mem.items()),
                        key=lambda p: p[0])
        points = [(xi, yi) for xi, yi in points if x_lo <= xi <= x_hi]
        if points:
            # Carry the final value out to the right edge rather than dropping to the next bucket,
            # which can lie off-chart and read as a crash on the AWS signal.
            cx, cy = [p[0] for p in points], [p[1] for p in points]
            cx.append(x_hi); cy.append(cy[-1])
            ax_m.plot(cx, cy, color=SLOT["aws"], lw=1.5, drawstyle="steps-post", marker="o", ms=6,
                      mec=SURFACE, mew=1.5, label="CloudWatch memory (1-min average)",
                      markevery=list(range(len(points))))
    if ts["usage_log_mem_mb"].notna().any():
        line(ax_m, "usage_log_mem_mb", "AWS USAGE_LOGS memory (per second)", SLOT["aws"], 1024, alpha=0.6)
    ceiling = f.get("mem_total_mb") or NOMINAL_CEILING_MB
    ax_m.axhline(ceiling / 1024, color=AXIS, lw=1.0, zorder=0)
    ax_m.annotate(f"VM MemTotal {ceiling:,.0f} MB — the fixed per-session ceiling",
                  (1.0, ceiling / 1024), xycoords=("axes fraction", "data"), xytext=(-4, -4),
                  textcoords="offset points", color=INK2, va="top", ha="right", fontsize=8.5)

    probe_label = ("Renderer probe: evaluate(1+1) on the page under pressure" if f["strategy"] == "inpage"
                   else "Renderer probe: evaluate(1+1) on a light tab (heavy tabs are separate)")
    pre_mask = ts["elapsed_s"] <= (f["failure_elapsed_s"] if t_fail_min is not None else np.inf)
    line(ax_l, "probe_latency_ms", probe_label, SLOT["probe"], mask=pre_mask)
    line(ax_l, "probe_browser_ms", "Browser-process probe: Browser.getVersion (≈ network RTT floor)",
         SLOT["browser"], mask=pre_mask)
    steps = pd.to_numeric(ts["alloc_step_ms"], errors="coerce")
    ok = steps.notna()
    if ok.any():
        ax_l.plot(x[ok], steps[ok], ls="none", marker="o", ms=6, color=SLOT["step"], mec=SURFACE, mew=1.5,
                  label="Allocation step duration")
    ax_l.set_yscale("log")
    ax_l.yaxis.set_major_locator(LogLocator(base=10, subs=(1.0, 2.0, 5.0), numticks=15))
    ax_l.yaxis.set_minor_formatter(NullFormatter())
    ax_l.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v / 1000:g} s" if v >= 1000 else f"{v:g} ms"))
    ax_m.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g} GB"))
    ax_m.set_ylim(0, max(ceiling / 1024 * 1.12, float(np.nanmax(pd.to_numeric(ts["cumulative_alloc_mb"], errors="coerce")
                                                                 .where(pre).fillna(0)) / 1024) * 1.08))

    # selective direct labels: values at the failure point (or at the end of the ramp)
    ref = f["failure_elapsed_s"] if t_fail_min is not None else float(ts["elapsed_s"].iloc[-1])
    used_y: list[float] = []
    for col, color in (("cumulative_alloc_mb", SLOT["alloc"]), ("browser_pmf_mb", SLOT["pmf"]),
                       ("host_mem_inuse_excl_cache_mb", SLOT["vm"])):
        s = pd.to_numeric(ts[col], errors="coerce").where(ts["elapsed_s"] <= ref).dropna()
        if s.empty:
            continue
        y = s.iloc[-1] / 1024
        xi = ts.loc[s.index[-1], "elapsed_s"] / 60.0
        ax_m.plot([xi], [y], marker="o", ms=6, color=color, mec=SURFACE, mew=1.5, zorder=5)
        if all(abs(y - u) > 0.18 for u in used_y):
            ax_m.annotate(f"{s.iloc[-1]:,.0f} MB", (xi, y), xytext=(-8, 0), textcoords="offset points",
                          ha="right", va="center", color=INK2, fontsize=8.5)
            used_y.append(y)

    if t_fail_min is not None:
        for ax in (ax_m, ax_l):
            ax.axvline(t_fail_min, color=CRITICAL, lw=1.2, zorder=4)
        crash = f.get("crash") or {}
        label = f"✕ {f['mode_short']}"
        if crash.get("exit_status"):
            label += f"\nChrome: {crash.get('status')} / {crash['exit_status']}"
        label += f"\n{fmt(f['alloc_at_failure_mb'], 'MB')} allocated · t+{f['failure_elapsed_s']:.0f} s"
        # Anchored to the top-left corner of the panel: a ramp leaves that corner empty, and the
        # ceiling label owns the top-right. The vline already marks where it happened.
        ax_m.annotate(label, (0.015, 0.97), xycoords="axes fraction", ha="left", va="top", color=INK,
                      fontsize=9, linespacing=1.4)

    for ax in (ax_m, ax_l):
        ax.set_facecolor(SURFACE)
        ax.grid(axis="y", color=GRID, lw=0.6)
        ax.set_axisbelow(True)
        for side in ("top", "right"):
            ax.spines[side].set_visible(False)
        for side in ("left", "bottom"):
            ax.spines[side].set_color(AXIS)
        ax.tick_params(colors=MUTED, labelcolor=INK2)
    # One figure-level legend in the reserved top band: an in-panel legend would collide with the
    # curves once they reach the ceiling.
    handles = [h for ax in (ax_m, ax_l) for h in ax.get_legend_handles_labels()[0]]
    labels = [l for ax in (ax_m, ax_l) for l in ax.get_legend_handles_labels()[1]]
    fig.legend(handles, labels, loc="upper left", bbox_to_anchor=(0.085, 0.915), ncol=2, frameon=False,
               fontsize=8.5, labelcolor=INK2, handlelength=1.8, columnspacing=2.0, borderpad=0)
    ax_m.set_ylabel("Memory", color=INK2)
    ax_l.set_ylabel("Round-trip latency (log)", color=INK2)
    ax_l.set_xlabel("Minutes since session start", color=INK2)
    p = f["params"]
    title = f"AgentCore Browser memory ramp — {f['strategy']}"
    if f["strategy"] == "inpage" and p.get("alloc_kind"):
        title += f" ({p['alloc_kind']}, {p.get('fill')} fill)"
    if p.get("step_mb") and p.get("interval_s"):
        title += f", +{p['step_mb']:,} MB every {p['interval_s']:g} s"
    fig.suptitle(title, x=0.085, ha="left", y=0.98, color=INK, fontsize=11.5, fontweight="bold")
    fig.text(0.085, 0.945, f"{f['mode_short']} · paged to disk: {f['paged']} · slowed before failing: "
             f"{f['slowdown']}", color=INK2, fontsize=9)
    fig.savefig(out, dpi=150, facecolor=SURFACE)
    plt.close(fig)


# --------------------------------------------------------------------------- entry points
def generate(run_dir: Path) -> dict:
    run_dir = Path(run_dir)
    meta = _load_json(run_dir / "run_meta.json")
    rows = load_rows(run_dir)
    cw_doc = _load_json(run_dir / "cloudwatch.json")
    usage_path = run_dir / "usage_logs.jsonl"
    usage = [json.loads(line) for line in usage_path.read_text().splitlines() if line.strip()] if usage_path.exists() else []
    ts, _events = build_timeseries(rows, meta, cw_doc, usage)
    findings = analyze(run_dir, rows, ts, meta, cw_doc, usage)
    ts.drop(columns=["t"]).to_csv(run_dir / "timeseries.csv", index=False, float_format="%.3f")
    plot(ts, cw_doc, findings, run_dir / "chart.png")
    (run_dir / "FINDINGS.md").write_text(render_findings(findings))
    (run_dir / "findings.json").write_text(json.dumps(findings, indent=1, default=str))
    return findings


def _idle_gb(ts: pd.DataFrame) -> float | None:
    if ts.empty:
        return None
    base = ts[ts["phase"] == "baseline"]
    for col in ("host_mem_inuse_excl_cache_mb", "browser_pmf_mb"):
        s = pd.to_numeric(base[col], errors="coerce").dropna()
        if len(s):
            return max(MIN_BILLED_GB, float(s.median()) / 1024.0)
    return None


TAB_LEVEL = ("oom_kill", "v8_heap_oom", "chrome_oom", "renderer_crash")
BROWSER_LEVEL = ("browser_restart", "connection_drop_oom")


def _span(vals, unit: str = "s", digits: int = 0) -> str:
    vals = [v for v in vals if v is not None]
    if not vals:
        return "n/a"
    lo, hi = min(vals), max(vals)
    return (f"{lo:.{digits}f} {unit}" if round(lo, digits) == round(hi, digits)
            else f"{lo:.{digits}f}–{hi:.{digits}f} {unit}")


def headline(runs: list[dict]) -> list[str]:
    """The three questions answered from every run that reached the limit, not from one chosen run."""
    failed = [r for r in runs if r.get("failure_event") and r.get("valid")]
    if not failed:
        return []
    caps = next((r["host_caps"] for r in failed if r.get("host_caps")), {})
    tab = [r for r in failed if r["mode"] in TAB_LEVEL]
    browser = [r for r in failed if r["mode"] in BROWSER_LEVEL]
    confirmed = [r for r in browser if r["mode"] == "browser_restart"]
    sigkill = [r for r in tab if str((r.get("crash") or {}).get("exit_status") or "").startswith("SIGKILL")]
    v8 = [r for r in tab if r["mode"] == "v8_heap_oom"]

    def kills(sel):
        pts = sorted({r["alloc_at_failure_mb"] for r in sel if r.get("alloc_at_failure_mb")})
        return " / ".join(f"{p:,.0f}" for p in pts) + " MB" if pts else "n/a"

    def single(fill):
        return [r for r in failed if r["strategy"] == "inpage" and r["params"].get("alloc_kind") == "arraybuffer"
                and r["params"].get("fill") == fill]

    rand, touch = single("random"), single("touch")
    lat_ratio = [r["probe_ramp"]["p50"] / r["probe"]["baseline_p50"] for r in failed
                 if (r.get("probe_ramp") or {}).get("p50") and (r.get("probe") or {}).get("baseline_p50")]
    step_ratio = [r["step_ms_last3_p50"] / r["step_ms_first5_p50"] for r in failed
                  if r.get("step_ms_first5_p50") and r.get("step_ms_last3_p50")]
    stall_tab = [r["failing_step_ms"] / 1000 for r in tab if r.get("failing_step_ms") and r["mode"] != "v8_heap_oom"]
    stall_browser = [r["failing_step_ms"] / 1000 for r in browser if r.get("failing_step_ms")]
    out = ["## The three answers", "", f"_From all {len(failed)} runs that reached the limit._", "",
           f"**1. Does it page to disk?** No. `SwapCached` peaked at "
           f"{max(r.get('swap_cached_max_mb') or 0 for r in failed):.0f} MB across every run, the VM has no zram or "
           f"zswap (`zswap_enabled={caps.get('zswap_enabled')}`, zram devices: {caps.get('zram_devices') or 'none'}), "
           f"and disk writes didn't rise. Under pressure Linux evicted up to "
           f"{max(r.get('cache_evicted_pct') or 0 for r in failed):.0f}% of its page cache instead."
           + (f" Compressible data failed at the same point as incompressible data ({kills(touch)} vs {kills(rand)}), "
              "so there is no hidden compression or de-duplication either." if touch and rand else ""), "",
           f"**2. Does it slow down first?** Not in a way you can use as a warning. Median round-trip latency during "
           f"the ramp was within {max(abs(r - 1) for r in lat_ratio) * 100:.0f}% of its baseline in every run, and "
           f"allocation steps got at most "
           f"{max(step_ratio):.1f}× slower near the limit. Then the final allocation stalled: {_span(stall_tab)} "
           f"before a tab was killed, {_span(stall_browser)} before Chrome restarted."
           + (" The JavaScript-heap crash came with no stall at all." if v8 else ""), "",
           "**3. What is the failure mode?** One of two outcomes, and the session survived both:", "",
           f"- **The tab dies and everything else keeps running ({len(tab)} runs).** The Linux OOM killer sent the "
           f"tab's process `SIGKILL` ({len(sigkill)} runs)"
           + (f", or, for JavaScript-heavy pages, Chrome crashed the tab itself at its JavaScript heap limit, at "
              f"{kills(v8)} ({len(v8)} run)" if v8 else "")
           + ". Chrome kept the same process and connection, and the other tabs stayed open.",
           f"- **Chrome restarts and every tab is lost ({len(browser)} runs).** The whole VM froze for "
           f"{_span(stall_browser)}, the CDP connection dropped, and Chrome came back as a new process"
           + (f". The process ID changed in {'both' if len(confirmed) == 2 else f'all {len(confirmed)}'} runs "
              "that checked it" if len(confirmed) > 1 else (". The process ID changed in the run that checked it"
                                                            if confirmed else ""))
           + (f"; the other {len(browser) - len(confirmed)} predate the check but show the same freeze, disconnect "
              "and return to idle memory" if 0 < len(confirmed) < len(browser) else "") + ".", "",
           "In every case `GetBrowserSession` still reported `READY`, and a new tab or connection worked within "
           "about a second.", ""]
    billed = [r for r in runs if (r.get("billing") or {}).get("billed_memory_mb")]
    if billed:
        shares = [r["billing"]["billed_memory_mb"]["share_at_full_vm"] * 100 for r in billed]
        medians = [r["billing"]["billed_memory_mb"]["median"] / 1024 for r in billed]
        idle = [r for r in billed if (r.get("max_allocated_ok_mb") or 0) <= 1]
        lows = [r["billing"]["billed_memory_mb"].get("min_whole_seconds", r["billing"]["billed_memory_mb"]["min"]) / 1024
                for r in billed]
        idle_use = [((r["billing"].get("memory_by_signal") or {}).get("vm_in_use_excl_cache") or {}).get("mean_gb")
                    for r in idle]
        out += [f"**Cost consequence.** AgentCore's per-second usage telemetry metered memory at a median of "
                f"{_span(medians, 'GB', 1)} per session across all {len(billed)} sessions with data, and never "
                f"below {min(lows):.1f} GB outside the session's final shutdown seconds"
                + (f", including {len(idle)} idle sessions in which the browser used only "
                   f"{_span(idle_use, 'GB', 1)}" if idle else "")
                + ". It neither tracked what the pages used nor behaved as a strict high-water mark. Memory "
                f"therefore costs roughly ${min(medians) * PRICE_GB_HOUR:.3f}–${max(medians) * PRICE_GB_HOUR:.3f} "
                "per session-hour whatever the pages do; the lever on memory cost is **session count × lifetime**. "
                "CPU does scale with work, and idle CPU is free.", ""]
    return out


def write_combined(runs_dir: Path, out: Path) -> None:
    """Top-level FINDINGS.md: every run in a table, plus the latest valid run of each strategy in full."""
    runs = []
    for path in sorted(Path(runs_dir).glob("*/findings.json")):
        try:
            runs.append(json.loads(path.read_text()))
        except ValueError:
            continue
    if not runs:
        return
    def completeness(r):
        """Prefer a run with the survivor check, then AWS's per-second usage data, then recency."""
        return (bool(r.get("survivors")),
                bool(((r.get("cloudwatch") or {}).get("usage_log_records") or 0) > 0),
                r.get("run_dir", ""))

    # One full worked example per outcome: the most complete recent run of each kind.
    best = {}
    for label, pick in (("Chrome restart", lambda r: r["mode"] == "browser_restart"),
                        ("tab killed (single page)", lambda r: r["mode"] in TAB_LEVEL and r["strategy"] == "inpage"),
                        ("tab killed (several tabs)", lambda r: r["mode"] in TAB_LEVEL and r["strategy"] == "tabs")):
        candidates = [r for r in runs if r.get("valid") and r.get("failure_event") and pick(r)]
        if candidates:
            best[label] = max(candidates, key=completeness)

    lines = ["# AgentCore Browser 4 GB memory ceiling — findings", "",
             "_Auto-generated by `benchmark.py` after every run (per-run reports live in `runs/<run-id>/`)._", ""]
    lines += headline(runs) + [
             "| run | strategy | failure mode | memory at failure | paged to disk? | slowed first? | memory after failure |",
             "|---|---|---|---:|---|---|---:|"]
    for r in runs:
        failed = bool(r.get("failure_event"))
        at = fmt(r["alloc_at_failure_mb"], "MB") if failed else f"— (stopped at {fmt(r['max_allocated_ok_mb'], 'MB')})"
        mode = MODE_SHORT.get(r["mode"], r["mode"]) + (" (inferred)" if r.get("oom_inferred") else "")
        lines.append(f"| `{r['run_dir']}` | {r['strategy']} | {mode} | {at} | {r['paged']} | {r['slowdown']}"
                     f" | {fmt(r.get('memory_delta_after_failure_mb'), 'MB')} |")
    lines.append("")

    # Reproducibility: the same failure mode at the same allocation across repeats is the strongest
    # claim this benchmark can make, so state it up front when there are repeats to compare.
    repeats = []
    configs: dict[tuple, list] = {}
    for r in runs:
        if r.get("failure_event"):
            p = r["params"]
            key = ((r["strategy"], p.get("alloc_kind"), p.get("fill"), p.get("step_mb")) if r["strategy"] == "inpage"
                   else (r["strategy"], None, None, p.get("step_mb")))
            configs.setdefault(key, []).append(r)
    for (strategy, kind, fill, step), hits in configs.items():
        if len(hits) < 2:
            continue
        modes = {MODE_SHORT.get(r["mode"], r["mode"]) for r in hits}
        at = [r["alloc_at_failure_mb"] for r in hits if r.get("alloc_at_failure_mb")]
        spread = (f"at {at[0]:,.0f} MB every time" if at and len(set(at)) == 1
                  else f"between {min(at):,.0f} and {max(at):,.0f} MB" if at else "")
        label = f"`{strategy}`" + (f" ({kind}, {fill} fill)" if kind else "") + f", {step} MB steps"
        repeats.append(f"- {label}: {len(hits)} runs reached the limit with "
                       f"{'the same failure mode' if len(modes) == 1 else 'differing failure modes'} "
                       f"({', '.join(sorted(modes))}) {spread}.")
    if repeats:
        lines += ["**Reproducibility**", "", *repeats, ""]

    billed_rows = []
    for r in runs:
        bm = (r.get("billing") or {}).get("billed_memory_mb")
        if not bm:
            continue
        observed = ((r.get("billing") or {}).get("memory_by_signal") or {}).get("vm_in_use_excl_cache") or {}
        billed_rows.append(
            f"| `{r['run_dir']}` | {fmt(r['max_allocated_ok_mb'], 'MB')} | "
            f"{observed.get('peak_gb', float('nan')):.2f} GB | {bm['median'] / 1024:.2f} GB | "
            f"{bm['share_at_full_vm'] * 100:.0f}% | {bm['n_records']} |")
    if billed_rows:
        lines += ["**Billed memory vs actual consumption** (from AWS's per-second `USAGE_LOGS`). Runs that "
                  "allocated almost nothing were billed the same as runs that reached the ceiling:", "",
                  "| run | allocated | observed peak (VM in use) | billed memory (median) | share of seconds "
                  "at full 4 GB | records |", "|---|---:|---:|---:|---:|---:|", *billed_rows, ""]

    for strategy, pick in best.items():
        body = (Path(runs_dir) / pick["run_dir"] / "FINDINGS.md").read_text().splitlines()
        lines += [f"## Example: {strategy}", "",
                  f"![chart](runs/{pick['run_dir']}/chart.png)", ""]
        lines += [("#" + ln if ln.startswith("#") else ln) for ln in body[1:]]
        lines.append("")
    out.write_text("\n".join(lines))
