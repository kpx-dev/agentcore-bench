#!/usr/bin/env python3
"""Benchmark what an AgentCore Browser session does at its fixed 4 GB memory ceiling.

Drives one session's memory upward in fixed steps (inside one renderer's heap, or by opening
heavy tabs), samples CloudWatch plus independent in-browser and guest-VM signals the whole
time, and records exactly how and when the session fails. See README.md.

    python benchmark.py --strategy inpage                 # run (default: +100 MB every 10 s)
    python benchmark.py --discover-only                   # list AgentCore CloudWatch metrics
    python benchmark.py --reanalyze runs/<run-id>         # re-pull late CloudWatch data + report
"""

from __future__ import annotations

import argparse
import asyncio
import atexit
import datetime as dt
import importlib.metadata
import json
import logging
import os
import platform
import signal
import sys
import time
from pathlib import Path

import boto3
from playwright.async_api import BrowserContext, Page, async_playwright

import analysis
from agentcore import AgentCoreBrowser
from cdp_signals import BrowserSignals, HostProbe, PageSignals, decode_exit_status, short, timed
from cloudwatch import CloudWatchMonitor
from strategies import InPageStrategy, TabsStrategy

log = logging.getLogger("bench")
HERE = Path(__file__).resolve().parent

SNAPSHOT_KEYS = (
    "cumulative_alloc_mb", "cdp_heap_mb", "browser_pmf_mb", "max_renderer_pmf_mb",
    "host_mem_used_mb", "host_mem_free_mb", "host_mem_avail_est_mb", "host_anon_mb", "host_file_mb",
    "host_swap_cached_mb", "cloudwatch_mem_mb", "cloudwatch_cpu", "probe_latency_ms",
    "probe_browser_ms", "alloc_step_ms", "chrome_cpu_pct", "alloc_renderer_cpu_pct",
)
NOTABLE_PROCESS_TYPES = {"browser", "renderer", "gpu"}


def utcnow() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


# --------------------------------------------------------------------------- recording
class Recorder:
    """Append-only JSONL of every sample and event, flushed per line so a killed run stays diagnostic."""

    def __init__(self, path: Path):
        self._f = path.open("a", buffering=1)
        self.latest: dict[str, tuple[float, float]] = {}

    def record(self, kind: str, **fields) -> dict:
        row = {"t": time.time(), "kind": kind, **fields}
        self._f.write(json.dumps(row, default=str) + "\n")
        for k, v in fields.items():
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                self.latest[k] = (row["t"], v)
        return row

    def value(self, key: str):
        return self.latest.get(key, (None, None))[1]

    def snapshot(self) -> dict:
        now = time.time()
        return {k: {"value": v, "age_s": round(now - t, 1)}
                for k, (t, v) in self.latest.items() if k in SNAPSHOT_KEYS}

    def event(self, name: str, **detail) -> dict:
        snap = self.snapshot()
        log.warning("EVENT %s %s", name, json.dumps(detail, default=str))
        if snap:
            log.warning("      last readings: %s",
                        ", ".join(f"{k}={v['value']} ({v['age_s']}s ago)" for k, v in snap.items()))
        return self.record("event", event=name, detail=detail, snapshot=snap)

    def close(self) -> None:
        self._f.close()


class RunState:
    def __init__(self) -> None:
        self.stop = asyncio.Event()      # wind everything down
        self.failed = asyncio.Event()    # terminal failure seen: stop allocating, observe the aftermath
        self.failure: dict | None = None
        self.crashes = 0
        self.closing = False             # we are closing the connection ourselves
        self.disconnected = False
        self.session_unhealthy = False
        self.labels_by_target: dict[str, str] = {}
        self.step_lock = asyncio.Lock()  # held while a pressure step runs (probes skip, not queue)
        self.cumulative_mb = 0
        self.in_flight_mb: int | None = None
        self.steps_started = 0           # lets analysis exclude deltas that span a step


# --------------------------------------------------------------------------- benchmark
class Benchmark:
    def __init__(self, args: argparse.Namespace, rec: Recorder, ac: AgentCoreBrowser,
                 cwmon: CloudWatchMonitor, meta: dict):
        self.args, self.rec, self.ac, self.cwmon, self.meta = args, rec, ac, cwmon, meta
        self.state = RunState()
        self.pages: dict[str, PageSignals] = {}
        self.alloc_renderer_pid: int | None = None
        self.browser_pid: int | None = None
        self.process_pmf_mb: dict[int, float] = {}
        self.browser = self.ctx = self.bsig = self.strategy = self.host = None

    async def run(self) -> None:
        self._install_signal_handlers()
        url, headers = self.ac.signed_ws_request()
        async with async_playwright() as pw:
            t0 = time.perf_counter()
            self.browser = await pw.chromium.connect_over_cdp(url, headers=headers, timeout=60_000)
            connect_ms = round((time.perf_counter() - t0) * 1000)
            self.browser.on("disconnected", self._on_disconnected)
            log.info("Connected over CDP in %d ms (Chrome %s)", connect_ms, self.browser.version)
            self.ctx = self.browser.contexts[0] if self.browser.contexts else await self.browser.new_context()
            self.bsig = BrowserSignals(await self.browser.new_browser_cdp_session())
            await self.bsig.watch_targets(self._on_target_crashed,
                                          lambda p: log.debug("targetDestroyed %s", p.get("targetId")))
            self.strategy = self._make_strategy()
            env = await self.strategy.setup()
            self.rec.record("env", connect_ms=connect_ms, chrome_version=self.browser.version, **env)
            log.info("Browser env: %s", env)
            if self.args.host_interval_s > 0:
                self.host = HostProbe(self.ctx, self.register_page)
                self.rec.record("host_caps", **await self.host.setup())

            loops = [self._page_loop(), self._browser_loop(), self._cw_loop(), self._health_loop()]
            if self.host and self.host.sig:
                loops.append(self._host_loop())
            tasks = [asyncio.create_task(c) for c in loops]
            try:
                await self._drive()
                await self._tail()
                await self._recovery_check(pw)
            finally:
                self.state.stop.set()
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                self.state.closing = True
                try:
                    await asyncio.wait_for(self.browser.close(), 15)
                except Exception as exc:  # noqa: BLE001 - the session is stopped right after anyway
                    log.debug("browser.close(): %s", short(exc))

    def _make_strategy(self):
        if self.args.strategy == "inpage":
            return InPageStrategy(self.ctx, self.register_page, self.args.step_mb, self.args.alloc_kind,
                                  self.args.fill, self.args.step_timeout_s)
        return TabsStrategy(self.ctx, self.register_page, self.args.step_mb, self.args.step_timeout_s)

    async def register_page(self, label: str, role: str, page: Page,
                            ctx: BrowserContext | None = None) -> PageSignals:
        sig = await PageSignals.attach(label, role, ctx or self.ctx, page)
        self.pages[label] = sig
        if sig.target_id:
            self.state.labels_by_target[sig.target_id] = label
        page.on("crash", lambda _p: self._on_crash(sig, "page.on(crash)"))
        page.on("close", lambda _p: self._on_close(sig))
        return sig

    # ------------------------------------------------------------ failure detection
    def _event(self, name: str, terminal: bool, **detail) -> dict:
        row = self.rec.event(name, terminal=terminal, **detail)
        if terminal and self.state.failure is None:
            self.state.failure = row
            self.state.failed.set()
            log.error("TERMINAL FAILURE: %s at %d MB cumulative allocation", name, self.state.cumulative_mb)
        return row

    def _on_crash(self, sig: PageSignals, source: str, **extra) -> None:
        if sig.crashed:
            return
        sig.crashed = True
        counts = sig.role == "strategy" and self.strategy.counts_as_failure(sig.label)
        if counts:
            self.state.crashes += 1
        self._event("page_crash", terminal=counts and self.state.crashes >= self.args.stop_after_crashes,
                    page=sig.label, role=sig.role, source=source, crashes=self.state.crashes,
                    in_flight_target_mb=self.state.in_flight_mb, **extra)

    def _on_target_crashed(self, params: dict) -> None:
        label = self.state.labels_by_target.get(params.get("targetId"))
        status, code = params.get("status"), params.get("errorCode")
        self._event("target_crashed", terminal=False, page=label or "untracked", status=status,
                    error_code=code, exit_status=decode_exit_status(code), target_id=params.get("targetId"))
        if label in self.pages:
            self._on_crash(self.pages[label], "Target.targetCrashed", status=status, error_code=code)

    def _on_close(self, sig: PageSignals) -> None:
        if sig.crashed or sig.closed or self.state.closing:
            return
        sig.closed = True
        counts = sig.role == "strategy" and self.strategy.counts_as_failure(sig.label)
        self._event("page_closed", terminal=counts, page=sig.label, role=sig.role)

    def _on_disconnected(self, _browser) -> None:
        self.state.disconnected = True
        if not self.state.closing:
            self._event("cdp_disconnected", terminal=True, in_flight_target_mb=self.state.in_flight_mb)

    def _install_signal_handlers(self) -> None:
        loop, task = asyncio.get_running_loop(), asyncio.current_task()

        def handler(name: str) -> None:
            if not self.state.stop.is_set():
                log.warning("%s received: stopping gracefully (send again to abort)", name)
                self._event("interrupted", terminal=False, signal=name)
                self.state.stop.set()
            elif task is not None:
                task.cancel()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, handler, sig.name)
            except NotImplementedError:  # pragma: no cover - Windows
                pass

    # ------------------------------------------------------------ driver
    async def _drive(self) -> None:
        a = self.args
        log.info("Baseline: sampling %ss before the first allocation", a.baseline_s)
        self.rec.record("phase", phase="baseline")
        await self._sleep(a.baseline_s, wake_on_failure=True)
        if self.state.stop.is_set() or self.state.failed.is_set():
            return
        self.rec.record("phase", phase="ramp")
        deadline = time.monotonic() + a.max_duration_s
        next_at = time.monotonic()
        step = 0
        while not (self.state.stop.is_set() or self.state.failed.is_set()):
            intended = (step + 1) * a.step_mb
            if intended > a.max_mb:
                log.info("Reached --max-mb %d without a terminal failure", a.max_mb)
                self._event("max_mb_reached", terminal=False, cumulative_alloc_mb=self.state.cumulative_mb)
                return
            if time.monotonic() > deadline:
                self._event("max_duration_reached", terminal=False, max_duration_s=a.max_duration_s)
                return
            step += 1
            self.state.steps_started = step
            log.info("step %d: +%d MB -> intended cumulative %d MB", step, a.step_mb, intended)
            self.rec.record("alloc_start", step=step, intended_cumulative_mb=intended)
            crashes_before = self.state.crashes
            self.state.in_flight_mb = intended
            async with self.state.step_lock:
                res = await self.strategy.step(step)
            self.state.in_flight_mb = None
            if res.ok:
                self.state.cumulative_mb = res.intended_cumulative_mb
                self.rec.record("alloc", step=step, ok=True, cumulative_alloc_mb=self.state.cumulative_mb,
                                alloc_step_ms=res.step_ms, **res.detail)
                self._log_step(step, res.step_ms)
            else:
                self.rec.record("alloc", step=step, ok=False, alloc_step_ms=res.step_ms,
                                intended_cumulative_mb=intended, error=res.error, timed_out=res.timed_out)
                log.warning("step %d failed after %.0f ms: %s", step, res.step_ms, res.error)
                # A crash/disconnect rejects the in-flight call slightly before its event arrives.
                await self._sleep(3, wake_on_failure=True)
                if self.state.failed.is_set() or self.state.stop.is_set():
                    return
                if self.state.crashes > crashes_before:
                    self.state.cumulative_mb = intended  # tabs: that tab died; keep ramping
                else:
                    self._event("alloc_step_timeout" if res.timed_out else "alloc_failed", terminal=True,
                                step=step, intended_cumulative_mb=intended, error=res.error)
                    return
            next_at += a.interval_s
            await self._sleep(max(0.0, next_at - time.monotonic()), wake_on_failure=True)

    def _log_step(self, step: int, step_ms: float) -> None:
        v = self.rec.value
        parts = [f"step {step} ok in {step_ms:.0f} ms", f"cumulative {self.state.cumulative_mb} MB"]
        for key, label in (("cdp_heap_mb", "cdp heap"), ("browser_pmf_mb", "chrome PMF"),
                           ("host_mem_free_mb", "VM free"), ("host_mem_avail_est_mb", "VM avail~"),
                           ("probe_latency_ms", "probe"), ("probe_browser_ms", "browser probe")):
            if v(key) is not None:
                unit = "ms" if key.endswith("_ms") else "MB"
                parts.append(f"{label} {v(key):.0f} {unit}")
        log.info(" | ".join(parts))

    async def _tail(self) -> None:
        if self.state.stop.is_set():
            return
        why = self.state.failure["event"] if self.state.failure else "end of ramp"
        log.info("Observing %ss after %s: does memory drop (kill), plateau, or keep climbing (paging)?",
                 self.args.post_failure_s, why)
        self.rec.record("phase", phase="post", reason=why)
        await self._sleep(self.args.post_failure_s)

    async def _recovery_check(self, pw) -> None:
        """After a failure, prove whether the session is still usable — and re-read guest memory.

        When the CDP connection drops, every sampler dies with it, so the only way to see whether the
        VM got its memory back (kill) or is still pinned near the ceiling (paging) is to reconnect and
        read meminfo again.
        """
        if not self.state.failed.is_set() or self.state.stop.is_set():
            return
        t0 = time.perf_counter()
        via = "reconnect" if self.state.disconnected else "new_page"
        other = None
        try:
            if self.state.disconnected:
                url, headers = self.ac.signed_ws_request()
                other = await pw.chromium.connect_over_cdp(url, headers=headers, timeout=30_000)
                ctx = other.contexts[0] if other.contexts else await other.new_context()
            else:
                ctx = self.ctx
            page = await asyncio.wait_for(ctx.new_page(), 30)
            await asyncio.wait_for(page.set_content("<title>recovery</title>ok"), 30)
            ok = await asyncio.wait_for(page.evaluate("1+1"), 30) == 2
            ms = round((time.perf_counter() - t0) * 1000)
            survivors = await self._survivors(other or self.browser, ctx)
            self._event("recovery_ok" if ok else "recovery_failed", terminal=False, via=via, ms=ms, **survivors)
            await self._post_failure_host_sample(ctx)
        except Exception as exc:  # noqa: BLE001
            self._event("recovery_failed", terminal=False, via=via, error=short(exc),
                        ms=round((time.perf_counter() - t0) * 1000))
        finally:
            if other is not None:
                try:
                    await other.close()
                except Exception:  # noqa: BLE001
                    pass

    async def _survivors(self, browser, ctx) -> dict:
        """What outlived the failure, observed over CDP: the same Chrome browser process (PID) or a
        restarted one, which page targets remain, and whether the page under pressure still answers.

        A dropped connection closes every Playwright page object, so "page closed" alone cannot tell
        a killed renderer from a dropped stream; asking the surviving browser can."""
        out: dict = {"browser_pid_before": self.browser_pid}
        try:
            cdp = await browser.new_browser_cdp_session()
            procs = (await asyncio.wait_for(cdp.send("SystemInfo.getProcessInfo"), 15))["processInfo"]
            pid = next((p["id"] for p in procs if p["type"] == "browser"), None)
            out["browser_pid_after"] = pid
            if pid is not None and self.browser_pid is not None:
                out["same_browser_process"] = pid == self.browser_pid
            targets = (await asyncio.wait_for(cdp.send("Target.getTargets"), 15))["targetInfos"]
            out["surviving_pages"] = sorted(t.get("title") or t.get("url", "")
                                            for t in targets if t.get("type") == "page"
                                            and t.get("title") != "recovery")
        except Exception as exc:  # noqa: BLE001
            out["survivor_check_error"] = short(exc)
        # Does the page that held the memory still answer, and does it still hold its data? A crashed
        # tab keeps its target (and title) in the browser but cannot evaluate anything.
        pressured = {"inpage": "bench-alloc"}.get(self.args.strategy)
        if pressured:
            answered = None
            for page in ctx.pages:
                try:
                    title, held = await asyncio.wait_for(page.evaluate(
                        "[document.title, globalThis.__bench ? globalThis.__bench.mb : null]"), 10)
                except Exception:  # noqa: BLE001 - dead renderer, or a page we cannot evaluate
                    continue
                if title == pressured:
                    answered = held
                    break
            if answered is not None:
                out["pressured_page"] = f"alive, still holding {answered} MB"
            elif pressured in out.get("surviving_pages", []):
                out["pressured_page"] = "dead: its target remains but its renderer no longer answers"
            else:
                out["pressured_page"] = "gone: no target with its title remains"
        log.info("After the failure: %s", out)
        return out

    async def _post_failure_host_sample(self, ctx) -> None:
        """Read guest meminfo over a (possibly new) connection and record it as a post-failure sample."""
        if self.args.host_interval_s <= 0:
            return
        # Bind the registration to the (possibly new) context: self.ctx died with the old connection.
        probe = HostProbe(ctx, lambda label, role, page: self.register_page(label, role, page, ctx))
        try:
            await probe._open()  # noqa: SLF001 - same module's helper, reused deliberately
            fields = await asyncio.wait_for(probe.sample(), self.args.probe_timeout_s * 2)
            if fields:
                self.rec.record("host", after_failure=1, **fields)
                log.info("Post-failure guest memory: free %s MB, in use excl. cache %s MB",
                         fields.get("host_mem_free_mb"),
                         round(fields.get("host_mem_total_mb", 0) - fields.get("host_mem_avail_est_mb", 0), 1))
        except Exception as exc:  # noqa: BLE001
            log.warning("Post-failure host sample failed: %s", short(exc))

    # ------------------------------------------------------------ samplers
    async def _page_loop(self) -> None:
        while not self.state.stop.is_set() and not self.state.disconnected:
            try:
                await self._page_sample()
            except Exception as exc:  # noqa: BLE001
                log.debug("page sample failed: %s", short(exc))
            await self._sleep(self.args.sample_interval_s)

    async def _page_sample(self) -> None:
        if self.state.step_lock.locked():
            # Never queue a probe behind an allocation: that would time the allocation, not the browser.
            self.rec.record("page", step_in_flight=1, steps_started=self.state.steps_started)
            return
        fields: dict = {"steps_started": self.state.steps_started}
        probe = self.strategy.probe
        timeout = self.args.probe_timeout_s
        if probe and probe.alive:
            try:
                _, ms = await timed(probe.page.evaluate("1+1"), timeout)
                fields["probe_latency_ms"] = round(ms, 1)
            except asyncio.TimeoutError:
                self._event("probe_timeout", terminal=False, page=probe.label, timeout_s=timeout)
            except Exception as exc:  # noqa: BLE001
                fields["probe_error"] = short(exc)
            try:
                fields.update(await asyncio.wait_for(probe.main_thread(), timeout))
            except Exception:  # noqa: BLE001
                pass
        targets = self.strategy.heap_targets()
        if targets:
            results = await asyncio.gather(*(asyncio.wait_for(t.heap(), timeout) for t in targets),
                                           return_exceptions=True)
            heaps = [r for r in results if isinstance(r, dict)]
            if heaps:
                js = sum(h["js_heap_used_mb"] for h in heaps)
                backing = sum(h["backing_store_mb"] for h in heaps)
                fields.update(cdp_js_heap_used_mb=round(js, 1), cdp_backing_store_mb=round(backing, 1),
                              cdp_heap_mb=round(js + backing, 1), cdp_heap_pages=len(heaps))
        if len(fields) > 1:
            self.rec.record("page", **fields)

    async def _browser_loop(self) -> None:
        a = self.args
        every = max(1, round(a.memdump_interval_s / a.sample_interval_s)) if a.memdump_interval_s > 0 else 0
        n = dump_failures = 0
        while not self.state.stop.is_set() and not self.state.disconnected:
            fields: dict = {"during_step": int(self.state.step_lock.locked()),
                            "steps_started": self.state.steps_started}
            try:
                fields["probe_browser_ms"] = round(await self.bsig.probe_ms(a.probe_timeout_s), 1)
            except asyncio.TimeoutError:
                self._event("browser_probe_timeout", terminal=False, timeout_s=a.probe_timeout_s)
            except Exception as exc:  # noqa: BLE001
                fields["browser_probe_error"] = short(exc)
            cpu = None
            try:
                cpu = await self.bsig.process_cpu(a.probe_timeout_s)
                fields.update({k: cpu[k] for k in ("n_processes", "n_renderers", "chrome_cpu_pct") if k in cpu})
                self.browser_pid = next((pid for pid, t in cpu["pid_types"].items() if t == "browser"),
                                        self.browser_pid)
                for pid, ptype in (cpu.get("vanished") or {}).items():
                    last_mb = self.process_pmf_mb.get(pid)
                    if ptype in NOTABLE_PROCESS_TYPES and (pid == self.alloc_renderer_pid
                                                           or (last_mb or 0) >= 50):
                        self._event("process_vanished", terminal=False, pid=pid, process_type=ptype,
                                    last_known_pmf_mb=last_mb,
                                    was_largest_renderer=pid == self.alloc_renderer_pid)
            except Exception as exc:  # noqa: BLE001
                log.debug("process info failed: %s", short(exc))
            if every and n % every == 0 and dump_failures < 5:
                try:
                    dump = await self.bsig.memory_dump(a.probe_timeout_s)
                    procs = dump.pop("processes")
                    self.process_pmf_mb = {p["pid"]: p["pmf_mb"] for p in procs}
                    self.rec.record("memdump", processes=procs)
                    fields.update(dump)
                    self.alloc_renderer_pid = dump.get("max_renderer_pid")
                    dump_failures = 0
                except Exception as exc:  # noqa: BLE001
                    dump_failures += 1
                    log.warning("memory-infra dump failed (%d in a row): %s", dump_failures, short(exc))
                    await self.bsig.reset_tracing()
                    if dump_failures == 5:
                        log.warning("Disabling memory-infra dumps after 5 consecutive failures")
            if cpu and self.alloc_renderer_pid in (cpu.get("per_pid_cpu_pct") or {}):
                fields["alloc_renderer_cpu_pct"] = cpu["per_pid_cpu_pct"][self.alloc_renderer_pid]
            self.rec.record("browser", **fields)
            n += 1
            await self._sleep(a.sample_interval_s)

    async def _host_loop(self) -> None:
        while not self.state.stop.is_set() and not self.state.disconnected:
            try:
                fields = await asyncio.wait_for(self.host.sample(), self.args.probe_timeout_s * 2)
                if fields:
                    self.rec.record("host", during_step=int(self.state.step_lock.locked()), **fields)
            except asyncio.TimeoutError:
                self._event("host_probe_timeout", terminal=False, timeout_s=self.args.probe_timeout_s * 2)
            except Exception as exc:  # noqa: BLE001
                log.debug("host sample failed: %s", short(exc))
            await self._sleep(self.args.host_interval_s)

    async def _cw_loop(self) -> None:
        start = self.ac.session.created_at - dt.timedelta(minutes=2)
        session_id = self.ac.session.session_id
        last_discovery = time.monotonic()
        while not self.state.stop.is_set():
            try:
                if time.monotonic() - last_discovery >= self.args.cw_rediscover_s:
                    if not self.cwmon.namespaces:
                        await asyncio.to_thread(self.cwmon.discover_namespaces)
                    await asyncio.to_thread(self.cwmon.discover_metrics, session_id, True)
                    last_discovery = time.monotonic()
                cols = {"memory": "cloudwatch_mem_mb", "cpu": "cloudwatch_cpu",
                        "sessions": "cloudwatch_active_sessions"}
                units = {"memory": "MB", "cpu": "vCPU", "sessions": "session(s)"}
                for signal_, ts, value in await asyncio.to_thread(self.cwmon.poll, start, utcnow()):
                    self.rec.record("cw", cw_timestamp=ts.isoformat(), **{cols[signal_]: round(value, 3)})
                    log.info("CloudWatch %s datapoint %s -> %.3f %s", signal_, ts.isoformat(), value,
                             units[signal_])
            except Exception as exc:  # noqa: BLE001
                log.warning("CloudWatch poll failed: %s", short(exc))
            await self._sleep(self.args.cw_interval_s)

    async def _health_loop(self) -> None:
        errors = 0
        while not self.state.stop.is_set():
            try:
                status = (await asyncio.to_thread(self.ac.get)).get("status")
                errors = 0
                self.rec.record("session", session_status=status)
                if status != "READY" and not self.state.closing and not self.state.session_unhealthy:
                    self.state.session_unhealthy = True
                    self._event("session_unhealthy", terminal=True, status=status)
            except Exception as exc:  # noqa: BLE001
                errors += 1
                self.rec.record("session", session_error=short(exc))
                if errors == 3 and not self.state.session_unhealthy:
                    self.state.session_unhealthy = True
                    self._event("session_unhealthy", terminal=True, error=short(exc))
            await self._sleep(self.args.health_interval_s)

    async def _sleep(self, seconds: float, wake_on_failure: bool = False) -> None:
        waits = [asyncio.ensure_future(self.state.stop.wait())]
        if wake_on_failure:
            waits.append(asyncio.ensure_future(self.state.failed.wait()))
        try:
            await asyncio.wait(waits, timeout=seconds, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for w in waits:
                w.cancel()


# --------------------------------------------------------------------------- post-run collection
def collect_cloudwatch(cwmon: CloudWatchMonitor, meta: dict, run_dir: Path, wait_s: float,
                       usage_log_group: str | None, poll_s: float = 30) -> None:
    """Final CloudWatch/USAGE_LOGS pull for the session window (the session is already stopped)."""
    session_id = meta["session_id"]
    created = dt.datetime.fromisoformat(meta["session_created_at"])
    stopped = dt.datetime.fromisoformat(meta.get("session_stopped_at") or utcnow().isoformat())
    start, end = created - dt.timedelta(minutes=2), stopped + dt.timedelta(minutes=3)
    target = stopped.replace(second=0, microsecond=0) - dt.timedelta(minutes=1)
    deadline = time.monotonic() + wait_s
    covered = False
    try:
        while True:
            if not cwmon.namespaces:
                cwmon.discover_namespaces()
            cwmon.discover_metrics(session_id)
            cwmon.poll(start, min(end, utcnow()))
            mem = cwmon.series("memory")
            last = dt.datetime.fromisoformat(max(mem)) if mem else None
            covered = last is not None and last >= target
            if covered:
                log.info("CloudWatch memory datapoints cover the session (last %s)", last.isoformat())
                break
            if time.monotonic() >= deadline:
                log.warning("CloudWatch resource-usage data incomplete (last datapoint: %s). AWS documents "
                            "up to 60 min delay; re-pull later with: python benchmark.py --reanalyze %s",
                            last.isoformat() if last else "none", run_dir)
                break
            log.info("Waiting for CloudWatch resource-usage datapoints (%.0fs left)...",
                     deadline - time.monotonic())
            time.sleep(min(poll_s, max(1.0, deadline - time.monotonic())))
    except KeyboardInterrupt:
        log.warning("Skipping the rest of the CloudWatch wait")

    groups = [usage_log_group] if usage_log_group else cwmon.find_usage_log_groups()
    records = []
    for group in groups:
        records.extend(cwmon.fetch_usage_logs(group, session_id, start, end))

    # These signals arrive late, so --reanalyze is expected to be run repeatedly. A pull that fails
    # (expired credentials, throttling) must never erase what an earlier pull already collected.
    usage_path = run_dir / "usage_logs.jsonl"
    if records or not usage_path.exists():
        usage_path.write_text("".join(json.dumps(r) + "\n" for r in records))
    else:
        log.warning("Keeping the %d previously collected USAGE_LOGS record(s): this pull returned none",
                    sum(1 for _ in usage_path.open()))

    cw_path = run_dir / "cloudwatch.json"
    previous = json.loads(cw_path.read_text()) if cw_path.exists() else {}
    series = {sig: cwmon.series(sig) for sig in ("memory", "cpu", "sessions")}
    raw = dict(cwmon.raw_datapoints)
    # Merge datapoint-wise so a partial pull adds to, rather than replaces, the record.
    for sig, pts in (previous.get("series") or {}).items():
        series[sig] = (pts or {}) | (series.get(sig) or {})
    for key, pts in (previous.get("raw_datapoints") or {}).items():
        raw[key] = (pts or {}) | (raw.get(key) or {})
    if cwmon.errors and not any(series.values()):
        log.warning("CloudWatch pull failed (%s); keeping the previously collected series",
                    cwmon.errors[-1][:120])
    cw_path.write_text(json.dumps({
        "collected_at": utcnow().isoformat(),
        "window": [start.isoformat(), end.isoformat()],
        "covers_session": covered or bool(previous.get("covers_session")),
        "discovery": cwmon.discovery_report(),
        "series": series,
        "raw_datapoints": raw,
        "usage_log_groups": groups or previous.get("usage_log_groups"),
        "usage_log_records": sum(1 for _ in usage_path.open()) if usage_path.exists() else 0,
    }, indent=1, default=str))


# --------------------------------------------------------------------------- entry points
def parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--strategy", choices=["inpage", "tabs"], default="inpage",
                   help="inpage: retained allocations in one renderer; tabs: one heavy tab per step")
    p.add_argument("--step-mb", type=int, default=100, help="memory added per step")
    p.add_argument("--interval-s", type=float, default=10.0, help="seconds between step starts")
    p.add_argument("--max-mb", type=int, default=6144,
                   help="stop ramping at this intended cumulative allocation (default is 1.5x the ceiling)")
    p.add_argument("--region", default=os.environ.get("AGENTCORE_REGION", "us-west-2"),
                   help="AWS region (env AGENTCORE_REGION)")
    p.add_argument("--browser-id", default=os.environ.get("AGENTCORE_BROWSER_ID", "aws.browser.v1"),
                   help="browser identifier (env AGENTCORE_BROWSER_ID)")
    p.add_argument("--alloc-kind", choices=["arraybuffer", "jsheap"], default="arraybuffer",
                   help="inpage only. arraybuffer: off-V8-heap backing stores (hits the VM ceiling); "
                        "jsheap: V8 heap objects (hits jsHeapSizeLimit first, maximises GC work)")
    p.add_argument("--fill", choices=["random", "touch"], default="random",
                   help="random: incompressible bytes; touch: one write per page (compressible)")
    p.add_argument("--sample-interval-s", type=float, default=2.0, help="latency probe / CDP sample cadence")
    p.add_argument("--memdump-interval-s", type=float, default=6.0,
                   help="memory-infra per-process dump cadence (0 disables)")
    p.add_argument("--host-interval-s", type=float, default=5.0,
                   help="guest sysfs meminfo/diskstats cadence (0 disables)")
    p.add_argument("--cw-interval-s", type=float, default=10.0, help="CloudWatch GetMetricData cadence")
    p.add_argument("--cw-rediscover-s", type=float, default=60.0, help="CloudWatch ListMetrics re-discovery cadence")
    p.add_argument("--cw-period-s", type=int, default=60, help="GetMetricData period (vended at 1-minute resolution)")
    p.add_argument("--cw-final-wait-s", type=float, default=240.0,
                   help="after the session stops, wait up to this long for late CloudWatch datapoints")
    p.add_argument("--namespace", action="append", default=[e for e in [os.environ.get("AGENTCORE_CW_NAMESPACE")] if e],
                   help="extra CloudWatch namespace to include if listing has not caught up (env AGENTCORE_CW_NAMESPACE)")
    p.add_argument("--namespace-scan-pages", type=int, default=20, help="ListMetrics pages to scan for namespaces")
    p.add_argument("--usage-log-group", help="CloudWatch Logs group with USAGE_LOGS (default: discover)")
    p.add_argument("--health-interval-s", type=float, default=15.0, help="GetBrowserSession cadence")
    p.add_argument("--baseline-s", type=float, default=20.0, help="sampling before the first step")
    p.add_argument("--post-failure-s", type=float, default=60.0,
                   help="keep observing this long after the failure (or the end of the ramp)")
    p.add_argument("--stop-after-crashes", type=int, default=1, help="terminal after this many strategy-page crashes")
    p.add_argument("--probe-timeout-s", type=float, default=30.0)
    p.add_argument("--step-timeout-s", type=float, default=180.0)
    p.add_argument("--session-timeout-s", type=int, default=3600, help="sessionTimeoutSeconds (service-side backstop)")
    p.add_argument("--max-duration-s", type=float, default=1800.0, help="hard cap on the ramp phase")
    p.add_argument("--out-dir", type=Path, default=HERE / "runs")
    p.add_argument("--discover-only", action="store_true", help="list AgentCore CloudWatch metrics and exit")
    p.add_argument("--reanalyze", type=Path, metavar="RUN_DIR",
                   help="re-pull CloudWatch/USAGE_LOGS for a finished run and regenerate its reports")
    p.add_argument("--verbose", action="store_true")
    args = p.parse_args(argv)
    if args.step_mb <= 0 or args.max_mb < args.step_mb:
        p.error("--step-mb must be > 0 and <= --max-mb")
    if args.sample_interval_s <= 0 or args.interval_s <= 0:
        p.error("intervals must be > 0")
    return args


def setup_logging(run_dir: Path | None, verbose: bool) -> None:
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(logging.DEBUG if verbose else logging.INFO)
    console.setFormatter(fmt)
    root.addHandler(console)
    if run_dir is not None:
        file_handler = logging.FileHandler(run_dir / "benchmark.log")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(fmt)
        root.addHandler(file_handler)
    # matplotlib's font manager alone emits thousands of DEBUG lines per chart, which buries the
    # benchmark's own diagnostics in the log file.
    for noisy in ("botocore", "boto3", "urllib3", "asyncio", "matplotlib", "PIL"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def versions() -> dict:
    out = {"python": platform.python_version()}
    for pkg in ("boto3", "botocore", "playwright", "pandas", "matplotlib"):
        try:
            out[pkg] = importlib.metadata.version(pkg)
        except importlib.metadata.PackageNotFoundError:
            out[pkg] = None
    return out


def write_meta(run_dir: Path, meta: dict) -> None:
    (run_dir / "run_meta.json").write_text(json.dumps(meta, indent=1, default=str))


def _raise_interrupt(signum, _frame):
    raise KeyboardInterrupt(f"signal {signum}")


def new_monitor(boto_session: boto3.Session, args, region: str, browser_id: str) -> CloudWatchMonitor:
    cwmon = CloudWatchMonitor(boto_session, region, browser_id, args.namespace_scan_pages, args.cw_period_s)
    cwmon.namespaces = sorted(set(args.namespace))
    return cwmon


def reanalyze(args) -> int:
    run_dir = args.reanalyze.resolve()
    setup_logging(None, args.verbose)
    meta = json.loads((run_dir / "run_meta.json").read_text())
    if meta.get("session_id"):
        session = boto3.Session(region_name=meta["region"])
        cwmon = new_monitor(session, args, meta["region"], meta["browser_id"])
        cwmon.discover_namespaces()
        collect_cloudwatch(cwmon, meta, run_dir, 0, args.usage_log_group or meta.get("usage_log_group"))
    findings = analysis.generate(run_dir)
    analysis.write_combined(run_dir.parent, HERE / "FINDINGS.md")
    print(findings["summary_line"])
    return 0


def main(argv=None) -> int:
    args = parse_args(argv)
    if args.reanalyze:
        return reanalyze(args)

    signal.signal(signal.SIGTERM, _raise_interrupt)
    boto_session = boto3.Session(region_name=args.region)
    run_id = utcnow().strftime("%Y%m%dT%H%M%SZ")
    run_dir = None
    if not args.discover_only:
        run_dir = args.out_dir / f"{run_id}-{args.strategy}"
        run_dir.mkdir(parents=True, exist_ok=False)
    setup_logging(run_dir, args.verbose)

    identity = boto_session.client("sts").get_caller_identity()
    log.info("AWS identity %s (account %s), region %s, browser %s",
             identity["Arn"], identity["Account"], args.region, args.browser_id)
    cwmon = new_monitor(boto_session, args, args.region, args.browser_id)
    cwmon.discover_namespaces()
    cwmon.discover_metrics(session_id=None)
    if args.discover_only:
        report = cwmon.discovery_report() | {"usage_log_groups": cwmon.find_usage_log_groups()}
        report.pop("all_namespaces_scanned")
        print(json.dumps(report, indent=1, default=str))
        return 0

    ac = AgentCoreBrowser(boto_session, args.region, args.browser_id)
    atexit.register(ac.stop)  # last-resort teardown; stop() is idempotent
    meta = {
        "run_id": run_id, "strategy": args.strategy, "region": args.region, "browser_id": args.browser_id,
        "account": identity["Account"], "caller_arn": identity["Arn"], "argv": sys.argv[1:],
        "args": {k: str(v) if isinstance(v, Path) else v for k, v in vars(args).items()},
        "versions": versions(), "usage_log_group": args.usage_log_group,
    }
    others = ac.count_ready_sessions()
    meta["other_ready_sessions_at_start"] = others
    if others:
        log.warning("%d other READY session(s) on %s: CloudWatch resource metrics are aggregated per browser "
                    "tool, so they will include those sessions too.", others, args.browser_id)
    write_meta(run_dir, meta)
    (run_dir / "discovery.json").write_text(json.dumps(cwmon.discovery_report(), indent=1, default=str))

    rec = Recorder(run_dir / "samples.jsonl")
    exit_code = 0
    try:
        session = ac.start(f"mem-bench-{args.strategy}-{run_id}", args.session_timeout_s)
        meta.update(session_id=session.session_id, session_created_at=session.created_at.isoformat(),
                    automation_endpoint=session.automation_endpoint)
        write_meta(run_dir, meta)
        cwmon.discover_metrics(session.session_id)
        asyncio.run(Benchmark(args, rec, ac, cwmon, meta).run())
    except (KeyboardInterrupt, asyncio.CancelledError):
        log.warning("Interrupted; tearing down")
        rec.record("event", event="aborted", detail={"reason": "interrupted"}, snapshot=rec.snapshot())
        exit_code = 130
    except Exception as exc:  # noqa: BLE001 - keep the partial run diagnosable
        log.exception("Benchmark aborted")
        rec.record("event", event="runner_error", detail={"error": short(exc)}, snapshot=rec.snapshot())
        exit_code = 1
    finally:
        signal.signal(signal.SIGTERM, _raise_interrupt)
        ac.stop()
        if ac.session is not None:
            meta["session_stopped_at"] = ac.session.stopped_at.isoformat()
        meta["ended_at"] = utcnow().isoformat()
        write_meta(run_dir, meta)
        rec.close()

    if meta.get("session_id"):
        collect_cloudwatch(cwmon, meta, run_dir, args.cw_final_wait_s, args.usage_log_group)
    findings = analysis.generate(run_dir)
    analysis.write_combined(args.out_dir, HERE / "FINDINGS.md")
    log.info("Artifacts in %s", run_dir)
    log.info(findings["summary_line"])
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
