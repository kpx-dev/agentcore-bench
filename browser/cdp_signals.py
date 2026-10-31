"""In-browser signals gathered over CDP, independent of CloudWatch.

* PageSignals   - per-page V8 heap / ArrayBuffer backing store (Runtime.getHeapUsage) and
                  main-thread counters (Performance.getMetrics).
* BrowserSignals - browser-target round-trip probe, per-process CPU (SystemInfo.getProcessInfo)
                  and per-process private memory footprint via memory-infra trace dumps.
* HostProbe     - the guest VM's own view of memory and disk I/O, read from sysfs through a
                  file:// tab (procfs reads come back empty in Chrome, sysfs is readable).
"""

from __future__ import annotations

import asyncio
import logging
import re
import signal
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Awaitable, Callable

from playwright.async_api import BrowserContext, CDPSession, Page

log = logging.getLogger("bench.cdp")

MB = 2**20

TRACE_CONFIG = {
    "traceConfig": {
        "includedCategories": ["disabled-by-default-memory-infra"],
        "excludedCategories": ["*"],
        "memoryDumpConfig": {"triggers": []},
    },
    "transferMode": "ReportEvents",
}


def decode_exit_status(code) -> str | None:
    """Decode the raw waitpid() status Chrome reports as Target.targetCrashed errorCode on Linux."""
    if not isinstance(code, int) or code <= 0:
        return None
    if code & 0x7F == 0:
        return f"exit({code >> 8})"
    try:
        name = signal.Signals(code & 0x7F).name
    except ValueError:
        name = f"signal {code & 0x7F}"
    # 0x80 is the core-dump flag; SIGKILL never dumps core, so 137 here is the shell-style 128+9.
    return name + (" (core dumped)" if code & 0x80 and code & 0x7F != signal.SIGKILL else "")


def short(exc: BaseException) -> str:
    text = str(exc).strip().splitlines()
    return f"{type(exc).__name__}: {text[0][:200] if text else ''}"


async def timed(awaitable: Awaitable, timeout_s: float):
    """Await with a timeout; returns (result, elapsed_ms)."""
    t0 = time.perf_counter()
    result = await asyncio.wait_for(awaitable, timeout_s)
    return result, (time.perf_counter() - t0) * 1000.0


# --------------------------------------------------------------------------- pages
@dataclass
class PageSignals:
    label: str
    role: str                      # "strategy" pages are under pressure; "observer" pages watch
    page: Page
    cdp: CDPSession
    target_id: str | None
    crashed: bool = False
    closed: bool = False

    @classmethod
    async def attach(cls, label: str, role: str, ctx: BrowserContext, page: Page) -> "PageSignals":
        cdp = await ctx.new_cdp_session(page)
        target_id = None
        try:
            info = await cdp.send("Target.getTargetInfo")
            target_id = info["targetInfo"]["targetId"]
        except Exception as exc:  # noqa: BLE001 - optional enrichment only
            log.debug("Target.getTargetInfo failed for %s: %s", label, short(exc))
        await cdp.send("Performance.enable")
        return cls(label, role, page, cdp, target_id)

    @property
    def alive(self) -> bool:
        return not (self.crashed or self.closed or self.page.is_closed())

    async def heap(self) -> dict:
        usage = await self.cdp.send("Runtime.getHeapUsage")
        return {
            "js_heap_used_mb": usage["usedSize"] / MB,
            "js_heap_total_mb": usage["totalSize"] / MB,
            # ArrayBuffer backing stores live outside the V8 heap; Chrome reports them separately.
            "backing_store_mb": usage.get("backingStorageSize", 0) / MB,
        }

    async def main_thread(self) -> dict:
        metrics = {m["name"]: m["value"] for m in (await self.cdp.send("Performance.getMetrics"))["metrics"]}
        return {"main_task_s": metrics.get("TaskDuration"), "main_thread_cpu_s": metrics.get("ThreadTime")}


# --------------------------------------------------------------------------- browser
def _num(v) -> int:
    return int(v, 16) if isinstance(v, str) else int(v)


def _process_type(name: str) -> str:
    n = name.lower()
    if n == "browser":
        return "browser"
    if "extension" in n:
        return "extension"
    if "renderer" in n:
        return "renderer"
    if "gpu" in n:
        return "gpu"
    return "utility"


def parse_memory_dump(events: list[dict]) -> dict:
    """Aggregate a memory-infra trace into per-process private footprint / peak RSS / allocators."""
    names: dict[int, str] = {}
    procs: dict[int, dict] = defaultdict(lambda: {"alloc": {}})
    for e in events:
        ph = e.get("ph")
        if ph == "M" and e.get("name") == "process_name":
            names[e["pid"]] = (e.get("args") or {}).get("name", "")
        elif ph == "v":
            dumps = (e.get("args") or {}).get("dumps") or {}
            p = procs[e["pid"]]
            totals = dumps.get("process_totals") or {}
            if "private_footprint_bytes" in totals:
                p["pmf"] = _num(totals["private_footprint_bytes"])
            if "peak_resident_set_size" in totals:
                p["peak_rss"] = _num(totals["peak_resident_set_size"])
            if "resident_set_bytes" in totals:
                p["rss"] = _num(totals["resident_set_bytes"])
            for name, a in (dumps.get("allocators") or {}).items():
                if "/" in name:
                    continue
                size = ((a.get("attrs") or {}).get("size") or {}).get("value")
                if size is not None:
                    p["alloc"][name] = _num(size)

    per_process = []
    for pid, p in procs.items():
        if "pmf" not in p:
            continue
        name = names.get(pid, "?")
        per_process.append({
            "pid": pid, "name": name, "type": _process_type(name),
            "pmf_mb": round(p["pmf"] / MB, 1),
            "peak_rss_mb": round(p.get("peak_rss", 0) / MB, 1),
            "top_alloc_mb": {k: round(v / MB, 1) for k, v in sorted(p["alloc"].items(), key=lambda kv: -kv[1])[:5]},
        })
    per_process.sort(key=lambda r: -r["pmf_mb"])
    renderers = [r for r in per_process if r["type"] == "renderer"]
    by_type: dict[str, float] = defaultdict(float)
    for r in per_process:
        by_type[r["type"]] += r["pmf_mb"]
    biggest = renderers[0] if renderers else None
    return {
        "browser_pmf_mb": round(sum(r["pmf_mb"] for r in per_process), 1),
        "browser_peak_rss_mb": round(sum(r["peak_rss_mb"] for r in per_process), 1),
        "pmf_browser_process_mb": round(by_type["browser"], 1),
        "pmf_renderers_mb": round(by_type["renderer"], 1),
        "pmf_extensions_mb": round(by_type["extension"], 1),
        "pmf_gpu_mb": round(by_type["gpu"], 1),
        "pmf_utility_mb": round(by_type["utility"], 1),
        "max_renderer_pmf_mb": biggest["pmf_mb"] if biggest else None,
        "max_renderer_pid": biggest["pid"] if biggest else None,
        "n_dumped_processes": len(per_process),
        "processes": per_process,
    }


class BrowserSignals:
    """Browser-target CDP session: latency probe, per-process CPU, memory-infra dumps, target events."""

    def __init__(self, cdp: CDPSession):
        self.cdp = cdp
        self._trace_events: list[dict] = []
        self._trace_done: asyncio.Future | None = None
        self._prev_cpu: tuple[float, dict[int, tuple[str, float]]] | None = None
        cdp.on("Tracing.dataCollected", lambda p: self._trace_events.extend(p.get("value", [])))
        cdp.on("Tracing.tracingComplete", self._on_trace_complete)

    def _on_trace_complete(self, params: dict) -> None:
        if self._trace_done is not None and not self._trace_done.done():
            self._trace_done.set_result(params)

    async def watch_targets(self, on_crashed: Callable[[dict], None], on_destroyed: Callable[[dict], None]) -> None:
        self.cdp.on("Target.targetCrashed", on_crashed)
        self.cdp.on("Target.targetDestroyed", on_destroyed)
        await self.cdp.send("Target.setDiscoverTargets", {"discover": True})

    async def probe_ms(self, timeout_s: float) -> float:
        _, ms = await timed(self.cdp.send("Browser.getVersion"), timeout_s)
        return ms

    async def process_cpu(self, timeout_s: float) -> dict:
        resp = await asyncio.wait_for(self.cdp.send("SystemInfo.getProcessInfo"), timeout_s)
        now = time.monotonic()
        procs = {p["id"]: (p["type"], float(p["cpuTime"])) for p in resp["processInfo"]}
        out: dict = {
            "n_processes": len(procs),
            "n_renderers": sum(1 for typ, _ in procs.values() if typ == "renderer"),
            "pid_types": {pid: typ for pid, (typ, _) in procs.items()},
        }
        if self._prev_cpu:
            t_prev, prev = self._prev_cpu
            elapsed = max(now - t_prev, 1e-6)
            per_pid = {pid: max(0.0, cpu - prev[pid][1]) / elapsed * 100.0
                       for pid, (_, cpu) in procs.items() if pid in prev}
            out["chrome_cpu_pct"] = round(sum(per_pid.values()), 1)
            out["per_pid_cpu_pct"] = {pid: round(v, 1) for pid, v in per_pid.items()}
            out["vanished"] = {pid: prev[pid][0] for pid in prev if pid not in procs}
        self._prev_cpu = (now, procs)
        return out

    async def memory_dump(self, timeout_s: float) -> dict:
        self._trace_events = []
        self._trace_done = asyncio.get_running_loop().create_future()
        await asyncio.wait_for(self.cdp.send("Tracing.start", TRACE_CONFIG), timeout_s)
        try:
            resp = await asyncio.wait_for(
                self.cdp.send("Tracing.requestMemoryDump", {"deterministic": False, "levelOfDetail": "background"}),
                timeout_s)
        finally:
            await asyncio.wait_for(self.cdp.send("Tracing.end"), timeout_s)
            await asyncio.wait_for(asyncio.shield(self._trace_done), timeout_s)
        if not resp.get("success"):
            raise RuntimeError("Tracing.requestMemoryDump reported success=false")
        return parse_memory_dump(self._trace_events)

    async def reset_tracing(self) -> None:
        """Best effort: end a tracing session left open by a timed-out dump."""
        try:
            self._trace_done = asyncio.get_running_loop().create_future()
            await asyncio.wait_for(self.cdp.send("Tracing.end"), 5)
            await asyncio.wait_for(self._trace_done, 10)
        except Exception:  # noqa: BLE001
            pass


# --------------------------------------------------------------------------- host (guest VM)
MEMINFO_PATH = "/sys/devices/system/node/node0/meminfo"
MEMINFO_FIELDS = {
    "MemTotal": "host_mem_total_mb", "MemFree": "host_mem_free_mb", "MemUsed": "host_mem_used_mb",
    "AnonPages": "host_anon_mb", "FilePages": "host_file_mb", "Shmem": "host_shmem_mb",
    "SwapCached": "host_swap_cached_mb", "Mapped": "host_mapped_mb", "Dirty": "host_dirty_mb",
    "Writeback": "host_writeback_mb", "KReclaimable": "host_kreclaimable_mb",
    "SReclaimable": "host_sreclaimable_mb", "PageTables": "host_pagetables_mb",
}


def clean_file_text(text: str) -> str:
    """sysfs files report a 4 KiB size, so Chrome pads the real content with garbage; cut it off."""
    for stop in ("\x00", "\ufffd"):
        idx = text.find(stop)
        if idx != -1:
            text = text[:idx]
    return text.strip()


def parse_node_meminfo(text: str) -> dict[str, int]:
    out = {}
    for m in re.finditer(r"^Node \d+ ([\w()]+):\s+(\d+)(?: kB)?$", text, re.M):
        out[m.group(1)] = int(m.group(2))
    return out


def parse_listing(text: str) -> list[str]:
    """Names from Chrome's file:// directory listing ("Index of ...")."""
    lines = text.splitlines()
    try:
        start = next(i for i, line in enumerate(lines) if line.startswith("Name\t")) + 1
    except StopIteration:
        return []
    return [line.split("\t", 1)[0].rstrip("/") for line in lines[start:] if line.strip()]


def parse_block_stat(text: str) -> dict[str, int] | None:
    parts = text.split()
    if len(parts) < 11:
        return None
    vals = [int(p) for p in parts]
    return {"read_ios": vals[0], "read_sectors": vals[2], "write_ios": vals[4],
            "write_sectors": vals[6], "in_flight": vals[8], "io_ticks_ms": vals[9]}


@dataclass
class HostProbe:
    ctx: BrowserContext
    register: Callable[[str, str, Page], Awaitable[PageSignals]]
    timeout_s: float = 15.0
    devices: list[str] = field(default_factory=list)
    sig: PageSignals | None = None
    reopens: int = 0
    _prev_blk: tuple[float, dict] | None = None

    async def setup(self) -> dict:
        """One-time swap/zram/zswap capability checks, then open the sampling tab."""
        caps: dict = {}
        scratch = await self.ctx.new_page()  # failed file:// loads can wedge a tab, so use a throwaway one
        try:
            caps["meminfo_readable"] = await self._read(scratch, MEMINFO_PATH) is not None
            listing = await self._read(scratch, "/sys/block/")
            devs = parse_listing(listing) if listing else []
            caps["block_devices"] = devs
            caps["zram_devices"] = [d for d in devs if d.startswith("zram")]
            self.devices = [d for d in devs if not d.startswith(("loop", "ram"))]
            for d in self.devices:
                size = await self._read(scratch, f"/sys/block/{d}/size")
                ro = await self._read(scratch, f"/sys/block/{d}/ro")
                caps[f"blk_{d}_size_gb"] = round(int(size) * 512 / 2**30, 2) if size and size.isdigit() else None
                caps[f"blk_{d}_readonly"] = ro == "1" if ro else None
            caps["zswap_enabled"] = await self._read(scratch, "/sys/module/zswap/parameters/enabled")
            caps["ksm_run"] = await self._read(scratch, "/sys/kernel/mm/ksm/run")
            caps["thp_enabled"] = await self._read(scratch, "/sys/kernel/mm/transparent_hugepage/enabled")
            caps["kernel_swap_support"] = await self._read(scratch, "/sys/kernel/mm/swap/") is not None
            meminfo = await self._read(scratch, MEMINFO_PATH)
            if meminfo:
                kb = parse_node_meminfo(meminfo)
                caps["mem_total_mb"] = round(kb.get("MemTotal", 0) / 1024, 1)
        finally:
            await scratch.close()
        log.info("Guest VM capabilities: %s", caps)
        if caps.get("meminfo_readable"):
            await self._open()
        return caps

    async def _open(self) -> None:
        page = await self.ctx.new_page()
        self.sig = await self.register("host", "observer", page)

    async def _read(self, page: Page, path: str) -> str | None:
        try:
            await page.goto("file://" + path, wait_until="domcontentloaded", timeout=self.timeout_s * 1000)
            return clean_file_text(await page.evaluate("document.body ? document.body.innerText : ''"))
        except Exception as exc:  # noqa: BLE001 - missing files are expected during capability checks
            log.debug("host read %s failed: %s", path, short(exc))
            try:
                await page.wait_for_load_state("load", timeout=5000)
            except Exception:  # noqa: BLE001
                pass
            return None

    async def sample(self) -> dict:
        if self.sig is None:
            return {}
        if not self.devices:
            listing = await self._read(self.sig.page, "/sys/block/")
            self.devices = [d for d in (parse_listing(listing) if listing else [])
                            if not d.startswith(("loop", "ram"))]
        if not self.sig.alive:
            log.warning("Host observer tab is gone (crashed=%s); reopening it", self.sig.crashed)
            await self._open()
        t0 = time.perf_counter()
        text = await self._read(self.sig.page, MEMINFO_PATH)
        out: dict = {"host_probe_ms": round((time.perf_counter() - t0) * 1000.0, 1)}
        if not text:
            out["host_error"] = "meminfo read failed"
            return out
        kb = parse_node_meminfo(text)
        for key, col in MEMINFO_FIELDS.items():
            if key in kb:
                out[col] = round(kb[key] / 1024, 1)
        reclaimable_slab = kb.get("KReclaimable", kb.get("SReclaimable", 0))
        # Upper-bound estimate of what can still be handed out without swap: free + unpinned page
        # cache + reclaimable slab (node meminfo has no MemAvailable line).
        out["host_mem_avail_est_mb"] = round(
            (kb.get("MemFree", 0) + kb.get("FilePages", 0) - kb.get("Shmem", 0) + reclaimable_slab) / 1024, 1)

        blk = {}
        for dev in self.devices:
            stat = await self._read(self.sig.page, f"/sys/block/{dev}/stat")
            parsed = parse_block_stat(stat) if stat else None
            if parsed:
                blk[dev] = parsed
        now = time.monotonic()
        if self._prev_blk and blk:
            t_prev, prev = self._prev_blk
            elapsed = max(now - t_prev, 1e-6)
            rd = sum(blk[d]["read_sectors"] - prev[d]["read_sectors"] for d in blk if d in prev)
            wr = sum(blk[d]["write_sectors"] - prev[d]["write_sectors"] for d in blk if d in prev)
            out["host_disk_read_mb_s"] = round(rd * 512 / MB / elapsed, 2)
            out["host_disk_write_mb_s"] = round(wr * 512 / MB / elapsed, 2)
            out["host_disk_per_device"] = {
                d: {"read_mb_s": round((blk[d]["read_sectors"] - prev[d]["read_sectors"]) * 512 / MB / elapsed, 2),
                    "write_mb_s": round((blk[d]["write_sectors"] - prev[d]["write_sectors"]) * 512 / MB / elapsed, 2)}
                for d in blk if d in prev}
        if blk:
            self._prev_blk = (now, blk)
        return out
