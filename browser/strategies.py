"""Memory-pressure drivers.

inpage: grow ONE renderer's heap in fixed steps with retained, fully written allocations.
tabs:   open one deliberately heavy tab per step to grow the browser's process set.

Both expose the same interface to the orchestrator: setup(), step(n), probe (the page whose
round-trip latency is probed), heap_targets() and counts_as_failure(label).
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable

from playwright.async_api import BrowserContext, Page

from cdp_signals import PageSignals, short

Register = Callable[[str, str, Page], Awaitable[PageSignals]]

ENV_JS = """() => ({
  jsHeapSizeLimitMb: performance.memory ? performance.memory.jsHeapSizeLimit / 1048576 : null,
  deviceMemoryGb: navigator.deviceMemory ?? null,
  hardwareConcurrency: navigator.hardwareConcurrency,
  userAgent: navigator.userAgent,
})"""

# Every page of every allocation is written so it is really committed (lazily mapped zero pages
# would not count). 'random' fill also defeats zram/zswap compression and KSM page merging, so a
# kernel that "survives" by compressing memory cannot masquerade as having headroom.
INPAGE_ALLOC_JS = r"""async ({stepMb, kind, fill, seed}) => {
  const g = globalThis.__bench || (globalThis.__bench = { keep: [], mb: 0 });
  const t0 = performance.now();
  const bytes = stepMb * 1048576;
  try {
    if (kind === 'arraybuffer') {
      // Backing store lives outside the V8 heap, so this is limited by the OS, not jsHeapSizeLimit.
      const u32 = new Uint32Array(bytes / 4);
      if (fill === 'random') {
        let x = (seed >>> 0) || 1;
        for (let i = 0; i < u32.length; i++) { x ^= x << 13; x ^= x >>> 17; x ^= x << 5; u32[i] = x; }
      } else {
        for (let i = 0; i < u32.length; i += 1024) u32[i] = 1;  // one write per 4 KiB page
      }
      g.keep.push(u32);
    } else {
      // Unboxed doubles on the V8 heap: counted against jsHeapSizeLimit and traced by every GC.
      const chunk = 1 << 20;  // 8 MiB per array
      for (let done = 0; done < bytes; done += chunk * 8) {
        const a = new Array(chunk);
        for (let i = 0; i < chunk; i++) a[i] = Math.random();
        g.keep.push(a);
      }
    }
  } catch (e) {
    return { ok: false, error: String(e), totalMb: g.mb, ms: performance.now() - t0 };
  }
  g.mb += stepMb;
  return { ok: true, totalMb: g.mb, ms: performance.now() - t0, retained: g.keep.length };
}"""

# A deliberately heavy page: a CPU-backed canvas with every pixel written, many decoded images,
# and a large table. The budget split is approximate; actual memory is measured, not assumed.
HEAVY_TAB_JS = r"""async ({canvasMb, imageCount, tableRows, seed}) => {
  const t0 = performance.now();
  let x = (seed >>> 0) || 1;
  const next = () => { x ^= x << 13; x ^= x >>> 17; x ^= x << 5; return x >>> 0; };
  const keep = (globalThis.__heavy = { images: [] });
  const out = {};
  if (canvasMb > 0) {
    const side = Math.max(256, Math.floor(Math.sqrt(canvasMb * 1048576 / 4)));
    const canvas = document.createElement('canvas');
    canvas.width = side; canvas.height = side;
    document.body.appendChild(canvas);
    const g = canvas.getContext('2d', { willReadFrequently: true });  // software backing store
    const rows = 256;
    const strip = g.createImageData(side, rows);
    const px = new Uint32Array(strip.data.buffer);
    for (let y = 0; y < side; y += rows) {
      for (let i = 0; i < px.length; i++) px[i] = next() | 0xff000000;
      g.putImageData(strip, 0, y);
    }
    keep.canvas = canvas;
    out.canvasSide = side;
  }
  if (imageCount > 0) {
    // Uncompressed 512x512 32-bit BMPs: no encode cost, ~1 MiB each encoded and again decoded.
    const w = 512, h = 512, pixelBytes = w * h * 4;
    const hdr = new DataView(new ArrayBuffer(54));
    hdr.setUint8(0, 0x42); hdr.setUint8(1, 0x4d); hdr.setUint32(2, 54 + pixelBytes, true);
    hdr.setUint32(10, 54, true); hdr.setUint32(14, 40, true); hdr.setInt32(18, w, true);
    hdr.setInt32(22, h, true); hdr.setUint16(26, 1, true); hdr.setUint16(28, 32, true);
    hdr.setUint32(34, pixelBytes, true);
    const decodes = [];
    for (let i = 0; i < imageCount; i++) {
      const pixels = new Uint32Array(w * h);
      for (let j = 0; j < pixels.length; j++) pixels[j] = next();
      const url = URL.createObjectURL(new Blob([hdr.buffer, pixels.buffer], { type: 'image/bmp' }));
      const img = new Image();
      img.src = url;
      document.body.appendChild(img);
      keep.images.push(img);
      // Revoke once decoded so the bytes are not also held in the browser process's blob store.
      decodes.push(img.decode().catch(() => null).finally(() => URL.revokeObjectURL(url)));
    }
    await Promise.all(decodes);
    out.images = imageCount;
  }
  if (tableRows > 0) {
    const table = document.createElement('table');
    const body = table.createTBody();
    for (let r = 0; r < tableRows; r++) {
      const tr = body.insertRow();
      for (let c = 0; c < 10; c++) tr.insertCell().textContent = next().toString(36) + r;
    }
    document.body.appendChild(table);
    out.tableHeightPx = table.offsetHeight;  // force layout
  }
  out.nodes = document.getElementsByTagName('*').length;
  out.ms = performance.now() - t0;
  return out;
}"""


@dataclass
class StepResult:
    ok: bool
    step_ms: float
    intended_cumulative_mb: int
    detail: dict = field(default_factory=dict)
    error: str | None = None
    timed_out: bool = False


class InPageStrategy:
    name = "inpage"

    def __init__(self, ctx: BrowserContext, register: Register, step_mb: int, alloc_kind: str,
                 fill: str, step_timeout_s: float):
        self.ctx, self.register = ctx, register
        self.step_mb, self.alloc_kind, self.fill = step_mb, alloc_kind, fill
        self.step_timeout_s = step_timeout_s
        self.allocated_mb = 0
        self.probe: PageSignals | None = None

    async def setup(self) -> dict:
        page = await self.ctx.new_page()
        await page.set_content("<title>bench-alloc</title><body>allocation target</body>")
        self.probe = await self.register("alloc", "strategy", page)
        return await page.evaluate(ENV_JS)

    async def step(self, n: int) -> StepResult:
        intended = self.allocated_mb + self.step_mb
        t0 = time.perf_counter()
        args = {"stepMb": self.step_mb, "kind": self.alloc_kind, "fill": self.fill, "seed": 0x9E3779B9 ^ n}
        try:
            r = await asyncio.wait_for(self.probe.page.evaluate(INPAGE_ALLOC_JS, args), self.step_timeout_s)
        except asyncio.TimeoutError:
            return StepResult(False, _ms(t0), intended, error=f"step exceeded {self.step_timeout_s}s", timed_out=True)
        except Exception as exc:  # noqa: BLE001 - a crash mid-step surfaces here as "Target crashed"
            return StepResult(False, _ms(t0), intended, error=short(exc))
        if not r.get("ok"):
            return StepResult(False, _ms(t0), intended, detail=r, error=r.get("error"))
        self.allocated_mb = intended
        return StepResult(True, _ms(t0), intended, detail={"js_fill_ms": round(r["ms"], 1)})

    def heap_targets(self) -> list[PageSignals]:
        return [self.probe] if self.probe and self.probe.alive else []

    def counts_as_failure(self, label: str) -> bool:
        return label == "alloc"


class TabsStrategy:
    name = "tabs"

    def __init__(self, ctx: BrowserContext, register: Register, step_mb: int, step_timeout_s: float):
        self.ctx, self.register = ctx, register
        self.step_mb = step_mb
        self.step_timeout_s = step_timeout_s
        self.tabs: list[PageSignals] = []
        self.opened = 0
        self.probe: PageSignals | None = None

    def budget(self) -> dict:
        """Split one tab's budget: ~55% canvas, ~35% decoded images (1 MiB each), plus a fixed large
        table. The table is capped rather than scaled: 10 cells/row of DOM is far more CPU than memory,
        and on 1 vCPU a budget-sized table would dominate the step time without adding much footprint."""
        return {"canvasMb": int(self.step_mb * 0.55), "imageCount": max(1, int(self.step_mb * 0.35)),
                "tableRows": 3000}

    async def setup(self) -> dict:
        page = await self.ctx.new_page()
        await page.set_content("<title>bench-probe</title><body>latency probe</body>")
        self.probe = await self.register("probe", "strategy", page)
        return await page.evaluate(ENV_JS) | {"tab_budget": self.budget()}

    async def step(self, n: int) -> StepResult:
        intended = (self.opened + 1) * self.step_mb
        t0 = time.perf_counter()
        try:
            page = await asyncio.wait_for(self.ctx.new_page(), self.step_timeout_s)
            sig = await self.register(f"tab{n}", "strategy", page)
            await page.set_content(f"<title>bench-heavy-{n}</title><body></body>")
            r = await asyncio.wait_for(page.evaluate(HEAVY_TAB_JS, self.budget() | {"seed": 0x85EBCA6B ^ n}),
                                       self.step_timeout_s)
        except asyncio.TimeoutError:
            return StepResult(False, _ms(t0), intended, error=f"tab build exceeded {self.step_timeout_s}s", timed_out=True)
        except Exception as exc:  # noqa: BLE001
            return StepResult(False, _ms(t0), intended, error=short(exc))
        self.tabs.append(sig)
        self.opened += 1
        return StepResult(True, _ms(t0), intended, detail={"tab": n, "open_tabs": self.live_tabs(),
                                                           "build_ms": round(r.get("ms", 0), 1),
                                                           "dom_nodes": r.get("nodes")})

    def live_tabs(self) -> int:
        return sum(1 for t in self.tabs if t.alive)

    def heap_targets(self) -> list[PageSignals]:
        return [s for s in [self.probe, *self.tabs] if s and s.alive]

    def counts_as_failure(self, label: str) -> bool:
        return label == "probe" or label.startswith("tab")


def _ms(t0: float) -> float:
    return round((time.perf_counter() - t0) * 1000.0, 1)
