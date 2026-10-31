# AgentCore Browser memory-ceiling benchmark

Amazon Bedrock AgentCore Browser gives every session a fixed, non-adjustable **1 vCPU / 4 GB**
microVM. AWS does not document what happens when a session approaches that ceiling, so this
benchmark measures it: it drives one session's memory upward in controlled steps, samples several
independent signals throughout, and attributes the failure when the ceiling is hit.

It answers three questions explicitly, in `FINDINGS.md`:

1. Does it page to disk / swap?
2. Does it slow down first (GC thrash), and gradually or as a cliff?
3. What is the failure mode, and at what memory reading?

Plus the cost consequence of running near the ceiling, measured from AWS's own per-second usage logs.

**Measured results for `aws.browser.v1` in `us-west-2` are in [`FINDINGS.md`](FINDINGS.md)**, with
per-run reports, CSVs and charts under `runs/`.

## Prerequisites

Python 3.11+ (the runner itself does not need a local browser — Playwright drives the remote one).

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
```

### IAM

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "BrowserSessions",
      "Effect": "Allow",
      "Action": [
        "bedrock-agentcore:StartBrowserSession",
        "bedrock-agentcore:StopBrowserSession",
        "bedrock-agentcore:GetBrowserSession",
        "bedrock-agentcore:ListBrowserSessions",
        "bedrock-agentcore:ConnectBrowserAutomationStream"
      ],
      "Resource": "arn:aws:bedrock-agentcore:<region>:<account>:browser/*"
    },
    {
      "Sid": "Observation",
      "Effect": "Allow",
      "Action": [
        "cloudwatch:ListMetrics",
        "cloudwatch:GetMetricData",
        "logs:FilterLogEvents",
        "logs:GetLogEvents",
        "logs:DescribeDeliverySources",
        "logs:DescribeDeliveryDestinations",
        "logs:DescribeDeliveries"
      ],
      "Resource": "*"
    }
  ]
}
```

The CDP WebSocket is reached by SigV4-signing a `GET` on the automation-stream endpoint
(`bedrock-agentcore` service), so the runner needs no separate credential path.

`setup_usage_logs.py` additionally needs `logs:CreateLogGroup`, `logs:PutRetentionPolicy`,
`logs:PutDeliverySource`, `logs:PutDeliveryDestination`, `logs:CreateDelivery` (and the matching
`Delete*` for `--teardown`).

## Running

```bash
# What CloudWatch actually exposes for this browser tool — read-only, starts no session:
python benchmark.py --discover-only

# Per-second, per-session usage logs (optional but recommended; see "Signals" below):
python setup_usage_logs.py

# The benchmark. Defaults ramp +100 MB every 10 s until the ceiling or --max-mb:
python benchmark.py --strategy inpage
python benchmark.py --strategy tabs --step-mb 250 --interval-s 10

# Re-pull the delayed AWS metrics for a finished run and regenerate its reports:
python benchmark.py --reanalyze runs/<run-id>
```

Region and browser default to `us-west-2` / `aws.browser.v1` and can be set with `--region` /
`--browser-id` or `AGENTCORE_REGION` / `AGENTCORE_BROWSER_ID`.

The session is always stopped: a `finally` block, a SIGINT/SIGTERM handler (first signal winds down
cleanly, a second aborts) and an `atexit` hook all call an idempotent `stop_browser_session`.

### The two pressure strategies

| | `inpage` | `tabs` |
|---|---|---|
| What grows | one renderer's heap: retained `ArrayBuffer`s, every page written | the process set: one deliberately heavy tab per step (full-pixel canvas, decoded images, large table) |
| Tests | the renderer/tab heap path | the whole-browser path |
| Failure | either the tab is `SIGKILL`ed, or the VM stalls ~47 s, the connection drops and Chrome restarts (depends on `--fill`) | Chrome loses its biggest tab; the connection and other tabs survive |

`--alloc-kind` picks what `inpage` allocates. `arraybuffer` (default) puts bytes in backing stores
*outside* the V8 heap, so the VM ceiling is the binding constraint. `jsheap` allocates V8 objects
instead, which hits V8's own heap limit first (the tab crashed at ~1.9 GB, `jsHeapSizeLimit` is ~2.2 GB)
and maximises GC work — use it to study the JavaScript-heap path rather than the VM ceiling. `--fill random` (default)
writes incompressible bytes so that zram/zswap compression or KSM page-merging cannot fake headroom;
`--fill touch` writes one word per 4 KiB page instead.

## How to interpret the output

Each run writes `runs/<timestamp>-<strategy>/`:

| file | contents |
|---|---|
| `FINDINGS.md` | the three answers, with the evidence and the billing implication |
| `findings.json` | the same, machine-readable, including every classifier input |
| `timeseries.csv` | the time series — the table view of the chart |
| `chart.png` | memory vs time, probe latency overlaid, failure annotated |
| `samples.jsonl` | every raw sample and event, flushed per line |
| `cloudwatch.json`, `discovery.json` | what was discovered and pulled, verbatim |
| `usage_logs.jsonl` | AWS's per-second session usage records |
| `benchmark.log` | full debug log |

A top-level `FINDINGS.md` aggregates every run plus the latest run per strategy.

### Reading the discriminators

- **Paging vs kill.** Paging would keep memory pinned near the ceiling while allocations kept
  succeeding and latency climbed. A kill releases it. The benchmark reads the guest VM's own
  `meminfo` and per-device `stat` (through a `file://` tab — `/proc` reads back empty in Chrome, but
  `/sys/devices/system/node/node0/meminfo` works), so `SwapCached`, the free-memory floor, page-cache
  eviction and disk write rates are all directly observable. After a failure the **recovery
  reconnect re-reads `meminfo`**, which is the only way to see the release when the CDP connection
  died with the renderer.
- **Slowdown shape.** `gradual` means the *median* probe latency or step time degraded across the
  ramp (GC thrash / reclaim pressure). `cliff` means the median stayed flat and only the final step
  blew up. The report distinguishes them rather than collapsing both into "it slowed down".
- **Attribution.** `Target.targetCrashed` carries a raw `waitpid` status, decoded to a signal name
  (`SIGKILL` = OOM kill, `SIGTRAP` = V8's own out-of-memory abort). When the automation stream drops,
  no exit code arrives and every Playwright page reports "closed", so after reconnecting the benchmark
  asks the browser what survived: Chrome's browser-process PID (changed = Chrome was restarted and all
  tabs lost), the remaining page targets, and whether the page under pressure still answers. An
  explicit crash status from Chrome is never overridden by inference.
- **Probe latency floor.** The `Browser.getVersion` probe's baseline is essentially the runner's
  network RTT to the region (~40 ms from a laptop), not browser latency. Read it as a relative
  signal.

## Signals, and their caveats

| signal | resolution | scope | caveat |
|---|---|---|---|
| `AWS/Bedrock-AgentCore` `MemoryUsed-GBHours`, `CPUUsed-vCPUHours` | 1 min | **per browser tool** | aggregates every concurrent session on that tool; AWS documents a delay of up to 60 min (~18 min observed here), so a finished run usually needs `--reanalyze` later |
| `AWS/Bedrock-AgentCore` `ActiveSessionCount` | 1 min | per service | used to check whether the tool-level numbers above were contaminated by other sessions |
| `USAGE_LOGS` vended logs | **1 s** | **per session** | opt-in (`setup_usage_logs.py`); the only genuinely per-session usage signal; also delivered late |
| CDP `Runtime.getHeapUsage`, `Performance.getMetrics` | per sample | per page | V8 heap + ArrayBuffer backing store, not the process footprint |
| CDP memory-infra trace dumps | per sample | per process | private footprint and peak RSS per Chrome process; the dump itself can time out under heavy pressure |
| CDP `SystemInfo.getProcessInfo` | per sample | per process | cumulative CPU time, differenced into a rate |
| guest `sysfs` via `file://` | per sample | whole VM | the only view of swap, page cache and disk I/O |

Metric names are **discovered at runtime**, never hardcoded: the runner lists every CloudWatch
namespace, keeps the AgentCore ones, classifies metrics as memory/CPU/session-count by name, scores
them by how specifically their dimensions match this session, and logs what it chose. If nothing is
found it says so and falls back to the CDP and guest-VM signals. `--namespace` can name a namespace
explicitly if listing has not caught up.

Because the tool-level metrics are shared, the runner records the count of other `READY` sessions at
start and warns when the numbers could be contaminated.

## Cost of a run

List price is $0.00945/GB-hour of memory and $0.0895/vCPU-hour. AgentCore's own per-second usage
telemetry metered memory at 3.2–4.0 GB per second (median 3.5–4.0 GB per session) in every session
measured, including idle sessions whose browser used ~0.9 GB, so memory runs about
**$0.033–$0.038/session-hour** regardless of what the pages do, while CPU does scale with work. (That
is telemetry; AWS notes billing is based on metered usage and can differ.) A default
ramp reaches the ceiling in ~3 minutes, so a run costs well under a cent. The conservative defaults
exist to keep it that way; the dominant cost lever is session count × lifetime, not per-session
frugality. See `FINDINGS.md` for the measured numbers.

## Teardown

```bash
python setup_usage_logs.py --teardown                      # remove the log delivery wiring
python setup_usage_logs.py --teardown --delete-log-group   # and the log group
```

Browser sessions stop themselves; `--discover-only` and `--reanalyze` never start one. To confirm
nothing is left running:

```bash
aws bedrock-agentcore list-browser-sessions \
  --browser-identifier aws.browser.v1 --status READY --region us-west-2
```
