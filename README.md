# agentcore-bench

Reproducible benchmarks for [Amazon Bedrock AgentCore](https://aws.amazon.com/bedrock/agentcore/).
Each one measures, in your own AWS account, behaviour the AgentCore documentation does not spell out.
It records the evidence (API calls, in-guest signals, CloudWatch, CloudTrail, per-second usage logs)
and writes the answers to a `FINDINGS.md`.

| Benchmark | Question | Results |
|---|---|---|
| [`browser/`](browser/) | What happens when an AgentCore Browser session approaches its fixed 1 vCPU / 4 GB ceiling? | [`browser/FINDINGS.md`](browser/FINDINGS.md) |
| [`runtime-v2/`](runtime-v2/) | How does AgentCore Runtime V2 (`platformVersion=V2`, snapshot-based starts) really behave: VPC mode, pre-started instances, rollback, same-tag updates, endpoints, pricing? | [`runtime-v2/FINDINGS.md`](runtime-v2/FINDINGS.md) |

## AgentCore Browser: the 4 GB memory ceiling

[`browser/`](browser/) drives one browser session's memory upward in controlled steps, in one page's
heap or by opening heavy tabs. It samples Chrome (CDP), the guest VM's own memory counters, round-trip
latency and AgentCore's usage telemetry throughout, then works out how the session failed.

Headline results (`aws.browser.v1`, us-west-2, 14 sessions, October 2026):

- **Nothing is paged to disk.** There is no usable swap; Linux evicts its page cache, then kills something.
- **There is no useful early warning.** Median latency stayed within 4% of baseline until a final stall of 9–48 s.
- **There are two failure modes, and the session survives both.** Either the tab using the memory is
  OOM-killed and everything else keeps running, or the VM freezes for ~47 s and Chrome restarts,
  losing every tab.
- **Plan for ~2.5 GB of page memory, not 4 GB.** JavaScript-heavy pages hit Chrome's heap limit at ~1.9 GB per tab.
- **Metered memory was 3.5–4.0 GB per session** (median) whatever the pages used, idle sessions included.

```bash
cd browser
python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
python benchmark.py --discover-only        # read-only: which CloudWatch signals exist
python benchmark.py --strategy inpage      # one session, ~3 minutes, well under a cent
```

## AgentCore Runtime V2: six questions, measured

[`runtime-v2/`](runtime-v2/) deploys the same probe agent on V1 and V2 runtimes, in PUBLIC and
private-VPC network modes, and runs one experiment per question. The agent detects a snapshot restore
from inside the session: the wall-clock time jumps while monotonic time stands still, and a token drawn
at startup is frozen into the snapshot. Images are built and pushed without Docker.

Headline results (us-west-2, October 2026):

- **VPC mode is supported, and cold starts are not slower** (p50 1.79 s VPC vs 2.07 s PUBLIC). Creating
  the runtime is much slower: 9.4 min vs 3.3 min.
- **V2 keeps no pool.** Every new session restores the version's snapshot on demand, at most 0.09 s
  before its first request. V1 keeps a warm pool of paused, pre-booted microVMs, which made it faster at
  low load (p50 ~0.6 s vs ~2 s).
- **The platform version can be switched both ways** on an existing runtime: V2→V1 in 6 s, V1→V2 in ~3 min.
- **An update that names the same image tag re-pulls it and rebuilds the snapshot.** So does an update
  that changes nothing.
- **Snapshots are per endpoint and version, not per version.** Every endpoint change is a ~3-minute
  build. With a mutable tag, one version can run different images on different endpoints.
- **Snapshot creation and storage were not metered.** The restore is billed as the start of each
  session, and V2 has its own, higher unit rates.

```bash
cd runtime-v2
python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
python poc.py run                 # all six experiments, ~1 hour, under US$1; tears down at the end
python poc.py run --only q2,q4    # a subset
```

## Before you run them

- **Python 3.11+ and AWS credentials** for an account and Region where AgentCore is available (V2:
  us-east-1, us-east-2, us-west-2, eu-west-1, ap-northeast-1). Each benchmark's README lists the IAM
  permissions it needs.
- **Use a sandbox account.** The browser benchmark starts and stops sessions, and `setup_usage_logs.py`
  can optionally add a log delivery. The Runtime PoC creates runtimes, an ECR repository, IAM roles, a
  private VPC with endpoints, and log deliveries. It tags them, records each one in
  `runs/<run>/state.json`, and deletes them at the end; `poc.py teardown` removes anything left over.
- **Nothing is exposed.** The Runtime PoC's runtimes use IAM (SigV4) inbound auth, and the PoC checks
  that an unsigned invoke is refused. The VPC has no internet route; its only inbound rule lets the runtime
  reach the VPC endpoints over HTTPS.

## Repository layout

```
browser/            AgentCore Browser memory-ceiling benchmark (README, FINDINGS, runs/)
runtime-v2/         AgentCore Runtime V2 PoC (README, FINDINGS, runs/, agent/ probe agent)
```

Each `runs/<run-id>/` directory holds one run's raw evidence and its own `FINDINGS.md`. In the
published data, AWS account IDs are replaced with `123456789012` and network resource IDs with
placeholders.

## Disclaimer

This is not an official AWS project. The results are point-in-time measurements from one Region
(October 2026) and may change as the services evolve. Pricing observations reflect what was metered
in a test account; they are not pricing commitments. See the
[AgentCore pricing page](https://aws.amazon.com/bedrock/agentcore/pricing/) for authoritative rates.

Licensed under the [MIT License](LICENSE).
