# AgentCore Runtime V2: six questions, measured

Run `pricing1` · Region `us-west-2` · 2026-10-27 20:15 UTC · 5 new sessions per runtime for the cold-start comparison. Every number below comes from this run's API calls, invocations, the agent's own logs and CloudTrail (raw data in this directory).

## Answers

| # | Question | Measured answer |
|---|---|---|
| 1 | VPC mode: is V2 supported, and does it change restore/start time vs PUBLIC? | Not tested in this run. |
| 2 | Pre-started instances: does V2 keep a pool, or does every new session restore on demand? | **Every new session restores on demand; no pre-started or pre-restored V2 instances were seen.** 5/5 new V2 sessions restored the version's snapshot (sessions shared 2 snapshot copies per runtime), each restored at most 0.08 s before its first request arrived (5/5 ≤2 s), and before the first session nothing was booted or restored outside the snapshot build. V1, for comparison, does keep a warm pool: 5/5 V1 sessions landed on their own instance that had been booted ahead of time (median 4.7 min before the session, newest 4.5 min), ran ~3.3 s, was frozen, and was resumed ~0.13 s before the first request; 10 V1 instances had booted before the first session was even sent. At this low, sequential load that pool made V1's cold invoke (p50 511 ms) faster than V2's (p50 2.03 s). |
| 3 | Rollback: can a runtime created on V2 be switched back to V1? | Not tested in this run. |
| 4 | Same-tag updates: does an update to the same tag re-pull the image and rebuild the snapshot? | Not tested in this run. |
| 5 | Endpoints: is the snapshot per version, and does each endpoint restore its own version? | Not tested in this run. |
| 6 | Pricing: is snapshot creation, storage or restore charged? | All 32 per-second usage records AgentCore delivered (V1 · PUBLIC) belong to sessions this PoC opened (0 for any other session, 0 with no session); no V2 snapshot build is covered by the records delivered so far. On V1 the warm-pool instance's boot is metered and billed to the session that later claims it: each V1 session carried 6 s of records timestamped 4.7 min before its first call (~1.03 GB, 1.2+ vCPU while booting), and nothing for the time it sat paused (records for the sessions' own active seconds had not been delivered when the run collected them). CloudWatch MemoryUsed/CPUUsed showed 0 non-zero minutes inside those builds. Cost Explorer not checked yet (run `poc.py pricing` a day later). The pricing page has no snapshot line item, and V2 bills at its own rates ($0.1276/vCPU-h, $0.0169/GB-h vs V1 $0.0895, $0.00945). This is evidence of what was metered, not a pricing commitment. |

## 1. VPC mode

| Runtime | Create → READY | Sessions OK | Cold invoke p50 / p75 / p90 | Warm p50 | Start-up overhead p50 | First read of a 32 MiB image file: min / p50 / max | Second read p50 |
|---|---|---|---|---|---|---|---|
| V1 · PUBLIC | 18 s | 5/5 | 511 ms / 546 ms / 595 ms | 130 ms | 355 ms | 3 ms / 3 ms / 3 ms | 3 ms |
| V2 · PUBLIC | 3.3 min | 5/5 | 2.03 s / 2.53 s / 2.78 s | 155 ms | 1.87 s | 2.35 s / 4.36 s / 5.06 s | 5 ms |

*Cold invoke*: InvokeAgentRuntime with a new session id, timed by the client (includes the round trip to the Region). *Start-up overhead*: cold minus the next call on the same session. *File read*: a full read of a file baked into the image, on the session's third and fourth calls.

- Network seen from inside **V1 · PUBLIC**: internet reachable (DNS: public IPs); s3 reachable (DNS: public IPs); ecr_api reachable (DNS: public IPs); logs reachable (DNS: public IPs); sts reachable (DNS: public IPs)
- Network seen from inside **V2 · PUBLIC**: internet reachable (DNS: public IPs); s3 reachable (DNS: public IPs); ecr_api reachable (DNS: public IPs); logs reachable (DNS: public IPs); sts reachable (DNS: public IPs)

## 2. Pre-started instances

| Runtime | Sessions | How each session's instance started | Distinct startup tokens | Instance age at 1st request p50 (min) | Ran before freeze p50 | Resumed/restored before 1st request p50 / max | Before 1st session: boots / restores | Snapshot build: boots / test restores |
|---|---|---|---|---|---|---|---|---|
| V1 · PUBLIC | 5 | 5 pre-started instance | 5 | 4.7 min (4.5 min) | 3.29 s | 0.13 s / 0.17 s | 10 / 0 | 0 / 0 |
| V2 · PUBLIC | 5 | 5 snapshot restore | 2 | 2.7 min (2.3 min) | 69.23 s | 0.06 s / 0.08 s | 0 / 0 | 2 / 2 |

*Instance age*: wall-clock time from the agent process starting to the session's first request. *Ran before freeze*: how long that process ran before it was frozen (V1: paused once healthy; V2: snapshotted). *Resumed/restored before 1st request*: how long the instance had been running again when the request arrived. *Before 1st session*: processes booted or restored outside a snapshot build before the runtime's first invocation (after READY there were 30 s with no traffic at all).

How a V2 create unfolded (seconds after CreateAgentRuntime):

| Runtime | Image pulls | Agent booted | First ping after boot | Snapshot taken | Test restore | READY | First session |
|---|---|---|---|---|---|---|---|
| V2 · PUBLIC | 10, 89, 89, 89, 89 | 92, 93 | 1.7, 1.6 | 162, 171 | 172, 182 | 197 | 228 |

## 3. Rollback

## 4. Same-tag updates

## 5. Endpoints

## 6. Pricing

| Runtime | Step | Build minutes | Snapshot boots | Sessions nearby | Usage delivered for it |
|---|---|---|---|---|---|
| V2 · PUBLIC | create | 3.3 | 2 | False | False |

| Runtime | Usage records | Sessions | Records for sessions not opened by the PoC | Records with no session | Records inside quiet builds | vCPU-h | GB-h | ≈ USD |
|---|---|---|---|---|---|---|---|---|
| V1 · PUBLIC | 32 | 5 | 0 | 0 | 0 | 0.006272 | 0.008098 | 0.000638 |

| Platform | Sessions | Metered s p50 | …before the 1st call (lead) | …from the 1st call | Metered after last reply | 1st metered second GB / vCPU | Then GB p50 | GB-h p50 | vCPU-h p50 | ≈ USD p50 |
|---|---|---|---|---|---|---|---|---|---|---|
| V1 | 5 | 6 | 6 s (4.7 min earlier) | 0 s | – | 0.66 / 1.18 | 1.03 | 0.001619 | 0.001232 | 0.000126 |

## Also measured

- **V2 environment variables, 4500 bytes** (docs: 2.5 KB cap for container agents on V2, 4 KB on V1): rejected by CreateAgentRuntime: Environment variables exceed the maximum size of 4000 bytes. Current size: 4500 bytes.
- **V2 environment variables, 3000 bytes** (docs: 2.5 KB cap for container agents on V2, 4 KB on V1): accepted, and the runtime then ended CREATE_FAILED (The environment variable payload is 3004 bytes, exceeding the 2560-byte maximum supported for V2 agents. Reduce the total size of the configured environment variables and retry.)
- **Startup state is shared**: across 5 V2 sessions, a uuid4() drawn at import took only 2 distinct values on V2 · PUBLIC (one per snapshot copy); every session reported hostname localhost and PID 1
- **Startup code runs more than once per V2 version**: processes booted per create build: {'v2_public': 2} (plus a test restore of each)
- **Inbound auth**: an unsigned InvokeAgentRuntime was refused with HTTP 403 AccessDeniedException
- **Create/update → READY**: V1 · PUBLIC create (V1) 18 s; V2 · PUBLIC create (V2) 3.3 min

## Method

- One probe image: python:3.12-slim + a standard-library HTTP agent + a fixed 32 MiB file, assembled and pushed by `image.py` (no Docker). Builds A and B differ only in a build id.
- The agent recognizes a V2 restore from inside the process. Monotonic time does not advance across a restore, so the gap between wall-clock and monotonic time jumps, and the agent records when. It also draws a random token at import, which a snapshot freezes: sessions restored from the same snapshot report the same token.
- Each runtime has its own execution role, so CloudTrail's ECR pull events name the runtime and the step that pulled (`snapstart-build-<endpoint>` for snapshot builds).
- Runtimes use IAM (SigV4) inbound auth. Sessions are stopped after use; idle timeout is 10 min.
