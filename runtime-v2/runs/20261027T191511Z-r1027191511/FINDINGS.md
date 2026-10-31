# AgentCore Runtime V2: six questions, measured

Run `r1027191511` · Region `us-west-2` · 2026-10-27 19:15 UTC · 10 new sessions per runtime for the cold-start comparison. Every number below comes from this run's API calls, invocations, the agent's own logs and CloudTrail (raw data in this directory).

## Answers

| # | Question | Measured answer |
|---|---|---|
| 1 | VPC mode: is V2 supported, and does it change restore/start time vs PUBLIC? | **Yes.** V2 in VPC mode (private subnets, no internet; checked from inside the session: internet unreachable, ECR via its interface endpoint, S3 via the gateway endpoint) reached READY in 9.4 min and served 10/10 new sessions; cold-start invoke p50 1.79 s vs 2.07 s in PUBLIC (Δ p50 -280 ms, Δ p90 -261 ms); V2 PUBLIC reached READY in 3.3 min; for comparison V1 VPC vs PUBLIC: 629 ms vs 552 ms (Δ p50 +78 ms). |
| 2 | Pre-started instances: does V2 keep a pool, or does every new session restore on demand? | **Every new session restores on demand; no pre-started or pre-restored V2 instances were seen.** 20/20 new V2 sessions restored the version's snapshot (sessions shared 2 snapshot copies per runtime), each restored at most 0.09 s before its first request arrived (20/20 ≤2 s), and before the first session nothing was booted or restored outside the snapshot build. V1, for comparison, does keep a warm pool: 20/20 V1 sessions landed on their own instance that had been booted ahead of time (median 10.5 min before the session, newest 13 s), ran ~3.3 s, was frozen, and was resumed ~0.14 s before the first request; 10 and 10 V1 instances had booted before the first session was even sent. At this low, sequential load that pool made V1's cold invoke (p50 590 ms) faster than V2's (p50 1.93 s). |
| 3 | Rollback: can a runtime created on V2 be switched back to V1? | **Not fixed at create.** UpdateAgentRuntime(platformVersion=V1) on the V2 runtime created version 4 and reached READY in 6 s; GetAgentRuntime then reported V1; new sessions behaved as V1 (each on its own instance: 2 distinct startup tokens in 2 sessions); switching back to V2 worked too (READY in 3.1 min; 2/2 sessions restored a snapshot); a session opened before the switch kept its instance and answered through both switches; an endpoint pinned to the older V2 version kept serving snapshot restores (2/2). |
| 4 | Same-tag updates: does an update to the same tag re-pull the image and rebuild the snapshot? | **Yes: an update naming the same tag re-pulls it and rebuilds the snapshot.** V1 · PUBLIC: tag moved, no update: new sessions ran build A-r1027191511; the same-tag update (v2, READY in 6 s) → new sessions ran build B-r1027191511; an update with nothing changed still created v3 (READY in 6 s). V1 · VPC: tag moved, no update: new sessions ran build A-r1027191511; the same-tag update (v2, READY in 16 s) → new sessions ran build B-r1027191511; an update with nothing changed still created v3 (READY in 16 s). V2 · PUBLIC: with the tag moved but no update, new sessions still restored build A-r1027191511; the same-tag update (v2, READY in 3.1 min) resolved the tag again (ECR pulls: tag ci ×3, digest B-r1027191511 ×2); new sessions ran build B-r1027191511 from a newly built snapshot; an update with nothing changed still created v3 (READY in 3.1 min) and rebuilt the snapshot. V2 · VPC: with the tag moved but no update, new sessions still restored build A-r1027191511; the same-tag update (v2, READY in 3.4 min) resolved the tag again (ECR pulls: tag ci ×3, digest B-r1027191511 ×2); new sessions ran build B-r1027191511 from a newly built snapshot; an update with nothing changed still created v3 (READY in 3.4 min) and rebuilt the snapshot. (Build A `A-r1027191511`, build B `B-r1027191511`.) |
| 5 | Endpoints: is the snapshot per version, and does each endpoint restore its own version? | **The snapshot is per endpoint and version, not just per version.** Each endpoint restores the version it points to, but an endpoint gets its own snapshot build (minutes) even when another endpoint already has that version: with DEFAULT, LIVE and test all on v1, DEFAULT restored snapshot d8119717 (built by create); LIVE restored snapshot 0095c5db (built by endpoint_create:LIVE); test restored snapshot 40c27ba9 (built by endpoint_create:test); after the runtime moved to v2, every endpoint restored the version it pointed at: DEFAULT → marker m2; LIVE → marker m1; test → marker m2; 5 of 5 endpoint creates/updates built a snapshot of their own (3.1 min, 3.1 min, 3.1 min, 3.1 min, 3.1 min); re-pointing LIVE at v2 once nothing referenced it restored a snapshot built by endpoint_update:LIVE; **the same runtime version ran different images on different endpoints**: test on the version with marker m1 ran B-r1027191511 (first seen with A-r1027191511). The version stores the image tag, and every snapshot build (a new endpoint, an endpoint update) resolves that tag again, so with a mutable tag re-pointing an endpoint at an old version does not bring back the old image. |
| 6 | Pricing: is snapshot creation, storage or restore charged? | **Snapshot creation and storage: nothing was metered. Restore: metered, as part of the session.** All 179 per-second usage records AgentCore delivered (V2 · PUBLIC (endpoints/rollback)) belong to sessions this PoC opened (0 for any other session, 0 with no session), although 5 V2 snapshot builds (15.7 min of building) ran on those runtimes within the delivered period. Each new V2 session's first metered second (the restore) showed 1.49 GB and 2.00 vCPU, then ~1.33 GB while idle (reclaim starts after 120 s idle); a one-call session metered 14 s, 13 s of it after the last reply (StopRuntimeSession was called immediately), ≈ $0.000198 per session. CloudWatch MemoryUsed/CPUUsed showed 0 non-zero minutes inside those builds. Cost Explorer not checked yet (run `poc.py pricing` a day later). The pricing page has no snapshot line item, and V2 bills at its own rates ($0.1276/vCPU-h, $0.0169/GB-h vs V1 $0.0895, $0.00945). This is evidence of what was metered, not a pricing commitment. |

## 1. VPC mode

| Runtime | Create → READY | Sessions OK | Cold invoke p50 / p75 / p90 | Warm p50 | Start-up overhead p50 | First read of a 32 MiB image file: min / p50 / max | Second read p50 |
|---|---|---|---|---|---|---|---|
| V1 · PUBLIC | 18 s | 10/10 | 552 ms / 640 ms / 679 ms | 137 ms | 430 ms | 3 ms / 3 ms / 5 ms | 3 ms |
| V1 · VPC | 4.5 min | 10/10 | 629 ms / 684 ms / 800 ms | 143 ms | 465 ms | 3 ms / 3 ms / 5 ms | 3 ms |
| V2 · PUBLIC | 3.3 min | 10/10 | 2.07 s / 2.22 s / 2.42 s | 159 ms | 1.92 s | 1.99 s / 3.13 s / 8.66 s | 4 ms |
| V2 · VPC | 9.4 min | 10/10 | 1.79 s / 1.86 s / 2.16 s | 157 ms | 1.62 s | 37 ms / 40 ms / 7.98 s | 4 ms |

*Cold invoke*: InvokeAgentRuntime with a new session id, timed by the client (includes the round trip to the Region). *Start-up overhead*: cold minus the next call on the same session. *File read*: a full read of a file baked into the image, on the session's third and fourth calls.

- Network seen from inside **V1 · PUBLIC**: internet reachable (DNS: public IPs); s3 reachable (DNS: public IPs); ecr_api reachable (DNS: public IPs); logs reachable (DNS: public IPs); sts reachable (DNS: public IPs)
- Network seen from inside **V1 · VPC**: internet unreachable (DNS: public IPs); s3 reachable (DNS: public IPs); ecr_api reachable (DNS: private IPs); logs reachable (DNS: private IPs); sts unreachable (DNS: public IPs)
- Network seen from inside **V2 · PUBLIC**: internet reachable (DNS: public IPs); s3 reachable (DNS: public IPs); ecr_api reachable (DNS: public IPs); logs reachable (DNS: public IPs); sts reachable (DNS: public IPs)
- Network seen from inside **V2 · VPC**: internet unreachable (DNS: public IPs); s3 reachable (DNS: public IPs); ecr_api reachable (DNS: private IPs); logs reachable (DNS: private IPs); sts unreachable (DNS: public IPs)

## 2. Pre-started instances

| Runtime | Sessions | How each session's instance started | Distinct startup tokens | Instance age at 1st request p50 (min) | Ran before freeze p50 | Resumed/restored before 1st request p50 / max | Before 1st session: boots / restores | Snapshot build: boots / test restores |
|---|---|---|---|---|---|---|---|---|
| V1 · PUBLIC | 10 | 10 pre-started instance | 10 | 12.4 min (13 s) | 3.34 s | 0.13 s / 0.17 s | 10 / 0 | 0 / 0 |
| V1 · VPC | 10 | 10 pre-started instance | 10 | 8.6 min (23 s) | 3.25 s | 0.14 s / 0.18 s | 10 / 0 | 0 / 0 |
| V2 · PUBLIC | 10 | 10 snapshot restore | 2 | 11.9 min (11.0 min) | 69.87 s | 0.02 s / 0.07 s | 0 / 0 | 2 / 2 |
| V2 · VPC | 10 | 10 snapshot restore | 2 | 5.9 min (5.0 min) | 68.84 s | 0.04 s / 0.09 s | 0 / 0 | 2 / 2 |

*Instance age*: wall-clock time from the agent process starting to the session's first request. *Ran before freeze*: how long that process ran before it was frozen (V1: paused once healthy; V2: snapshotted). *Resumed/restored before 1st request*: how long the instance had been running again when the request arrived. *Before 1st session*: processes booted or restored outside a snapshot build before the runtime's first invocation (after READY there were 180 s with no traffic at all).

How a V2 create unfolded (seconds after CreateAgentRuntime):

| Runtime | Image pulls | Agent booted | First ping after boot | Snapshot taken | Test restore | READY | First session |
|---|---|---|---|---|---|---|---|
| V2 · PUBLIC (endpoints/rollback) | 11, 89, 89, 89, 89 | 93, 93 | 1.7, 1.6 | 162, 162 | 172, 172 | 198 | 1123 |
| V2 · VPC | 10, 453, 453, 453, 453 | 456, 457 | 1.7, 1.6 | 526, 536 | 536, 546 | 562 | 754 |
| V2 · PUBLIC | 11, 90, 90, 90, 90 | 93, 93 | 1.7, 1.6 | 163, 173 | 173, 183 | 197 | 754 |

## 3. Rollback

| Step | Sessions | Started as | POC_MARKER | Distinct startup tokens | Cold invoke p50 | Frozen p50 |
|---|---|---|---|---|---|---|
| Before: DEFAULT on V2 | 1 | 1 snapshot restore | m3 | 1 | 2.37 s | 461.12 s |
| After V2→V1: new sessions on DEFAULT | 2 | 2 fresh boot | m3 | 2 | 3.41 s | 0.00 s |
| After V2→V1: LIVE (pinned to an older V2 version) | 2 | 2 snapshot restore | m2 | 2 | 2.02 s | 52.18 s |
| After V1→V2: new sessions on DEFAULT | 2 | 2 snapshot restore | m3 | 2 | 1.87 s | 27.64 s |

- V2→V1 update: READY v4 in 6 s, pulled tag ci ×13, digest B-r1027191511 ×12
- V1→V2 update: READY v5 in 3.1 min, 2 snapshot boots, pulled tag ci ×3, digest B-r1027191511 ×2
- `GetAgentRuntime.platformVersion`: before rollback = V2, after rollback = V1, after rollforward = V2
- Session opened before the first switch kept its instance across 2 later calls: True
- Versions: v1 V2 m1, v2 V2 m2, v3 V2 m3, v4 V1 m3, v5 V2 m3

## 4. Same-tag updates

All runtimes were created from tag `ci` → build `A-r1027191511`. The tag was then moved to build `B-r1027191511`, and each runtime was updated twice with its configuration unchanged.

| Runtime | Build before | Tag moved, no update | Same-tag update | Build after | No-op update | Build after |
|---|---|---|---|---|---|---|
| V1 · PUBLIC | A-r1027191511 | A-r1027191511 | READY v2 in 6 s, pulled digest B-r1027191511 ×14, tag ci ×15 | B-r1027191511 | READY v3 in 6 s, pulled tag ci ×13, digest B-r1027191511 ×12 | B-r1027191511 |
| V1 · VPC | A-r1027191511 | A-r1027191511 | READY v2 in 16 s, pulled digest B-r1027191511 ×14, tag ci ×15 | B-r1027191511 | READY v3 in 16 s, pulled tag ci ×13, digest B-r1027191511 ×12 | B-r1027191511 |
| V2 · PUBLIC | A-r1027191511 | A-r1027191511 | READY v2 in 3.1 min, 2 snapshot boots, pulled tag ci ×3, digest B-r1027191511 ×2 | B-r1027191511 | READY v3 in 3.1 min, 2 snapshot boots, pulled tag ci ×3, digest B-r1027191511 ×2 | B-r1027191511 |
| V2 · VPC | A-r1027191511 | A-r1027191511 | READY v2 in 3.4 min, 2 snapshot boots, pulled tag ci ×3, digest B-r1027191511 ×2 | B-r1027191511 | READY v3 in 3.4 min, 2 snapshot boots, pulled tag ci ×3, digest B-r1027191511 ×2 | B-r1027191511 |

## 5. Endpoints

| Configuration | Endpoint | Sessions | POC_MARKER (version) | Image build | Snapshot (startup token) | Snapshot built by | Started as |
|---|---|---|---|---|---|---|---|
| DEFAULT, LIVE, test → v1 | DEFAULT | 2 | m1 | A-r1027191511 | d8119717 | create | 2 snapshot restore |
| DEFAULT, LIVE, test → v1 | LIVE | 2 | m1 | A-r1027191511 | 0095c5db | endpoint_create:LIVE | 2 snapshot restore |
| DEFAULT, LIVE, test → v1 | test | 2 | m1 | B-r1027191511 | 40c27ba9 | endpoint_create:test | 2 snapshot restore |
| DEFAULT, test → v2; LIVE → v1 | DEFAULT | 2 | m2 | B-r1027191511 | 3c2b217d, b5158cfe | q5.update_m2 | 2 snapshot restore |
| DEFAULT, test → v2; LIVE → v1 | LIVE | 2 | m1 | A-r1027191511 | 552c0a57 | endpoint_create:LIVE | 2 snapshot restore |
| DEFAULT, test → v2; LIVE → v1 | test | 2 | m2 | B-r1027191511 | 7d9fa2aa | endpoint_update:test | 2 snapshot restore |
| LIVE → v2 again, after no endpoint pointed at it | LIVE | 2 | m2 | B-r1027191511 | ad006329, f5f987db | endpoint_update:LIVE | 2 snapshot restore |

Lifecycle steps on this runtime:

| Step | Version | Outcome | Time | Snapshot builds (boots) | Builder sessions | Pulled |
|---|---|---|---|---|---|---|
| create | 1 | READY | 3.3 min | 2 | snapstart-build-DEFAULT | tag ci ×3, digest A-r1027191511 ×2 |
| endpoint_create:LIVE | 1 | READY | 3.1 min | 2 | snapstart-build-LIVE | digest A-r1027191511 ×2, tag ci ×2 |
| endpoint_create:test | 1 | READY | 3.1 min | 2 | snapstart-build-test | tag ci ×2, digest B-r1027191511 ×2 |
| q5.update_m2 | 2 | READY | 3.1 min | 2 | snapstart-build-DEFAULT | tag ci ×3, digest B-r1027191511 ×2 |
| endpoint_update:test | 2 | READY | 3.1 min | 2 | snapstart-build-test | tag ci ×2, digest B-r1027191511 ×2 |
| q5.update_m3 | 3 | READY | 3.1 min | 2 | snapstart-build-DEFAULT | tag ci ×3, digest B-r1027191511 ×2 |
| endpoint_update:test | 3 | READY | 3.1 min | 2 | snapstart-build-test | tag ci ×2, digest B-r1027191511 ×2 |
| endpoint_update:LIVE | 2 | READY | 3.1 min | 2 | snapstart-build-LIVE | digest B-r1027191511 ×2, tag ci ×2 |

## 6. Pricing

| Runtime | Step | Build minutes | Snapshot boots | Sessions nearby | Usage delivered for it |
|---|---|---|---|---|---|
| V2 · PUBLIC (endpoints/rollback) | create | 3.3 | 2 | False | True |
| V2 · PUBLIC (endpoints/rollback) | endpoint_create:LIVE | 3.1 | 2 | False | True |
| V2 · PUBLIC (endpoints/rollback) | endpoint_create:test | 3.1 | 2 | False | True |
| V2 · PUBLIC (endpoints/rollback) | q5.update_m2 | 3.1 | 2 | True | True |
| V2 · PUBLIC (endpoints/rollback) | endpoint_update:test | 3.1 | 2 | True | True |
| V2 · PUBLIC (endpoints/rollback) | q5.update_m3 | 3.1 | 2 | True | False |
| V2 · PUBLIC (endpoints/rollback) | endpoint_update:test | 3.1 | 2 | True | False |
| V2 · PUBLIC (endpoints/rollback) | endpoint_update:LIVE | 3.1 | 2 | True | False |
| V2 · PUBLIC (endpoints/rollback) | q3.rollforward_to_v2 | 3.1 | 2 | True | False |
| V2 · VPC | create | 9.4 | 2 | False | True |
| V2 · VPC | q4.same_tag_update | 3.3 | 2 | True | True |
| V2 · VPC | q4.noop_update | 3.4 | 2 | True | True |
| V2 · PUBLIC | create | 3.3 | 2 | False | True |
| V2 · PUBLIC | q4.same_tag_update | 3.1 | 2 | True | True |
| V2 · PUBLIC | q4.noop_update | 3.1 | 2 | True | True |

| Runtime | Usage records | Sessions | Records for sessions not opened by the PoC | Records with no session | Records inside quiet builds | vCPU-h | GB-h | ≈ USD |
|---|---|---|---|---|---|---|---|---|
| V2 · PUBLIC (endpoints/rollback) | 179 | 12 | 0 | 0 | 0 | 0.010291 | 0.062906 | 0.002376 |

| Platform | Sessions | Metered s p50 | …before the 1st call (lead) | …from the 1st call | Metered after last reply | 1st metered second GB / vCPU | Then GB p50 | GB-h p50 | vCPU-h p50 | ≈ USD p50 |
|---|---|---|---|---|---|---|---|---|---|---|
| V2 | 12 | 14 | 0 s (0 s earlier) | 14 s | 13 s | 1.49 / 2.00 | 1.33 | 0.005206 | 0.000851 | 0.000198 |

## Also measured

- **V2 environment variables, ~3 KB** (docs: 2.5 KB cap for container agents on V2, 4 KB on V1): accepted by CreateAgentRuntime (the runtime was deleted before it finished building)
- **Startup state is shared**: across 20 V2 sessions, a uuid4() drawn at import took only 2 distinct values on V2 · PUBLIC, 2 distinct values on V2 · VPC (one per snapshot copy); every session reported hostname localhost and PID 1
- **Startup code runs more than once per V2 version**: processes booted per create build: {'v2_endpoints': 2, 'v2_vpc': 2, 'v2_public': 2} (plus a test restore of each)
- **Inbound auth**: an unsigned InvokeAgentRuntime was refused with HTTP 403 AccessDeniedException
- **Create/update → READY**: V2 · PUBLIC (endpoints/rollback) create (V2) 3.3 min; V2 · PUBLIC (endpoints/rollback) q5.update_m2 (V2) 3.1 min; V2 · PUBLIC (endpoints/rollback) q5.update_m3 (V2) 3.1 min; V2 · PUBLIC (endpoints/rollback) q3.rollback_to_v1 (V1) 6 s; V2 · PUBLIC (endpoints/rollback) q3.rollforward_to_v2 (V2) 3.1 min; V2 · VPC create (V2) 9.4 min; V2 · VPC q4.same_tag_update (V2) 3.4 min; V2 · VPC q4.noop_update (V2) 3.4 min; V2 · PUBLIC create (V2) 3.3 min; V2 · PUBLIC q4.same_tag_update (V2) 3.1 min; V2 · PUBLIC q4.noop_update (V2) 3.1 min; V1 · VPC create (V1) 4.5 min; V1 · VPC q4.same_tag_update (V1) 16 s; V1 · VPC q4.noop_update (V1) 16 s; V1 · PUBLIC create (V1) 18 s; V1 · PUBLIC q4.same_tag_update (V1) 6 s; V1 · PUBLIC q4.noop_update (V1) 6 s

## Method

- One probe image: python:3.12-slim + a standard-library HTTP agent + a fixed 32 MiB file, assembled and pushed by `image.py` (no Docker). Builds A and B differ only in a build id.
- The agent recognizes a V2 restore from inside the process. Monotonic time does not advance across a restore, so the gap between wall-clock and monotonic time jumps, and the agent records when. It also draws a random token at import, which a snapshot freezes: sessions restored from the same snapshot report the same token.
- Each runtime has its own execution role, so CloudTrail's ECR pull events name the runtime and the step that pulled (`snapstart-build-<endpoint>` for snapshot builds).
- Runtimes use IAM (SigV4) inbound auth. Sessions are stopped after use; idle timeout is 10 min.
