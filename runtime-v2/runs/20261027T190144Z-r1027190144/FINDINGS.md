# AgentCore Runtime V2: six questions, measured

Run `r1027190144` · Region `us-west-2` · 2026-10-27 19:01 UTC · 10 new sessions per runtime for the cold-start comparison. Every number below comes from this run's API calls, invocations, the agent's own logs and CloudTrail (raw data in this directory).

## Answers

| # | Question | Measured answer |
|---|---|---|
| 1 | VPC mode: is V2 supported, and does it change restore/start time vs PUBLIC? | **No**: the V2 runtime in VPC mode ended None: None |
| 2 | Pre-started instances: does V2 keep a pool, or does every new session restore on demand? | Not tested in this run. |
| 3 | Rollback: can a runtime created on V2 be switched back to V1? | Not tested in this run. |
| 4 | Same-tag updates: does an update to the same tag re-pull the image and rebuild the snapshot? | Not tested in this run. |
| 5 | Endpoints: is the snapshot per version, and does each endpoint restore its own version? | Not tested in this run. |
| 6 | Pricing: is snapshot creation, storage or restore charged? | USAGE_LOGS records not collected (yet). Cost Explorer not checked yet (run `poc.py pricing` a day later). The pricing page has no snapshot line item, and V2 bills at its own rates ($0.1276/vCPU-h, $0.0169/GB-h vs V1 $0.0895, $0.00945). This is evidence of what was metered, not a pricing commitment. |

## 1. VPC mode

| Runtime | Create → READY | Sessions OK | Cold invoke p50 / p75 / p90 | Warm p50 | Start-up overhead p50 | First read of a 0 MiB image file: min / p50 / max | Second read p50 |
|---|---|---|---|---|---|---|---|
| V1 · PUBLIC | – | 0/0 | – / – / – | – | – | – / – / – | – |
| V1 · VPC | – | 0/0 | – / – / – | – | – | – / – / – | – |
| V2 · PUBLIC | – | 0/0 | – / – / – | – | – | – / – / – | – |
| V2 · VPC | – | 0/0 | – / – / – | – | – | – / – / – | – |

*Cold invoke*: InvokeAgentRuntime with a new session id, timed by the client (includes the round trip to the Region). *Start-up overhead*: cold minus the next call on the same session. *File read*: a full read of a file baked into the image, on the session's third and fourth calls.


## 2. Pre-started instances

How a V2 create unfolded (seconds after CreateAgentRuntime):

| Runtime | Image pulls | Agent booted | First ping after boot | Snapshot taken | Test restore | READY | First session |
|---|---|---|---|---|---|---|---|
| V2 · PUBLIC (endpoints/rollback) | – | – | – | – | – | 198 | – |

## 3. Rollback

## 4. Same-tag updates

## 5. Endpoints

## 6. Pricing

| Runtime | Step | Build minutes | Snapshot boots | Sessions nearby | Usage delivered for it |
|---|---|---|---|---|---|
| V2 · PUBLIC (endpoints/rollback) | create | 3.3 | 0 | False | False |

## Also measured

- **Startup code runs more than once per V2 version**: processes booted per create build: {'v2_endpoints': 0, 'v2_public': 0, 'v2_vpc': 0} (plus a test restore of each)
- **Create/update → READY**: V2 · PUBLIC (endpoints/rollback) create (V2) 3.3 min

## Method

- One probe image: python:3.12-slim + a standard-library HTTP agent + a fixed 32 MiB file, assembled and pushed by `image.py` (no Docker). Builds A and B differ only in a build id.
- The agent recognizes a V2 restore from inside the process. Monotonic time does not advance across a restore, so the gap between wall-clock and monotonic time jumps, and the agent records when. It also draws a random token at import, which a snapshot freezes: sessions restored from the same snapshot report the same token.
- Each runtime has its own execution role, so CloudTrail's ECR pull events name the runtime and the step that pulled (`snapstart-build-<endpoint>` for snapshot builds).
- Runtimes use IAM (SigV4) inbound auth. Sessions are stopped after use; idle timeout is 10 min.
