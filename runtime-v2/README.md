# AgentCore Runtime V2: six questions, measured

**Measured answers for `us-west-2` (2026-10-27) are in [`FINDINGS.md`](FINDINGS.md).** Per-run
evidence is under `runs/`.

AgentCore Runtime V2 (`platformVersion=V2`) starts every session by restoring a snapshot of your
agent instead of booting it. Several of its behaviours are undocumented. This PoC measures them in
your own account and writes the answers, with the evidence, to `runs/<run>/FINDINGS.md`:

| # | Question | How the PoC answers it |
|---|---|---|
| 1 | Is V2 supported in VPC mode, and does VPC change restore/start time? | The same image on four runtimes (V1/V2 × PUBLIC/VPC), with N new sessions each. Records create→READY time, cold-invoke latency (p50/p75/p90) and first-file-read time. The VPC is private-only (no internet, S3 gateway endpoint plus ECR/Logs interface endpoints), and the agent checks that from inside the session. |
| 2 | Does V2 keep pre-started/restored instances, or restore every session on demand? | The agent detects its own restore and reports how long before its first request it happened. A quiet window with zero traffic checks whether anything was started or restored ahead of time. V1 runs alongside for comparison. |
| 3 | Can a runtime created on V2 be switched back to V1? | V2 → V1 → V2 on one runtime: READY times and the reported `platformVersion`. Checks how new sessions behave, whether a session left running across both switches survives, and what happens to an endpoint pinned to the old V2 version. |
| 4 | Does an update to the same image tag re-pull the image and rebuild the snapshot? | The runtimes are created from tag `ci` (build A). The PoC then moves `ci` to build B and starts new sessions without an update. Next comes an update with an unchanged config (same tag), then an update where nothing changed at all. Each step records which build runs, whether the snapshot is new, and the ECR pulls in CloudTrail. |
| 5 | Is the snapshot per runtime version, and does each endpoint restore the version it points to? | DEFAULT, `LIVE` and `test` endpoints are moved across three versions. The snapshot each endpoint restores is identified by a random token drawn at startup, which the snapshot freezes. A final step re-points an endpoint at a version nothing else references. |
| 6 | Is snapshot creation, storage or restore charged? | USAGE_LOGS (AgentCore's per-session, per-second metering) and per-runtime CloudWatch usage metrics during every snapshot build. A day later, `poc.py pricing` lists the run's Cost Explorer usage types. |

It also checks three gotchas:
- the V2 environment-variable cap;
- startup state that every session shares (random IDs, hostname, PID);
- slow first file reads after a restore.

## How it detects a restore

`agent/agent.py` is a standard-library HTTP agent that implements the AgentCore contract
(`/ping`, `/invocations` on 8080). Every response says how the process behind it was started:

- **`boot_token`** is a random ID drawn at import. A V2 snapshot freezes it, so every session of a
  version reports the same one. On V1, each instance boots and draws its own.
- **`frozen_s`**: monotonic time does not advance across a V2 restore, but wall-clock time does. The
  gap between them jumps by the snapshot's age. The agent records when that jump happened. On a
  session's first request, *time since restore* tells an on-demand restore (well under a second)
  apart from a pre-restored pool (however long the instance waited).
- **`prestart_lead_s`** is how long the process ran before its first request. For V1 it shows
  instances started ahead of time, which is V1's warm pool.

The image is python:3.12-slim plus the agent and a fixed 32 MiB file. `image.py` assembles it and
pushes it to ECR through the ECR API, so **no Docker is needed**.

## Prerequisites

Python 3.10+ and credentials for the target account (`--profile` or the usual chain).

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt        # boto3 with platformVersion support
```

The caller needs permissions to create and delete, in one Region:
- AgentCore runtimes and endpoints (`bedrock-agentcore:*` on runtimes, `InvokeAgentRuntime`, `StopRuntimeSession`);
- an ECR repository, an IAM role (`iam:CreateRole`, `PutRolePolicy`, `PassRole`);
- a VPC with subnets, a route table, security groups and VPC endpoints;
- CloudWatch Logs groups and vended-log deliveries (`logs:PutDeliverySource`, `PutDeliveryDestination`, `CreateDelivery`).

For evidence it also needs `cloudtrail:LookupEvents`, `cloudwatch:ListMetrics` / `GetMetricData`
and, for `pricing`, `ce:GetCostAndUsage`. VPC mode creates the
`AWSServiceRoleForBedrockAgentCoreNetwork` service-linked role the first time it is used.

## Running

```bash
python poc.py run                                   # all six, us-west-2, ~1 hour, tears down at the end
python poc.py run --region us-east-1 --only q1,q2   # subset; V2 Regions: us-east-1/2, us-west-2, eu-west-1, ap-northeast-1
python poc.py run --subnets subnet-a,subnet-b --security-groups sg-x   # your own VPC instead of a new one
python poc.py report runs/<run>                     # later: re-pull CloudTrail/usage logs, rewrite FINDINGS.md
python poc.py pricing runs/<run>                    # next day: Cost Explorer usage types for the run
python poc.py teardown runs/<run>                   # after --keep, a crash, or if VPC pieces were still in use
```

Useful knobs: `--samples` (new sessions per runtime for Q1/Q2, default 10), `--step-sessions`
(sessions per endpoint after each change, default 2), `--quiet-s` (Q2's zero-traffic window,
default 180), `--settle-min` (wait for CloudTrail before reporting, default 10), `--keep`.

**To reproduce a specific network exactly**, run Q1/Q2 with `--subnets/--security-groups` set to
those private subnets. Those subnets need ECR (api + dkr), CloudWatch Logs and the S3 gateway
endpoint, or NAT. Runtimes created after the May 2026 VPC change no longer get a service-managed S3
gateway.

## What it creates, and what it costs

Everything is tagged `agentcore-v2-poc=<run>` and listed in `runs/<run>/state.json` as it is
created. Teardown deletes exactly that list:
- 5 runtimes (V1/V2 × PUBLIC/VPC, plus a V2 runtime for the endpoint and rollback tests), all with
  IAM (SigV4) inbound auth;
- 1 ECR repository and 1 execution role;
- 1 VPC: 2 private subnets, no internet route, an S3 gateway endpoint, and ECR api/dkr and Logs
  interface endpoints;
- 1 log group with USAGE_LOGS deliveries.

A full run is roughly 60 sessions of a few seconds each, a dozen create/update cycles and about an
hour of interface endpoints (3 × 2 AZs). That comes to well under US$1.

At the end of a run, teardown deletes the runtimes, interface endpoints, roles and repository. It
keeps the run's log groups, which have 30-day retention, because USAGE_LOGS arrive 10–25 minutes
late; `poc.py report` can then pick up the rest. AgentCore also keeps a VPC runtime's network
interfaces for up to 8 hours after the runtime is deleted, so the subnets and security groups can
stay in use for a while.

Once you have re-run `report`, run `poc.py teardown runs/<run>` to remove the log groups and
whatever AgentCore has released. The leftover pieces are free; the billed interface endpoints are
deleted first.

## Reading FINDINGS.md

The table at the top answers the six questions in one or two sentences each, with the numbers
measured in that run. The sections below it hold the evidence: per-runtime latency tables,
per-session restore timing, the builds and snapshot tokens seen after each change, the endpoint
→ version → snapshot mapping, and the usage records during snapshot builds. `results.json` holds the
same data in machine-readable form, and `events.jsonl` every call and invocation.

Caveats that matter when sharing results:
- **Latency** is measured from wherever you run the PoC. It includes the network round trip to the
  Region, so compare runtimes within a run rather than across machines.
- **Q2** shows behaviour at the PoC's traffic level: sequential sessions, a few seconds apart. A
  pool that only appears under sustained load would not show up here.
- **Q6** is evidence of what was metered in your account for these snapshot builds. It is not a
  pricing commitment: only AWS pricing can make that.
- **CloudTrail** lags by up to ~15 minutes, and USAGE_LOGS and metrics by minutes to an hour.
  `poc.py report` can be re-run.
