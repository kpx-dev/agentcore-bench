#!/usr/bin/env python3
"""Answer six AgentCore Runtime V2 questions by measuring them in your own account.

    python poc.py run                       # everything (~1 hour), tears its resources down at the end
    python poc.py run --only q2,q4          # a subset (q1 vpc, q2 pre-start, q3 rollback, q4 same tag,
                                            #           q5 endpoints, q6 pricing)
    python poc.py report runs/<run>         # re-pull late evidence (CloudTrail, usage logs) and rewrite FINDINGS.md
    python poc.py pricing runs/<run>        # a day later: Cost Explorer usage types for the run
    python poc.py teardown runs/<run>       # delete what a run created (after --keep, a crash, or lingering ENIs)

See README.md for what each experiment does and how to read FINDINGS.md.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import datetime as dt
import json
import logging
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

import evidence
import image
import report
from infra import TAG_KEY, Infra
from runtime import AgentCore, Runtime, code, message

log = logging.getLogger("poc")
HERE = Path(__file__).resolve().parent
V2_REGIONS = ("us-east-1", "us-east-2", "us-west-2", "eu-west-1", "ap-northeast-1")
MATRIX = (("v1_public", "V1", "PUBLIC"), ("v1_vpc", "V1", "VPC"), ("v2_public", "V2", "PUBLIC"), ("v2_vpc", "V2", "VPC"))
ENDPOINTS_RT = ("v2_endpoints", "V2", "PUBLIC")
CI_TAG = "ci"     # the one mutable tag every runtime is created from, as in a pipeline that reuses its tag
ALL = ("q1", "q2", "q3", "q4", "q5", "q6")


# --------------------------------------------------------------------------- run directory
class Run:
    """state.json (what exists, for teardown) and events.jsonl (everything that happened, for the report)."""

    def __init__(self, path: Path):
        self.dir = path
        path.mkdir(parents=True, exist_ok=True)
        self.state_path = path / "state.json"
        self.state: dict = json.loads(self.state_path.read_text()) if self.state_path.exists() else {}
        self.lock = threading.RLock()     # guards state: experiments and setup mutate it from several threads
        self._events_lock = threading.Lock()
        self._events = (path / "events.jsonl").open("a", buffering=1)

    @property
    def name(self) -> str:
        return self.state["run"]

    def save(self) -> None:
        with self.lock:
            tmp = self.state_path.with_suffix(".tmp")
            tmp.write_text(json.dumps(self.state, indent=1, default=str))
            tmp.replace(self.state_path)

    def record(self, kind: str, **fields) -> None:
        with self._events_lock:
            self._events.write(json.dumps({"t": time.time(), "kind": kind, **fields}, default=str) + "\n")


def setup_logging(run_dir: Path | None) -> None:
    fmt = logging.Formatter("%(asctime)s %(levelname)-7s [%(threadName)s] %(message)s", "%H:%M:%S")
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    for h in list(root.handlers):
        root.removeHandler(h)
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    console.setFormatter(fmt)
    root.addHandler(console)
    if run_dir:
        fh = logging.FileHandler(run_dir / "poc.log")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(fmt)
        root.addHandler(fh)
    for noisy in ("botocore", "urllib3", "boto3"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def parallel(fn, items: list, name: str) -> dict:
    """Run fn(item) concurrently; one item failing is recorded and does not stop the others."""
    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(items)), thread_name_prefix=name) as pool:
        futures = {pool.submit(fn, it): it for it in items}
        for fut in concurrent.futures.as_completed(futures):
            key = getattr(futures[fut], "key", str(futures[fut]))
            try:
                results[key] = fut.result()
            except Exception as exc:  # noqa: BLE001
                log.error("%s %s failed: %s", name, key, exc, exc_info=log.isEnabledFor(logging.DEBUG))
                results[key] = exc
    return results


# --------------------------------------------------------------------------- experiments
class Experiments:
    def __init__(self, args, session, run: Run, ac: AgentCore, repo: image.EcrRepository):
        self.args, self.session, self.run, self.ac, self.repo = args, session, run, ac, repo
        self.record = run.record

    def sessions(self, rt: Runtime, step: str, n: int, qualifier: str = "DEFAULT", ops=("probe",)) -> list[dict]:
        out = []
        for _ in range(n):
            out += self.ac.session(rt, step, ops, qualifier=qualifier)
        return out

    # -- Q1 / Q2 -------------------------------------------------------------------------------
    def cold_starts(self, matrix: list[Runtime]) -> None:
        """Q1 + Q2: sequential new sessions on every runtime; each session: cold call, warm call, two reads of
        the image's ballast file (first read after start, then cached), plus one egress probe per runtime."""
        def per_runtime(rt: Runtime) -> None:
            for i in range(self.args.samples):
                ops = ("probe", "probe", "ballast", "ballast") + (("egress",) if i == 0 else ())
                first = self.ac.session(rt, "q1", ops)[0]
                log.info("%s q1 %d/%d: cold %s ms, started=%s", rt.key, i + 1, self.args.samples, first["ms"],
                         (first.get("data") or {}).get("started"))
                time.sleep(self.args.gap_s)
        parallel(per_runtime, matrix, "q1")

    # -- Q4 ------------------------------------------------------------------------------------
    def same_tag(self, matrix: list[Runtime], build_b: image.Image) -> None:
        """Q4: move the shared tag to build B, then: new sessions with no update; an update that names the same
        tag with nothing else changed; and a second update with nothing changed at all."""
        build_b.push(CI_TAG)
        digest = self.repo.digest_of(CI_TAG)
        self.record("q4_tag_moved", tag=CI_TAG, digest=digest, expected=build_b.digest, build=build_b.build_id)
        if digest != build_b.digest:
            raise RuntimeError(f"tag {CI_TAG} points at {digest}, expected {build_b.digest}")
        n = self.args.step_sessions

        def per_runtime(rt: Runtime) -> None:
            self.sessions(rt, "q4.after_move", n)
            for action, step in (("q4.same_tag_update", "q4.after_update"), ("q4.noop_update", "q4.after_noop")):
                try:
                    self.ac.update(rt, action)
                except ClientError as exc:
                    log.warning("%s %s rejected: %s", rt.key, action, message(exc))
                    if action == "q4.same_tag_update":
                        return
                    continue
                self.sessions(rt, step, n)
        parallel(per_runtime, matrix, "q4")

    # -- Q5 ------------------------------------------------------------------------------------
    def endpoints(self, rt: Runtime) -> None:
        """Q5: which snapshot each endpoint restores as the endpoints and the runtime move between versions."""
        n, eps = self.args.step_sessions, ("DEFAULT", "LIVE", "test")
        v1 = rt.version
        self.ac.create_endpoint(rt, "LIVE", v1)
        self.ac.create_endpoint(rt, "test", v1)
        for q in eps:
            self.sessions(rt, "q5.all_on_v1", n, q)
        self.ac.update(rt, "q5.update_m2", marker="m2")
        v2 = rt.version
        self.ac.update_endpoint(rt, "test", v2)
        for q in eps:
            self.sessions(rt, "q5.split", n, q)
        self.ac.update(rt, "q5.update_m3", marker="m3")
        v3 = rt.version
        self.ac.update_endpoint(rt, "test", v3)   # now no endpoint points at v2: its snapshot is released
        self.record("q5_versions", v1=v1, v2=v2, v3=v3)
        time.sleep(60)
        self.ac.update_endpoint(rt, "LIVE", v2)   # ...and point one back at it
        self.sessions(rt, "q5.repoint_unreferenced", n, "LIVE")

    # -- Q3 ------------------------------------------------------------------------------------
    def rollback(self, rt: Runtime) -> None:
        """Q3: V2 -> V1 -> V2 on the same runtime, with a session left running across both switches and an
        endpoint pinned to an older V2 version."""
        n = self.args.step_sessions
        try:
            self.ac.endpoint(rt, "LIVE")
        except ClientError:
            self.ac.create_endpoint(rt, "LIVE", rt.version)
        sid = f"poc-inflight-{uuid.uuid4().hex}"
        self.ac.session(rt, "q3.inflight_before", stop=False, session_id=sid)
        before = self.ac.get(rt).get("platformVersion")
        self.ac.update(rt, "q3.rollback_to_v1", platform="V1")
        after = self.ac.get(rt).get("platformVersion")
        self.record("q3_platform", before=before, after_rollback=after, version=rt.version)
        self.sessions(rt, "q3.after_rollback", n)
        self.sessions(rt, "q3.pinned_after_rollback", n, "LIVE")
        self.ac.session(rt, "q3.inflight_after_rollback", stop=False, session_id=sid)
        self.ac.update(rt, "q3.rollforward_to_v2", platform="V2")
        self.record("q3_platform", before=after, after_rollforward=self.ac.get(rt).get("platformVersion"), version=rt.version)
        self.sessions(rt, "q3.after_rollforward", n)
        self.ac.session(rt, "q3.inflight_after_rollforward", session_id=sid)
        self.record_versions(rt)

    def record_versions(self, rt: Runtime) -> None:
        versions = []
        for page in self.ac.ctl.get_paginator("list_agent_runtime_versions").paginate(agentRuntimeId=rt.id):
            for v in page.get("agentRuntimes", []):
                info = self.ac.get(rt, v["agentRuntimeVersion"])
                versions.append({"version": v["agentRuntimeVersion"], "status": v.get("status"),
                                 "platform": info.get("platformVersion"),
                                 "marker": (info.get("environmentVariables") or {}).get("POC_MARKER"),
                                 "image": info.get("agentRuntimeArtifact", {}).get("containerConfiguration", {}).get("containerUri")})
        self.record("versions", rt=rt.key, versions=sorted(versions, key=lambda v: int(v["version"])))

    # -- extras --------------------------------------------------------------------------------
    def env_limit(self, template: Runtime, sizes=(3000, 4500)) -> None:
        """V2 documents a 2.5 KB cap on container environment variables (V1: 4 KB). For one size above the V2 cap and
        one above V1's: does CreateAgentRuntime reject it, and if it accepts it, does the runtime then build?"""
        base = template.name.rsplit("_", 2)[0]

        def attempt(nbytes: int) -> None:
            key = f"env_{nbytes}"
            env = {"POC_MARKER": "envcap"}
            env["POC_BIG"] = "x" * (nbytes - sum(len(k) + len(v) for k, v in env.items()) - len("POC_BIG"))
            probe = Runtime(key, "V2", "PUBLIC", f"{base}_{key}", template.image_uri, marker="envcap",
                            role_arn=template.role_arn)
            kwargs = self.ac._config(probe) | {"agentRuntimeName": probe.name, "environmentVariables": env}
            t0 = time.time()
            try:
                resp = self.ac.ctl.create_agent_runtime(**kwargs)
            except ClientError as exc:
                self.record("extra_env_limit", bytes=nbytes, accepted=False, error_code=code(exc), error=message(exc))
                log.info("Env vars of %d bytes on V2: CreateAgentRuntime rejected them: %s", nbytes, message(exc))
                return
            probe.id, probe.arn = resp["agentRuntimeId"], resp["agentRuntimeArn"]
            with self.run.lock:
                self.run.state.setdefault("runtimes", {})[key] = {"id": probe.id, "arn": probe.arn, "name": probe.name,
                                                                  "platform": "V2", "network": "PUBLIC"}
                self.run.save()
            try:
                outcome = self.ac.wait_ready(probe, f"env_limit_{nbytes}", t0)
                status, reason = outcome["status"], None
            except RuntimeError as exc:
                info = self.ac.get(probe)
                status, reason = info.get("status"), info.get("failureReason") or str(exc)
            self.record("extra_env_limit", bytes=nbytes, accepted=True, status=status, failure_reason=reason,
                        seconds=round(time.time() - t0, 1))
            log.info("Env vars of %d bytes on V2: accepted, runtime ended %s %s", nbytes, status, reason or "")
            self.ac.delete(probe.id)
        parallel(attempt, list(sizes), "envcap")

    def anonymous_denied(self, rt: Runtime) -> None:
        """Security check: the runtimes use IAM (SigV4) inbound auth, so an unsigned invoke must be refused."""
        url = (f"https://bedrock-agentcore.{self.args.region}.amazonaws.com/runtimes/"
               f"{urllib.parse.quote(rt.arn, safe='')}/invocations?qualifier=DEFAULT")
        req = urllib.request.Request(url, data=b'{"op":"probe"}', method="POST", headers={
            "Content-Type": "application/json",
            "X-Amzn-Bedrock-AgentCore-Runtime-Session-Id": f"poc-anonymous-{uuid.uuid4().hex}"})
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                status, etype, body = resp.status, resp.headers.get("x-amzn-ErrorType"), resp.read(300).decode(errors="replace")
        except urllib.error.HTTPError as exc:
            status, etype, body = exc.code, exc.headers.get("x-amzn-ErrorType"), exc.read(300).decode(errors="replace")
        except (urllib.error.URLError, OSError) as exc:
            self.record("extra_anonymous_invoke", rt=rt.key, error=str(exc))
            log.warning("Unsigned-invoke check could not connect: %s", exc)
            return
        self.record("extra_anonymous_invoke", rt=rt.key, status=status, error_type=etype, body=body)
        if status < 400:
            raise RuntimeError(f"{rt.key} answered an UNSIGNED invoke with HTTP {status}; stopping")
        log.info("Unsigned invoke refused as expected: HTTP %s %s", status, etype or body[:80])


# --------------------------------------------------------------------------- commands
def preflight(session, region: str) -> dict:
    if region not in V2_REGIONS:
        raise SystemExit(f"AgentCore Runtime V2 is offered in {', '.join(V2_REGIONS)}; got {region}")
    model = session.client("bedrock-agentcore-control", region_name=region).meta.service_model
    if "platformVersion" not in model.operation_model("CreateAgentRuntime").input_shape.members:
        raise SystemExit("This boto3/botocore has no platformVersion on CreateAgentRuntime: pip install -U boto3")
    ident = session.client("sts", region_name=region).get_caller_identity()
    return {"account": ident["Account"], "caller": ident["Arn"]}


def plan(args, run: str, uri: str) -> tuple[list[Runtime], Runtime | None]:
    selected = set(args.only)
    matrix = []
    if selected & {"q1", "q2", "q4", "q6"}:
        for key, platform, network in MATRIX:
            if network in args.networks and platform in args.platforms:
                matrix.append(Runtime(key, platform, network, f"v2poc_{run}_{key}", uri))
    endpoints_rt = None
    if selected & {"q3", "q5"}:
        key, platform, network = ENDPOINTS_RT
        endpoints_rt = Runtime(key, platform, network, f"v2poc_{run}_{key}", uri)
    return matrix, endpoints_rt


def cmd_run(args) -> int:
    session = boto3.Session(profile_name=args.profile, region_name=args.region)
    ident = preflight(session, args.region)
    name = args.name or dt.datetime.now(dt.timezone.utc).strftime("r%m%d%H%M%S")
    run = Run(HERE / "runs" / f"{dt.datetime.now(dt.timezone.utc):%Y%m%dT%H%M%SZ}-{name}")
    setup_logging(run.dir)
    run.state.update(run=name, region=args.region, started=time.time(), args=vars(args), **ident)
    run.save()
    log.info("Run %s in %s as %s -> %s", name, args.region, ident["caller"], run.dir)
    infra = Infra(session, args.region, ident["account"], name, run.state, run.save, run.lock)
    extras = concurrent.futures.ThreadPoolExecutor(1, thread_name_prefix="extras")
    try:
        repo_state = infra.ensure_repository()
        vpc_config = None
        if "VPC" in args.networks and set(args.only) & {"q1", "q2", "q4", "q6"}:
            if args.subnets:
                vpc_config = {"subnets": args.subnets, "securityGroups": args.security_groups}
            else:
                v = infra.ensure_vpc(args.vpc_cidr)
                vpc_config = {"subnets": v["subnets"], "securityGroups": [v["sg_runtime"]]}
            run.state["vpc_config"] = vpc_config
            run.save()

        repo = image.EcrRepository(session.client("ecr", region_name=args.region), repo_state["name"], repo_state["uri"])
        source = (HERE / "agent" / "agent.py").read_bytes()
        build_a = image.build(repo, f"A-{name}", source, args.base_image, args.ballast_mb)
        build_b = image.build(repo, f"B-{name}", source, args.base_image, args.ballast_mb)
        build_a.push("build-a")
        build_b.push("build-b")
        uri = build_a.push(CI_TAG)
        run.record("images", tag=CI_TAG, a={"build_id": build_a.build_id, "digest": build_a.digest},
                   b={"build_id": build_b.build_id, "digest": build_b.digest})

        if "q6" in args.only:
            infra.ensure_usage_destination()
        ac = AgentCore(session, args.region, None, vpc_config, run.record, args.idle_timeout_s)
        matrix, endpoints_rt = plan(args, name, uri)
        everything = matrix + ([endpoints_rt] if endpoints_rt else [])
        exp = Experiments(args, session, run, ac, repo)

        def create(rt: Runtime) -> None:
            rt.role_arn = infra.ensure_role(rt.key, rt.name)
            t0 = ac.create(rt, {TAG_KEY: name})
            with run.lock:
                run.state.setdefault("runtimes", {})[rt.key] = {"id": rt.id, "arn": rt.arn, "name": rt.name,
                                                                "platform": rt.platform, "network": rt.network}
                run.save()
            if "q6" in args.only:
                infra.add_usage_source(rt.name, rt.arn)
            ac.wait_ready(rt, "create", t0)

        created = parallel(create, everything, "create")
        ready = [rt for rt in everything if not isinstance(created.get(rt.key), Exception)]
        matrix = [rt for rt in matrix if rt in ready]
        envcap = None
        if not args.skip_extras and ready:
            exp.anonymous_denied(ready[0])
            envcap = extras.submit(exp.env_limit, ready[0])   # its builds overlap the quiet window and the experiments

        if "q2" in args.only and matrix:
            log.info("Q2: %ss with no invocations at all, so any instance started or restored now was started "
                     "ahead of time, not for a session", args.quiet_s)
            run.record("q2_quiet", start=time.time(), seconds=args.quiet_s)
            time.sleep(args.quiet_s)

        def matrix_track() -> None:
            if matrix and set(args.only) & {"q1", "q2", "q6"}:
                exp.cold_starts(matrix)
            if matrix and "q4" in args.only:
                exp.same_tag(matrix, build_b)

        def endpoint_track() -> None:
            if endpoints_rt in ready:
                if "q5" in args.only:
                    exp.endpoints(endpoints_rt)
                if "q3" in args.only:
                    exp.rollback(endpoints_rt)
        parallel(lambda f: f(), [matrix_track, endpoint_track], "track")
        for rt in matrix:
            exp.record_versions(rt)
        if envcap is not None:
            try:
                envcap.result()
            except Exception as exc:  # noqa: BLE001 - an extra must not sink the run
                log.warning("Env-var cap check failed: %s", exc)

        if args.settle_min > 0:
            log.info("Waiting %d min for CloudTrail, USAGE_LOGS and metrics to arrive", args.settle_min)
            time.sleep(args.settle_min * 60)
        evidence.collect(session, args.region, run.dir, run.state, run.save)
        res = report.write(run.dir)
        print_answers(res, run.dir)
        return 0
    finally:
        extras.shutdown(wait=True)   # anything the env-var check created is in state.json before teardown reads it
        run.state["finished"] = time.time()
        run.save()
        if args.keep:
            log.info("--keep: resources left running. Delete them with: python poc.py teardown %s", run.dir)
        else:
            # Log groups stay (30-day retention): USAGE_LOGS arrive 10-25 min late, so `poc.py report` can pick up the
            # rest later. `poc.py teardown <run>` removes them, and anything AgentCore was still holding.
            teardown(session, args.region, run, keep_logs=True)
            log.info("Kept the run's log groups for late evidence. Later: python poc.py report %s; then python poc.py "
                     "teardown %s", run.dir, run.dir)


def teardown(session, region: str, run: Run, keep_logs: bool = False) -> list[str]:
    ac = AgentCore(session, region, "", None, run.record)
    # Runtimes stay listed in state.json (the report maps usage records by ARN); deletion is flagged instead.
    live = {k: rt for k, rt in run.state.get("runtimes", {}).items() if not rt.get("deleted")}
    logs = session.client("logs", region_name=region)
    try:
        groups = set(run.state.get("runtime_log_groups", []))
        groups.update(evidence.runtime_log_groups(logs, [rt["id"] for rt in live.values()]))
        run.state["runtime_log_groups"] = sorted(groups)
    except ClientError as exc:
        log.warning("Listing runtime log groups: %s", message(exc))
    for key, rt in live.items():
        try:
            ac.delete(rt["id"])
        except ClientError as exc:
            log.warning("Deleting runtime %s: %s", key, message(exc))
    for rt in live.values():
        if ac.wait_deleted(rt["id"]):
            rt["deleted"] = True
            run.save()
    infra = Infra(session, region, run.state["account"], run.name, run.state, run.save, run.lock)
    left = infra.teardown(delete_logs=not keep_logs) + [f"runtime {k}" for k, rt in live.items() if not rt.get("deleted")]
    run.state["teardown"] = {"at": time.time(), "left": left}
    run.save()
    if left:
        log.warning("Not deleted yet (AgentCore releases its VPC network interfaces up to 8 hours after a runtime is "
                    "deleted): %s. Re-run: python poc.py teardown %s", ", ".join(left), run.dir)
    else:
        log.info("Teardown complete: nothing from run %s is left", run.name)
    return left


def print_answers(res: dict, run_dir: Path) -> None:
    print("\n" + "=" * 100)
    for i, (key, question) in enumerate(report.QUESTIONS, 1):
        print(f"\n{i}. {question}\n   {res[key].get('answer')}")
    print(f"\nDetails: {run_dir / 'FINDINGS.md'}\n" + "=" * 100)


def cmd_report(args) -> int:
    run = Run(Path(args.run_dir).resolve())
    setup_logging(None)
    if not args.no_collect:
        session = boto3.Session(profile_name=args.profile, region_name=run.state["region"])
        evidence.collect(session, run.state["region"], run.dir, run.state, run.save)
    print_answers(report.write(run.dir), run.dir)
    return 0


def cmd_pricing(args) -> int:
    run = Run(Path(args.run_dir).resolve())
    setup_logging(None)
    session = boto3.Session(profile_name=args.profile, region_name=run.state["region"])
    out = evidence.costs(session, run.dir)
    for row in out["rows"]:
        print(f"{row['date']}  {row['usage_type']:<45} {row['quantity']:>14.6f} {row['unit'] or '':<10} ${row['cost_usd']:.4f}")
    if not out["rows"]:
        print("No AgentCore line items yet (Cost Explorer lags ~24 h).")
    report.write(run.dir)
    return 0


def cmd_teardown(args) -> int:
    run = Run(Path(args.run_dir).resolve())
    setup_logging(run.dir)
    session = boto3.Session(profile_name=args.profile, region_name=run.state["region"])
    return 1 if teardown(session, run.state["region"], run, keep_logs=args.keep_logs) else 0


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0], formatter_class=argparse.RawDescriptionHelpFormatter,
                                epilog=__doc__.split("\n\n", 1)[1])
    p.add_argument("--profile", default=None, help="AWS profile (default: the usual credential chain)")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="create everything, run the experiments, report, tear down")
    r.add_argument("--region", default=os.environ.get("AWS_REGION", "us-west-2"))
    r.add_argument("--only", type=lambda s: [x.strip() for x in s.split(",")], default=list(ALL),
                   help="comma-separated subset of q1..q6")
    r.add_argument("--networks", type=lambda s: s.upper().split(","), default=["PUBLIC", "VPC"])
    r.add_argument("--platforms", type=lambda s: s.upper().split(","), default=["V1", "V2"])
    r.add_argument("--subnets", type=lambda s: s.split(","), help="use these existing private subnets instead of "
                   "creating a VPC (they need S3, ECR and Logs reachable: endpoints or NAT)")
    r.add_argument("--security-groups", type=lambda s: s.split(","), help="with --subnets")
    r.add_argument("--vpc-cidr", default="10.88.0.0/16")
    r.add_argument("--samples", type=int, default=10, help="new sessions per runtime for the cold-start comparison")
    r.add_argument("--step-sessions", type=int, default=2, help="new sessions per endpoint after each change")
    r.add_argument("--gap-s", type=float, default=2.0, help="pause between cold-start sessions")
    r.add_argument("--quiet-s", type=int, default=180, help="Q2: seconds with no traffic before the first session")
    r.add_argument("--settle-min", type=int, default=10, help="wait for CloudTrail/usage logs before reporting")
    r.add_argument("--idle-timeout-s", type=int, default=600)
    r.add_argument("--ballast-mb", type=int, default=32, help="size of the file baked into the image for read timing")
    r.add_argument("--base-image", default=image.DEFAULT_BASE)
    r.add_argument("--name", help="run name (default r<MMDDhhmm>)")
    r.add_argument("--skip-extras", action="store_true", help="skip the env-var cap and unsigned-invoke checks")
    r.add_argument("--keep", action="store_true", help="leave every resource in place (tear down later)")
    r.set_defaults(fn=cmd_run)

    for cmd, fn, helptext in (("report", cmd_report, "re-collect evidence and rewrite FINDINGS.md"),
                              ("pricing", cmd_pricing, "Cost Explorer usage types for a run (a day later)"),
                              ("teardown", cmd_teardown, "delete everything a run created")):
        s = sub.add_parser(cmd, help=helptext)
        s.add_argument("run_dir")
        if cmd == "report":
            s.add_argument("--no-collect", action="store_true", help="only re-analyse the files already pulled")
        if cmd == "teardown":
            s.add_argument("--keep-logs", action="store_true")
        s.set_defaults(fn=fn)

    args = p.parse_args(argv)
    if args.cmd == "run":
        bad = set(args.only) - set(ALL)
        if bad:
            p.error(f"unknown experiments {sorted(bad)}")
        if args.subnets and not args.security_groups:
            p.error("--subnets needs --security-groups")
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
