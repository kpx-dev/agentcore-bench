"""AgentCore Runtime control- and data-plane calls, with the waiting and timing the PoC needs.

Every call that changes something is recorded through `record` (what was asked, how long it took to
reach READY, what the service said), and so is every invocation, so the report can be rebuilt from
the run directory alone.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass

from botocore.config import Config
from botocore.exceptions import ClientError

log = logging.getLogger("poc.runtime")

TERMINAL = ("READY", "CREATE_FAILED", "UPDATE_FAILED", "DELETE_FAILED")


def code(exc: ClientError) -> str:
    return exc.response.get("Error", {}).get("Code", "")


def message(exc: ClientError) -> str:
    return exc.response.get("Error", {}).get("Message", str(exc))


def strip(resp: dict) -> dict:
    resp = dict(resp)
    resp.pop("ResponseMetadata", None)
    return resp


@dataclass
class Runtime:
    key: str        # e.g. "v2_vpc"
    platform: str   # "V1" | "V2": the platform version this runtime should be on now
    network: str    # "PUBLIC" | "VPC"
    name: str
    image_uri: str
    marker: str = "m1"   # POC_MARKER environment variable: changing it is how a test makes a new version
    role_arn: str = ""
    id: str = ""
    arn: str = ""
    version: str = ""


class AgentCore:
    def __init__(self, session, region: str, role_arn: str | None, vpc_config: dict | None, record,
                 idle_timeout_s: int = 600, max_lifetime_s: int = 3600):
        self.region, self.role_arn, self.vpc_config, self.record = region, role_arn, vpc_config, record
        self.lifecycle = {"idleRuntimeSessionTimeout": idle_timeout_s, "maxLifetime": max_lifetime_s}
        self.ctl = session.client("bedrock-agentcore-control", region_name=region,
                                  config=Config(retries={"max_attempts": 10, "mode": "adaptive"}))
        # Invocations are never retried: a retry would hide exactly the cold-start latency being measured.
        self.dp = session.client("bedrock-agentcore", region_name=region,
                                 config=Config(read_timeout=300, connect_timeout=10, retries={"total_max_attempts": 1}))
        self.dp_retry = session.client("bedrock-agentcore", region_name=region,
                                       config=Config(retries={"max_attempts": 5, "mode": "standard"}))

    # ------------------------------------------------------------ control plane
    def _config(self, rt: Runtime) -> dict:
        network = {"networkMode": rt.network}
        if rt.network == "VPC":
            network["networkModeConfig"] = self.vpc_config
        return {
            "agentRuntimeArtifact": {"containerConfiguration": {"containerUri": rt.image_uri}},
            "roleArn": rt.role_arn or self.role_arn,
            "networkConfiguration": network,
            "protocolConfiguration": {"serverProtocol": "HTTP"},
            "lifecycleConfiguration": self.lifecycle,
            "environmentVariables": {"POC_MARKER": rt.marker},
            # Always explicit: from Nov 5, 2026 an update that omits it moves the runtime to V2.
            "platformVersion": rt.platform,
        }

    def create(self, rt: Runtime, tags: dict) -> float:
        kwargs = self._config(rt) | {"agentRuntimeName": rt.name, "tags": tags,
                                     "description": f"AgentCore V2 PoC: {rt.platform} {rt.network}"}
        t0 = time.time()
        for attempt in range(1, 13):
            try:
                resp = self.ctl.create_agent_runtime(**kwargs)
                break
            except ClientError as exc:
                # A just-created execution role takes a few seconds before AgentCore can assume it.
                if code(exc) == "ValidationException" and "role" in message(exc).lower() and attempt < 12:
                    log.info("%s: execution role not assumable yet (%s), retrying", rt.key, message(exc))
                    time.sleep(5)
                    continue
                raise
        rt.id, rt.arn, rt.version = resp["agentRuntimeId"], resp["agentRuntimeArn"], resp["agentRuntimeVersion"]
        self.record("op", action="create", rt=rt.key, platform=rt.platform, network=rt.network, image=rt.image_uri,
                    marker=rt.marker, version=rt.version, status=resp["status"], t_start=t0, runtime_id=rt.id,
                    arn=rt.arn)
        log.info("%s: CreateAgentRuntime -> %s version %s (%s)", rt.key, rt.id, rt.version, resp["status"])
        return t0

    def get(self, rt: Runtime, version: str | None = None) -> dict:
        kw = {"agentRuntimeId": rt.id} | ({"agentRuntimeVersion": version} if version else {})
        return strip(self.ctl.get_agent_runtime(**kw))

    def wait_ready(self, rt: Runtime, action: str, t_start: float, timeout_s: int = 2400) -> dict:
        """Poll until a terminal status; record how long the operation took to settle."""
        deadline, last = time.monotonic() + timeout_s, None
        while True:
            info = self.get(rt)
            status = info["status"]
            if status != last:
                log.info("%s: %s %s after %.0fs", rt.key, action, status, time.time() - t_start)
                last = status
            if status in TERMINAL or time.monotonic() > deadline:
                break
            time.sleep(5)
        result = {"action": action, "rt": rt.key, "status": status, "seconds": round(time.time() - t_start, 1),
                  "version": info.get("agentRuntimeVersion"), "platform_version": info.get("platformVersion"),
                  "failure_reason": info.get("failureReason"), "t_start": t_start, "t_done": time.time()}
        self.record("op_done", **result)
        if status != "READY":
            raise RuntimeError(f"{rt.key}: {action} ended {status}: {info.get('failureReason')}")
        return result

    def update(self, rt: Runtime, action: str, **changes) -> dict:
        """UpdateAgentRuntime with the runtime's full configuration (plus `changes`), then wait for READY
        and for DEFAULT to serve the new version."""
        for k, v in changes.items():
            setattr(rt, k, v)
        kwargs = self._config(rt)
        t0 = time.time()
        deadline = time.monotonic() + 900
        while True:
            try:
                resp = self.ctl.update_agent_runtime(agentRuntimeId=rt.id, **kwargs)
                break
            except ClientError as exc:
                # The documented V2 behaviour: an update while the runtime is still settling is a conflict.
                if code(exc) == "ConflictException" and time.monotonic() < deadline:
                    self.record("op_conflict", action=action, rt=rt.key, error=message(exc))
                    time.sleep(10)
                    continue
                self.record("op_rejected", action=action, rt=rt.key, error_code=code(exc), error=message(exc),
                            t_start=t0)
                raise
        before = rt.version
        rt.version = resp["agentRuntimeVersion"]
        self.record("op", action=action, rt=rt.key, platform=rt.platform, network=rt.network, image=rt.image_uri,
                    marker=rt.marker, version=rt.version, previous_version=before, status=resp["status"], t_start=t0)
        log.info("%s: %s -> version %s (was %s, %s)", rt.key, action, rt.version, before, resp["status"])
        result = self.wait_ready(rt, action, t0)
        result["default_endpoint"] = self.wait_endpoint(rt, "DEFAULT", rt.version, record=False)
        return result

    def create_endpoint(self, rt: Runtime, name: str, version: str) -> dict:
        t0 = time.time()
        resp = self.ctl.create_agent_runtime_endpoint(agentRuntimeId=rt.id, name=name, agentRuntimeVersion=version)
        self.record("op", action=f"endpoint_create:{name}", rt=rt.key, endpoint=name, version=version,
                    status=resp["status"], t_start=t0)
        return self.wait_endpoint(rt, name, version, t_start=t0, action=f"endpoint_create:{name}")

    def update_endpoint(self, rt: Runtime, name: str, version: str) -> dict:
        t0 = time.time()
        resp = self.ctl.update_agent_runtime_endpoint(agentRuntimeId=rt.id, endpointName=name,
                                                      agentRuntimeVersion=version)
        self.record("op", action=f"endpoint_update:{name}", rt=rt.key, endpoint=name, version=version,
                    status=resp["status"], t_start=t0)
        return self.wait_endpoint(rt, name, version, t_start=t0, action=f"endpoint_update:{name}")

    def endpoint(self, rt: Runtime, name: str) -> dict:
        return strip(self.ctl.get_agent_runtime_endpoint(agentRuntimeId=rt.id, endpointName=name))

    def wait_endpoint(self, rt: Runtime, name: str, version: str, t_start: float | None = None,
                      action: str | None = None, record: bool = True, timeout_s: int = 2400) -> dict:
        t_start = t_start or time.time()
        deadline, last = time.monotonic() + timeout_s, None
        while True:
            ep = self.endpoint(rt, name)
            state = (ep.get("status"), ep.get("liveVersion"), ep.get("targetVersion"))
            if state != last:
                log.info("%s: endpoint %s %s live=%s target=%s after %.0fs", rt.key, name, *state, time.time() - t_start)
                last = state
            if (ep.get("status") == "READY" and ep.get("liveVersion") == version) or ep.get("status", "").endswith("FAILED"):
                break
            if time.monotonic() > deadline:
                break
            time.sleep(5)
        result = {"action": action or f"endpoint_wait:{name}", "rt": rt.key, "endpoint": name, "version": version,
                  "status": ep.get("status"), "live_version": ep.get("liveVersion"),
                  "target_version": ep.get("targetVersion"), "failure_reason": ep.get("failureReason"),
                  "seconds": round(time.time() - t_start, 1), "t_start": t_start, "t_done": time.time()}
        if record:
            self.record("op_done", **result)
        if ep.get("status") != "READY" or ep.get("liveVersion") != version:
            raise RuntimeError(f"{rt.key}: endpoint {name} did not serve version {version}: {result}")
        return result

    def delete(self, runtime_id: str, timeout_s: int = 900) -> None:
        """Delete the runtime's custom endpoints, then the runtime (retrying while it is still settling)."""
        deadline = time.monotonic() + timeout_s
        try:
            while True:
                eps = [ep for ep in self.ctl.list_agent_runtime_endpoints(agentRuntimeId=runtime_id).get("runtimeEndpoints", [])
                       if ep["name"] != "DEFAULT"]
                if not eps or time.monotonic() > deadline:
                    break
                for ep in eps:
                    if ep.get("status") != "DELETING":
                        try:
                            self.ctl.delete_agent_runtime_endpoint(agentRuntimeId=runtime_id, endpointName=ep["name"])
                        except ClientError as exc:
                            if code(exc) not in ("ConflictException", "ResourceNotFoundException"):
                                raise
                time.sleep(5)
            while True:
                try:
                    self.ctl.delete_agent_runtime(agentRuntimeId=runtime_id)
                    break
                except ClientError as exc:
                    if code(exc) == "ConflictException" and time.monotonic() < deadline:
                        time.sleep(10)
                        continue
                    raise
        except ClientError as exc:
            if code(exc) != "ResourceNotFoundException":
                raise
            return
        log.info("Deleting runtime %s", runtime_id)

    def wait_deleted(self, runtime_id: str, timeout_s: int = 900) -> bool:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            try:
                self.ctl.get_agent_runtime(agentRuntimeId=runtime_id)
            except ClientError as exc:
                if code(exc) == "ResourceNotFoundException":
                    return True
                raise
            time.sleep(10)
        return False

    # ------------------------------------------------------------ data plane
    def invoke(self, rt: Runtime, qualifier: str, session_id: str, payload: dict, step: str, idx: int) -> dict:
        t_send = time.time()
        t0 = time.perf_counter()
        sample = {"rt": rt.key, "platform": rt.platform, "network": rt.network, "qualifier": qualifier,
                  "session": session_id, "step": step, "idx": idx, "op": payload.get("op", "probe"),
                  "client_send_wall": t_send}
        try:
            resp = self.dp.invoke_agent_runtime(agentRuntimeArn=rt.arn, qualifier=qualifier, runtimeSessionId=session_id,
                                                payload=json.dumps(payload).encode(), contentType="application/json",
                                                accept="application/json")
            body = resp["response"].read()
            sample["ms"] = round((time.perf_counter() - t0) * 1000, 1)
            sample["status"] = resp.get("statusCode")
            sample["data"] = json.loads(body)
            sample["ok"] = True
        except Exception as exc:  # noqa: BLE001 - a failed invocation is a result to report
            sample["ms"] = round((time.perf_counter() - t0) * 1000, 1)
            sample["ok"] = False
            sample["error"] = (f"{code(exc)}: {message(exc)}" if isinstance(exc, ClientError)
                               else f"{type(exc).__name__}: {exc}")[:500]
            log.warning("%s/%s %s op=%s failed after %.0f ms: %s", rt.key, qualifier, step, sample["op"],
                        sample["ms"], sample["error"])
        sample["client_recv_wall"] = time.time()
        self.record("invoke", **sample)
        return sample

    def stop_session(self, rt: Runtime, qualifier: str, session_id: str) -> None:
        try:
            self.dp_retry.stop_runtime_session(agentRuntimeArn=rt.arn, qualifier=qualifier, runtimeSessionId=session_id)
        except ClientError as exc:
            # Already gone (idle timeout, failed start) is fine: the point is not to leave sessions running.
            self.record("stop_failed", rt=rt.key, session=session_id, error=f"{code(exc)}: {message(exc)}")

    def session(self, rt: Runtime, step: str, ops: tuple[str, ...] = ("probe",), qualifier: str = "DEFAULT",
                stop: bool = True, session_id: str | None = None) -> list[dict]:
        """One runtime session: invoke each op in order on the same session id, then stop it."""
        sid = session_id or f"poc-{rt.key.replace('_', '-')}-{uuid.uuid4().hex}"
        out = []
        for i, op in enumerate(ops):
            payload = {"op": op} | ({"region": self.region} if op == "egress" else {})
            out.append(self.invoke(rt, qualifier, sid, payload, step, i))
            if not out[-1]["ok"] and i == 0:
                break
        if stop:
            self.stop_session(rt, qualifier, sid)
        return out
