"""AgentCore Browser session lifecycle and the SigV4-signed CDP WebSocket handshake."""

from __future__ import annotations

import datetime as dt
import logging
from dataclasses import dataclass, field
from urllib.parse import urlsplit, urlunsplit

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.config import Config
from botocore.exceptions import ClientError

log = logging.getLogger("bench.agentcore")

SIGNING_SERVICE = "bedrock-agentcore"


@dataclass
class BrowserSession:
    browser_id: str
    session_id: str
    region: str
    automation_endpoint: str
    live_view_endpoint: str | None
    created_at: dt.datetime
    stopped_at: dt.datetime | None = None
    raw_start_response: dict = field(default_factory=dict, repr=False)


class AgentCoreBrowser:
    """Thin wrapper over the `bedrock-agentcore` data-plane browser APIs.

    `stop()` is idempotent so it can be called from several teardown paths
    (finally block, signal handler, atexit) without double-stopping.
    """

    def __init__(self, boto_session: boto3.Session, region: str, browser_id: str):
        self.boto_session = boto_session
        self.region = region
        self.browser_id = browser_id
        self.client = boto_session.client(
            "bedrock-agentcore",
            region_name=region,
            config=Config(retries={"max_attempts": 8, "mode": "adaptive"}),
        )
        self.session: BrowserSession | None = None

    def start(self, name: str, timeout_s: int, viewport: tuple[int, int] | None = None) -> BrowserSession:
        kwargs: dict = {
            "browserIdentifier": self.browser_id,
            "name": name,
            "sessionTimeoutSeconds": timeout_s,
        }
        if viewport:
            kwargs["viewPort"] = {"width": viewport[0], "height": viewport[1]}
        log.info("StartBrowserSession browser=%s name=%s timeout=%ss region=%s",
                 self.browser_id, name, timeout_s, self.region)
        resp = self.client.start_browser_session(**kwargs)
        resp.pop("ResponseMetadata", None)
        streams = resp.get("streams") or {}
        automation = streams.get("automationStream") or {}
        self.session = BrowserSession(
            browser_id=resp["browserIdentifier"],
            session_id=resp["sessionId"],
            region=self.region,
            automation_endpoint=automation.get("streamEndpoint", ""),
            live_view_endpoint=(streams.get("liveViewStream") or {}).get("streamEndpoint"),
            created_at=resp.get("createdAt") or dt.datetime.now(dt.timezone.utc),
            raw_start_response=resp,
        )
        log.info("Session started id=%s automationStream=%s (status=%s)",
                 self.session.session_id, self.session.automation_endpoint,
                 automation.get("streamStatus"))
        if not self.session.automation_endpoint:
            raise RuntimeError(f"StartBrowserSession returned no automation stream endpoint: {resp}")
        return self.session

    def get(self) -> dict:
        """GetBrowserSession; returns the response without ResponseMetadata."""
        assert self.session is not None
        resp = self.client.get_browser_session(
            browserIdentifier=self.session.browser_id, sessionId=self.session.session_id
        )
        resp.pop("ResponseMetadata", None)
        return resp

    def count_ready_sessions(self) -> int | None:
        """Number of READY sessions on this browser (used to flag shared-metric contamination)."""
        try:
            n, token = 0, None
            while True:
                kw = {"browserIdentifier": self.browser_id, "status": "READY", "maxResults": 100}
                if token:
                    kw["nextToken"] = token
                resp = self.client.list_browser_sessions(**kw)
                n += len(resp.get("items", []))
                token = resp.get("nextToken")
                if not token:
                    return n
        except ClientError as exc:
            log.warning("ListBrowserSessions failed: %s", exc)
            return None

    def stop(self) -> None:
        s = self.session
        if s is None or s.stopped_at is not None:
            return
        try:
            log.info("StopBrowserSession id=%s", s.session_id)
            self.client.stop_browser_session(browserIdentifier=s.browser_id, sessionId=s.session_id)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            # A session the service already terminated (e.g. after a VM kill) is not an error for us.
            if code in ("ResourceNotFoundException", "ValidationException", "ConflictException"):
                log.warning("StopBrowserSession: %s (session likely already terminated)", exc)
            else:
                raise
        finally:
            s.stopped_at = dt.datetime.now(dt.timezone.utc)

    def signed_ws_request(self) -> tuple[str, dict[str, str]]:
        """SigV4-sign the automation-stream WebSocket upgrade (GET on the https form of the URL)."""
        assert self.session is not None
        url = self.session.automation_endpoint
        parts = urlsplit(url)
        https_url = urlunsplit(("https", parts.netloc, parts.path, parts.query, ""))
        creds = self.boto_session.get_credentials()
        if creds is None:
            raise RuntimeError("No AWS credentials found for signing the CDP WebSocket")
        request = AWSRequest(method="GET", url=https_url, headers={"host": parts.netloc})
        SigV4Auth(creds.get_frozen_credentials(), SIGNING_SERVICE, self.region).add_auth(request)
        # Host, X-Amz-Date, Authorization and (for temporary creds) X-Amz-Security-Token.
        return url, dict(request.headers.items())
