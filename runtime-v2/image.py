"""Assemble the probe image and push it to Amazon ECR without Docker.

An image is a manifest that names one config blob and a list of layer blobs. This module takes the
stock python base image from ECR Public, adds two layers of its own and pushes the result with the
ECR layer-upload APIs, so the PoC needs neither Docker nor a build service:

  ballast layer  /app/ballast.bin, N MiB of deterministic incompressible bytes. Identical in every
                 build, so ECR stores it once. The agent times a full read of it.
  app layer      /app/agent.py and /app/build.json. build.json carries the build id, so two builds of
                 identical code still have different digests, exactly like two CI builds that push
                 the same tag.

`Image.push(tag)` points any tag at an image; pushing build B to the tag build A was created under is
how the same-tag test reproduces a pipeline that reuses its tag.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import logging
import tarfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import PurePosixPath

log = logging.getLogger("poc.image")

PUBLIC_REGISTRY = "public.ecr.aws"
DEFAULT_BASE = f"{PUBLIC_REGISTRY}/docker/library/python:3.12-slim"
INDEX_TYPES = ("application/vnd.oci.image.index.v1+json",
               "application/vnd.docker.distribution.manifest.list.v2+json")
# manifest media type -> (config media type, gzip layer media type) of the same format
FORMATS = {
    "application/vnd.oci.image.manifest.v1+json":
        ("application/vnd.oci.image.config.v1+json", "application/vnd.oci.image.layer.v1.tar+gzip"),
    "application/vnd.docker.distribution.manifest.v2+json":
        ("application/vnd.docker.container.image.v1+json", "application/vnd.docker.image.rootfs.diff.tar.gzip"),
}
ACCEPT = ", ".join((*INDEX_TYPES, *FORMATS))


def sha256(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


class PublicImage:
    """Anonymous, read-only access to one image on ECR Public."""

    def __init__(self, ref: str):
        host, _, rest = ref.partition("/")
        if host != PUBLIC_REGISTRY or ":" not in rest:
            raise ValueError(f"expected {PUBLIC_REGISTRY}/<repository>:<tag>, got {ref!r}")
        self.ref = ref
        self.repo, _, self.tag = rest.rpartition(":")
        url = f"https://{PUBLIC_REGISTRY}/token/?scope=repository:{self.repo}:pull&service={PUBLIC_REGISTRY}"
        with urllib.request.urlopen(url, timeout=30) as resp:
            self._token = json.load(resp)["token"]
        self._opener = urllib.request.build_opener(_NoRedirect)

    def _get(self, path: str, accept: str | None = None) -> tuple[str, bytes]:
        headers = {"Authorization": f"Bearer {self._token}", **({"Accept": accept} if accept else {})}
        req = urllib.request.Request(f"https://{PUBLIC_REGISTRY}/v2/{self.repo}/{path}", headers=headers)
        try:
            with self._opener.open(req, timeout=120) as resp:
                return resp.headers.get("Content-Type", ""), resp.read()
        except urllib.error.HTTPError as exc:
            if exc.code not in (301, 302, 303, 307, 308):
                raise
            location = exc.headers["Location"]
        # Blobs redirect to a pre-signed CDN URL, which rejects a second credential: no bearer token.
        with urllib.request.urlopen(location, timeout=600) as resp:
            return resp.headers.get("Content-Type", ""), resp.read()

    def manifest(self, os_: str = "linux", arch: str = "arm64") -> tuple[str, dict]:
        ctype, body = self._get(f"manifests/{self.tag}", ACCEPT)
        doc = json.loads(body)
        mtype = doc.get("mediaType") or ctype
        if mtype in INDEX_TYPES:
            entry = next((m for m in doc["manifests"]
                          if m.get("platform", {}).get("os") == os_ and m["platform"].get("architecture") == arch),
                         None)
            if entry is None:
                raise RuntimeError(f"{self.ref} has no {os_}/{arch} image")
            ctype, body = self._get(f"manifests/{entry['digest']}", ACCEPT)
            doc = json.loads(body)
            mtype = doc.get("mediaType") or ctype
        if mtype not in FORMATS:
            raise RuntimeError(f"{self.ref}: unsupported manifest type {mtype}")
        return mtype, doc

    def blob(self, digest: str) -> bytes:
        data = self._get(f"blobs/{digest}")[1]
        if sha256(data) != digest:
            raise RuntimeError(f"{self.ref}: blob {digest} failed digest verification")
        return data


class EcrRepository:
    """Blob and manifest pushes through the ECR API (the calls `docker push` makes for you)."""

    def __init__(self, ecr, name: str, uri: str):
        self.ecr, self.name, self.uri = ecr, name, uri

    def missing(self, digests: list[str]) -> set[str]:
        resp = self.ecr.batch_check_layer_availability(repositoryName=self.name, layerDigests=digests)
        have = {layer["layerDigest"] for layer in resp.get("layers", [])
                if layer.get("layerAvailability") == "AVAILABLE"}
        return set(digests) - have

    def upload(self, digest: str, data: bytes) -> None:
        up = self.ecr.initiate_layer_upload(repositoryName=self.name)
        part = min(int(up.get("partSize") or 10 << 20), 20 << 20)
        for first in range(0, len(data), part):
            chunk = data[first:first + part]
            self.ecr.upload_layer_part(repositoryName=self.name, uploadId=up["uploadId"], partFirstByte=first,
                                       partLastByte=first + len(chunk) - 1, layerPartBlob=chunk)
        try:
            self.ecr.complete_layer_upload(repositoryName=self.name, uploadId=up["uploadId"], layerDigests=[digest])
        except self.ecr.exceptions.LayerAlreadyExistsException:
            pass

    def put_manifest(self, manifest: bytes, media_type: str, tag: str) -> str:
        try:
            resp = self.ecr.put_image(repositoryName=self.name, imageManifest=manifest.decode(),
                                      imageManifestMediaType=media_type, imageTag=tag)
            return resp["image"]["imageId"]["imageDigest"]
        except self.ecr.exceptions.ImageAlreadyExistsException:
            return sha256(manifest)  # the tag already points at exactly this manifest

    def digest_of(self, tag: str) -> str | None:
        try:
            resp = self.ecr.describe_images(repositoryName=self.name, imageIds=[{"imageTag": tag}])
        except self.ecr.exceptions.ImageNotFoundException:
            return None
        return resp["imageDetails"][0]["imageDigest"]


@dataclass
class Image:
    repo: EcrRepository
    build_id: str
    digest: str
    manifest: bytes
    media_type: str

    def push(self, tag: str) -> str:
        """Point `tag` at this image (moving it if it pointed elsewhere); returns the image URI."""
        digest = self.repo.put_manifest(self.manifest, self.media_type, tag)
        if digest != self.digest:
            raise RuntimeError(f"ECR stored {digest} for build {self.build_id}, expected {self.digest}")
        log.info("Pushed build %s as %s:%s (%s)", self.build_id, self.repo.uri, tag, self.digest)
        return f"{self.repo.uri}:{tag}"


def tar_gz(files: dict[str, bytes], compresslevel: int = 6) -> tuple[bytes, str]:
    """A reproducible gzip'd tar (fixed owner, mode and mtime) and its diff_id (digest of the raw tar)."""
    buf = io.BytesIO()
    dirs = sorted({str(p) for path in files for p in PurePosixPath(path).parents if str(p) != "."})
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.PAX_FORMAT) as tar:
        for d in dirs:
            info = tarfile.TarInfo(d)
            info.type, info.mode, info.mtime = tarfile.DIRTYPE, 0o755, 0
            tar.addfile(info)
        for path in sorted(files):
            info = tarfile.TarInfo(path)
            info.size, info.mode, info.mtime = len(files[path]), 0o644, 0
            tar.addfile(info, io.BytesIO(files[path]))
    raw = buf.getvalue()
    return gzip.compress(raw, compresslevel=compresslevel, mtime=0), sha256(raw)


def ballast_bytes(mb: int) -> bytes:
    # Deterministic so every build shares the layer, incompressible so the image really carries it.
    return hashlib.shake_256(b"agentcore-v2-poc ballast").digest(mb << 20)


def build(repo: EcrRepository, build_id: str, agent_source: bytes, base_ref: str = DEFAULT_BASE,
          ballast_mb: int = 32) -> Image:
    """Assemble base + ballast + app, upload whatever blobs ECR does not already hold, and return the
    image (not yet tagged: call `push`)."""
    base = PublicImage(base_ref)
    mtype, base_manifest = base.manifest()
    config_type, layer_type = FORMATS[mtype]
    config = json.loads(base.blob(base_manifest["config"]["digest"]))

    build_info = {"build_id": build_id, "built_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                  "base": base_ref}
    ours = []  # (descriptor, blob, diff_id)
    if ballast_mb > 0:
        blob, diff_id = tar_gz({"app/ballast.bin": ballast_bytes(ballast_mb)}, compresslevel=1)
        ours.append(({"mediaType": layer_type, "digest": sha256(blob), "size": len(blob)}, blob, diff_id))
    blob, diff_id = tar_gz({"app/agent.py": agent_source,
                            "app/build.json": json.dumps(build_info, indent=1).encode()})
    ours.append(({"mediaType": layer_type, "digest": sha256(blob), "size": len(blob)}, blob, diff_id))

    cfg = config.setdefault("config", {})
    cfg["Env"] = [e for e in cfg.get("Env", []) if not e.startswith(("POC_IMAGE_BUILD=", "AGENT_HOST="))] + [
        f"POC_IMAGE_BUILD={build_id}", "AGENT_HOST=0.0.0.0", "PYTHONUNBUFFERED=1"]
    cfg.update(Cmd=["python", "/app/agent.py"], Entrypoint=None, WorkingDir="/app", ExposedPorts={"8080/tcp": {}})
    config["rootfs"]["diff_ids"] = config["rootfs"]["diff_ids"] + [d for _, _, d in ours]
    config["history"] = config.get("history", []) + [
        {"created": build_info["built_at"], "created_by": f"agentcore-v2-poc image.py ({name})"}
        for name in (["ballast"] if ballast_mb > 0 else []) + ["app"]]
    config_blob = json.dumps(config, separators=(",", ":")).encode()

    manifest = json.dumps({
        "schemaVersion": 2,
        "mediaType": mtype,
        "config": {"mediaType": config_type, "digest": sha256(config_blob), "size": len(config_blob)},
        "layers": [{k: layer[k] for k in ("mediaType", "digest", "size")} for layer in base_manifest["layers"]]
                  + [d for d, _, _ in ours],
    }, separators=(",", ":")).encode()

    local = {sha256(config_blob): config_blob, **{d["digest"]: b for d, b, _ in ours}}
    todo = repo.missing([layer["digest"] for layer in base_manifest["layers"]] + list(local))
    for digest in sorted(todo):
        data = local.get(digest) or base.blob(digest)
        log.info("Uploading blob %s (%.1f MB)", digest[:19], len(data) / 1e6)
        repo.upload(digest, data)
    log.info("Build %s assembled: %d blobs uploaded, %d already in ECR", build_id, len(todo),
             len(base_manifest["layers"]) + len(local) - len(todo))
    return Image(repo=repo, build_id=build_id, digest=sha256(manifest), manifest=manifest, media_type=mtype)
