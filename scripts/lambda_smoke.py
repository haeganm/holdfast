"""Run the cloud peer's real binary inside AWS's Lambda runtime image and
send it API Gateway HTTP API events, against DynamoDB Local.

    python -I scripts/lambda_smoke.py

The image ships the Runtime Interface Emulator, so this exercises what the
unit tests can't: the binary starts as a provided.al2023 bootstrap, speaks
the Lambda runtime API, and handles payload 2.0 events end to end. It builds
a linux/amd64 binary and runs it in the image's linux/amd64 variant, so an
x86-64 runner needs no CPU emulation. Terraform deploys the linux/arm64
build, which check.sh only cross-compiles: nothing here runs that one.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LAMBDA_IMAGE = "public.ecr.aws/lambda/provided:al2023.2026.10.19.23@sha256:49c8b85e353ea6052759c3dcf6761b35c055c29c8016a9cd734ac4d24a060729"
DDB_IMAGE = "amazon/dynamodb-local:3.3.2@sha256:6ada631ec7aa5fc83446855eb17233e3dad184fec47477e8140c6c569cfe0c86"
NET, DDB, FN = "hf-lambda-net", "hf-lambda-ddb", "hf-lambda"
DDB_PORT, FN_PORT = 18001, 19009
TABLE, TOKEN = "holdfast-smoke", "smoke-token-0123456789abcdef0123456789"


def sh(*args: str, check: bool = True) -> str:
    p = subprocess.run(args, capture_output=True, text=True)
    if check and p.returncode != 0:
        raise SystemExit(f"{' '.join(args)} exited {p.returncode}: {p.stderr.strip() or p.stdout.strip()}")
    return p.stdout


def cleanup() -> None:
    sh("docker", "rm", "-f", FN, DDB, check=False)
    sh("docker", "network", "rm", NET, check=False)


def post(url: str, body: dict, headers: dict | None = None) -> dict:
    req = urllib.request.Request(url, data=json.dumps(body).encode(), method="POST",
                                 headers={"Content-Type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=15) as r:
        return json.load(r)


def create_table() -> None:
    # DynamoDB Local doesn't verify signatures, so a plain request with a
    # well-formed header creates the table without any AWS tooling.
    auth = "AWS4-HMAC-SHA256 Credential=local/20260101/us-east-1/dynamodb/aws4_request, SignedHeaders=host, Signature=0"
    body = {"TableName": TABLE, "BillingMode": "PAY_PER_REQUEST",
            "AttributeDefinitions": [{"AttributeName": "icao", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "icao", "KeyType": "HASH"}]}
    deadline = time.time() + 30
    while True:
        try:
            req = urllib.request.Request(f"http://127.0.0.1:{DDB_PORT}", data=json.dumps(body).encode(), method="POST",
                                         headers={"Content-Type": "application/x-amz-json-1.0", "Authorization": auth,
                                                  "X-Amz-Target": "DynamoDB_20120810.CreateTable"})
            urllib.request.urlopen(req, timeout=5).read()
            return
        except (urllib.error.URLError, ConnectionError):
            if time.time() > deadline:
                raise SystemExit("DynamoDB Local never came up")
            time.sleep(0.5)


def event(method: str, path: str, body: dict | None = None, token: str | None = TOKEN) -> dict:
    headers = {"content-type": "application/json"}
    if token is not None:
        headers["authorization"] = f"Bearer {token}"
    return {
        "version": "2.0", "routeKey": "$default", "rawPath": path, "rawQueryString": "",
        "headers": headers,
        "requestContext": {"http": {"method": method, "path": path, "sourceIp": "127.0.0.1"}},
        "body": json.dumps(body) if body is not None else "", "isBase64Encoded": False,
    }


def invoke(ev: dict) -> dict:
    url = f"http://127.0.0.1:{FN_PORT}/2015-03-31/functions/function/invocations"
    deadline = time.time() + 30
    while True:
        try:
            return post(url, ev)
        except (urllib.error.URLError, ConnectionError):
            if time.time() > deadline:
                raise SystemExit("the Lambda emulator never answered:\n" + sh("docker", "logs", FN, check=False))
            time.sleep(0.5)


def main() -> int:
    cleanup()
    build = ROOT / "cloud" / "build"
    build.mkdir(exist_ok=True)
    env = os.environ | {"GOOS": "linux", "GOARCH": "amd64", "CGO_ENABLED": "0"}
    p = subprocess.run(["go", "build", "-tags", "lambda.norpc", "-trimpath", "-o", str(build / "bootstrap-amd64"), "."],
                       cwd=ROOT / "cloud", env=env, capture_output=True, text=True)
    if p.returncode != 0:
        raise SystemExit("build failed: " + p.stderr)
    try:
        sh("docker", "network", "create", NET)
        sh("docker", "run", "-d", "--name", DDB, "--network", NET, "-p", f"127.0.0.1:{DDB_PORT}:8000",
           DDB_IMAGE, "-jar", "DynamoDBLocal.jar", "-inMemory")
        create_table()
        sh("docker", "run", "-d", "--name", FN, "--platform", "linux/amd64", "--network", NET, "-p", f"127.0.0.1:{FN_PORT}:8080",
           "-v", f"{build / 'bootstrap-amd64'}:/var/runtime/bootstrap:ro",
           "-e", f"TABLE={TABLE}", "-e", f"PEER_TOKEN={TOKEN}", "-e", f"AWS_ENDPOINT_URL_DYNAMODB=http://{DDB}:8000",
           "-e", "AWS_REGION=us-east-1", "-e", "AWS_ACCESS_KEY_ID=local", "-e", "AWS_SECRET_ACCESS_KEY=local",
           LAMBDA_IMAGE, "bootstrap")

        now = int(time.time() * 1000)
        track = {"obs": {"lat": 39.0, "lon": -77.0, "alt": 31000, "gs": 420, "hdg": 87, "at": now},
                 "ts": {"w": now, "l": 0}, "origin": "a"}
        checks = []
        r = invoke(event("GET", "/hash", token="wrong-token-0123456789abcdef0123456789"))
        checks.append(("a wrong token is refused", r.get("statusCode") == 401, r))
        r = invoke(event("GET", "/hash", token=None))
        checks.append(("a request without a token is refused", r.get("statusCode") == 401, r))
        r = invoke(event("POST", "/push", {"from": "a", "hlc": {"w": now, "l": 0}, "tracks": {"ABCDEF": track}}))
        checks.append(("a push is stored", r.get("statusCode") == 204, r))
        r = invoke(event("GET", "/hash"))
        n = json.loads(r.get("body") or "{}").get("n")
        checks.append(("the next invocation reads it back from the table", r.get("statusCode") == 200 and n == 1, r))
        r = invoke(event("POST", "/sync", {"from": "b", "hlc": {"w": now, "l": 0}, "digest": {}}))
        body = json.loads(r.get("body") or "{}")
        checks.append(("a sync from an empty peer gets the track", r.get("statusCode") == 200 and "ABCDEF" in body.get("tracks", {}), r))

        failed = [name for name, ok, _ in checks if not ok]
        for name, ok, resp in checks:
            print(f"{'ok  ' if ok else 'FAIL'} {name}" + ("" if ok else f": {json.dumps(resp)[:300]}"))
        if failed:
            print(sh("docker", "logs", FN, check=False)[-2000:])
            return 1
        print("lambda smoke: the linux/amd64 binary runs in the Lambda runtime and round-trips through DynamoDB")
        return 0
    finally:
        cleanup()


if __name__ == "__main__":
    sys.exit(main())
