#!/usr/bin/env bash
# The one gate. CI runs exactly this. Exits nonzero on the first failure.
#
#   ./check.sh                 everything (about 20 min), Docker required
#   HOLDFAST_PROPERTY_RUNS=200 ./check.sh    fewer property-test cases
set -euo pipefail
cd "$(dirname "$0")"

GOLANG_IMAGE="golang:1.27.3-trixie@sha256:c65ecfe613b539ac74c62a4c3b2c27e8f8a6076384c8f27e8a3c0f3be17cc385"
DDB_IMAGE="amazon/dynamodb-local:3.3.2@sha256:6ada631ec7aa5fc83446855eb17233e3dad184fec47477e8140c6c569cfe0c86"
TF_IMAGE="hashicorp/terraform:1.17.0@sha256:4d1b496a256f2989bccdb04b9104a4ede97234b10fba90259e173e905ec4fb58"
TRIVY_IMAGE="aquasec/trivy:0.76.1@sha256:790fd66d1697395d8693d1acaa1ea702406a20f109b6fc7bac8db623eca6c06c"
export HOLDFAST_PROPERTY_RUNS="${HOLDFAST_PROPERTY_RUNS:-2000}"
export HOLDFAST_SIM_RUNS="${HOLDFAST_SIM_RUNS:-500}"
FUZZ_TIME="${HOLDFAST_FUZZ_TIME:-20s}"
ADSB_FUZZ_TIME="${HOLDFAST_ADSB_FUZZ_TIME:-10s}"
# Pinned like everything else; both were at least three weeks old when chosen.
STATICCHECK="honnef.co/go/tools/cmd/staticcheck@v0.8.2"
GOVULNCHECK="golang.org/x/vuln/cmd/govulncheck@v1.9.0"
PY="${PYTHON:-python3}"
command -v "$PY" >/dev/null 2>&1 || PY=python
export PY
HERE="$(pwd -W 2>/dev/null || pwd)" # what docker -v wants on Windows and Linux alike

step() { printf '\n== %s\n' "$*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

step "line endings"
if git ls-files --eol | grep -q 'i/crlf'; then
  git ls-files --eol | grep 'i/crlf' >&2
  fail "CRLF in the index; scripts with CRLF break inside the containers"
fi

step "gofmt (both modules)"
unformatted="$(gofmt -l .)"
[[ -z "$unformatted" ]] || fail "gofmt needed: $unformatted"

step "go vet, staticcheck and govulncheck (needs the network for the vulnerability database)"
for mod in . cloud; do
  (cd "$mod" && go vet ./... && go run "$STATICCHECK" ./... && go run "$GOVULNCHECK" ./...)
done

docker info >/dev/null 2>&1 || fail "docker is not running"
# DynamoDB Local for the cloud peer's tests and mutants: on a network the
# race container can join, and on a port the host can reach.
cleanup() {
  docker rm -f hf-dynamodb >/dev/null 2>&1 || true
  docker network rm hf-check-net >/dev/null 2>&1 || true
}
trap cleanup EXIT
cleanup
docker network create hf-check-net >/dev/null
docker run -d --name hf-dynamodb --network hf-check-net -p 127.0.0.1:8000:8000 "$DDB_IMAGE" \
  -jar DynamoDBLocal.jar -inMemory >/dev/null
export HOLDFAST_DYNAMODB=http://127.0.0.1:8000 HOLDFAST_REQUIRE_DYNAMODB=1

step "go test -race, $HOLDFAST_PROPERTY_RUNS property cases, $HOLDFAST_SIM_RUNS simulated schedules per clock, cloud store on DynamoDB Local"
if [[ "$(uname -s)" == Linux ]]; then
  go test -race -count=1 ./...
  (cd cloud && go test -race -count=1 ./...)
else
  # The race detector needs cgo and a C compiler, so off Linux it runs in the
  # same image the node is built in.
  MSYS_NO_PATHCONV=1 docker run --rm -v "$HERE:/src" -w /src \
    -v holdfast-gocache:/root/.cache/go-build -v holdfast-gomod:/go/pkg/mod \
    -e HOLDFAST_PROPERTY_RUNS -e HOLDFAST_SIM_RUNS "$GOLANG_IMAGE" go test -race -count=1 ./...
  MSYS_NO_PATHCONV=1 docker run --rm --network hf-check-net -v "$HERE:/src" -w /src/cloud \
    -v holdfast-gocache:/root/.cache/go-build -v holdfast-gomod:/go/pkg/mod \
    -e HOLDFAST_DYNAMODB=http://hf-dynamodb:8000 -e HOLDFAST_REQUIRE_DYNAMODB=1 \
    "$GOLANG_IMAGE" go test -race -count=1 ./...
fi

step "fuzz the trust boundary for $FUZZ_TIME, and the ADS-B parser for $ADSB_FUZZ_TIME"
go test -run '^$' -fuzz '^FuzzHandlers$' -fuzztime "$FUZZ_TIME" ./internal/node
go test -run '^$' -fuzz '^FuzzParse$' -fuzztime "$ADSB_FUZZ_TIME" ./internal/adsb

step "mutants"
scripts/mutants.sh

step "cloud: the linux/arm64 Lambda binary Terraform deploys builds"
(cd cloud && GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go build -tags lambda.norpc -trimpath -o build/bootstrap .)

step "cloud: the binary runs in AWS's Lambda runtime image and round-trips through DynamoDB"
"$PY" scripts/lambda_smoke.py

step "terraform: fmt, validate, and the plan tests with a mocked provider"
tf() {
  MSYS_NO_PATHCONV=1 docker run --rm -v "$HERE/cloud/terraform:/tf" -w /tf \
    -v holdfast-tfcache:/plugins -e TF_PLUGIN_CACHE_DIR=/plugins "$TF_IMAGE" "$@"
}
tf init -backend=false -input=false >/dev/null
tf fmt -check -recursive
tf validate -no-color
tf test -no-color

step "terraform: Trivy misconfiguration scan"
MSYS_NO_PATHCONV=1 docker run --rm -v "$HERE/cloud/terraform:/tf:ro" -v holdfast-trivycache:/root/.cache/trivy \
  "$TRIVY_IMAGE" config --exit-code 1 --skip-version-check --quiet /tf

step "harness unit tests"
"$PY" -m unittest discover -s harness

step "harness smoke: one clean run, one partition that must heal"
"$PY" harness/run.py --smoke

printf '\ncheck: all passed\n'
