#!/usr/bin/env bash
# The one gate. CI runs exactly this. Exits nonzero on the first failure.
#
#   ./check.sh                 everything, including a Docker smoke run (~2.5 min)
#   HOLDFAST_PROPERTY_RUNS=200 ./check.sh    fewer property-test cases
set -euo pipefail
cd "$(dirname "$0")"

GOLANG_IMAGE="golang:1.27.3-trixie@sha256:c65ecfe613b539ac74c62a4c3b2c27e8f8a6076384c8f27e8a3c0f3be17cc385"
export HOLDFAST_PROPERTY_RUNS="${HOLDFAST_PROPERTY_RUNS:-2000}"
PY="${PYTHON:-python3}"
command -v "$PY" >/dev/null 2>&1 || PY=python

step() { printf '\n== %s\n' "$*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

step "line endings"
if git ls-files --eol | grep -q 'i/crlf'; then
  git ls-files --eol | grep 'i/crlf' >&2
  fail "CRLF in the index; scripts with CRLF break inside the containers"
fi

step "gofmt"
unformatted="$(gofmt -l .)"
[[ -z "$unformatted" ]] || fail "gofmt needed: $unformatted"

step "go vet"
go vet ./...

step "go test -race, $HOLDFAST_PROPERTY_RUNS property cases"
if [[ "$(uname -s)" == Linux ]]; then
  go test -race -count=1 ./...
else
  # The race detector needs cgo and a C compiler, so off Linux it runs in the
  # same image the node is built in.
  docker info >/dev/null 2>&1 || fail "docker is not running (the race detector runs in a container off Linux)"
  MSYS_NO_PATHCONV=1 docker run --rm -v "$(pwd -W 2>/dev/null || pwd):/src" -w /src \
    -v holdfast-gocache:/root/.cache/go-build -e HOLDFAST_PROPERTY_RUNS \
    "$GOLANG_IMAGE" go test -race -count=1 ./...
fi

step "mutants"
scripts/mutants.sh

step "harness unit tests"
"$PY" -m unittest discover -s harness

step "harness smoke: one clean run, one partition that must heal"
docker info >/dev/null 2>&1 || fail "docker is not running"
"$PY" harness/run.py --smoke

printf '\ncheck: all passed\n'
