#!/usr/bin/env bash
# Plants known bugs in a scratch copy of the repo and checks that the tests
# catch every one. A test suite that stays green under a planted bug is not
# testing that behaviour.
#
# Each mutant: description | file | perl substitution applied to that file.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
PY="${PY:-${PYTHON:-python3}}"  # check.sh exports PY
# Every mutant was checked to die at the simulation's default of 200
# schedules; the gate's larger run is for finding new bugs, not for these.
export HOLDFAST_SIM_RUNS=200
command -v "$PY" >/dev/null 2>&1 || PY=python
mutants=(
  "Put keeps the smaller track|internal/crdt/track.go|s/if ok && Compare\(cur, t\) >= 0 \{/if ok \&\& Compare(cur, t) <= 0 {/"
  "Compare drops the content tie-break|internal/crdt/track.go|s/return cmp\.Or\(/return 0 * cmp.Or(/"
  "Diff ignores a stamp tie with different contents|internal/crdt/track.go|s/case mv\.Sum != theirs\.Sum:/case false:/"
  "Diff never asks for aircraft only the peer has|internal/crdt/track.go|s/want = append\(want, id\)\n\t\t\}\n\t\}\n\tslices/_ = id\n\t\t}\n\t}\n\tslices/"
  "HLC Update forgets the +1 when the remote is ahead|internal/hlc/hlc.go|s/Logical: m\.Logical \+ 1\}/Logical: m.Logical}/"
  "HLC lets the logical part pass the bound|internal/hlc/hlc.go|s/if c\.last\.Logical > MaxLogical \{/if false {/"
  "HLC Now reuses a stamp in the same millisecond|internal/hlc/hlc.go|s/\} else \{\n\t\tc\.last\.Logical\+\+\n\t\}\n\tc\.rollOver\(\)/}\n\tc.rollOver()/"
  "node merges before advancing its clock|internal/node/node.go|s/n\.clock\.Update\(high\)/_ = high/"
  "node counts a cut-off body as a rejection|internal/node/node.go|s/if body\.err != nil \{/if false {/"
  "node never holds its reports after a start|internal/node/node.go|s/if cfg\.StartupHold > 0 && !cfg\.Wall \{/if false {/"
  "node drops what it held when the hold ends|internal/node/node.go|s/n\.stamp\(id, held\[id\]\)/_ = id/"
  "node keeps the first held report, not the latest|internal/node/node.go|s/obs\.ObservedAtMS >= prev\.ObservedAtMS/obs.ObservedAtMS < prev.ObservedAtMS/"
  "node advances its clock before validating|internal/node/node.go|s/\tif err := n\.validate\(msgTS, tracks\); err != nil \{/\tn.clock.Update(msgTS)\n\tif err := n.validate(msgTS, tracks); err != nil {/"
  "expiry cutoff follows the clock back|internal/node/node.go|s/if c <= old \|\| n\.cut\.CompareAndSwap\(old, c\) \{\n\t\t\treturn max\(c, old\)/if c <= old || true {\n\t\t\treturn c/"
  "node accepts a track it already expired|internal/node/node.go|s/\t\tif t\.TS\.WallMS < cut \{\n\t\t\tn\.c\.mergeExpired\.Add\(1\)/\t\tif t.TS.WallMS < cut && false {\n\t\t\tn.c.mergeExpired.Add(1)/"
  "node never expires anything|internal/node/node.go|s/\tfor _, id := range n\.tracks\.Expire\(n\.cutoff\(\)\) \{/\tfor _, id := range n.tracks.Expire(min(n.cutoff(), math.MinInt64)) {/"
  "Expire drops tracks at the cutoff too|internal/crdt/track.go|s/if t\.TS\.WallMS < cutoffMS \{/if t.TS.WallMS <= cutoffMS {/"
  "receive reads its expiry cutoff before the lock|internal/node/node.go|s/\tcut := n\.cutoff\(\)\n\tn\.clock\.Update\(high\)/\tn.clock.Update(high)/;s/\tn\.mu\.Lock\(\)\n\tdefer n\.mu\.Unlock\(\)\n\t\/\/ The cutoff is read under the lock/\tcut := n.cutoff()\n\tn.mu.Lock()\n\tdefer n.mu.Unlock()\n\t\/\/ The cutoff is read under the lock/"
  "Restore keeps tracks that already expired|internal/node/node.go|s/\tmaps\.DeleteFunc\(keep, func\(_ crdt\.ICAO, t crdt\.Track\) bool \{ return t\.TS\.WallMS < cut \}\)/\t_ = cut/"
  "a node with no peers never expires|internal/node/node.go|s/\t\t\tn\.mu\.Unlock\(\)\n\t\t\tn\.expire\(\)\n/\t\t\tn.mu.Unlock()\n/"
  "node accepts stamps from the far future|internal/node/node.go|s/if ts\.WallMS > limit \{/if false {/"
  "node accepts a logical part that wraps the clock|internal/node/node.go|s/if ts\.Logical > hlc\.MaxLogical \{/if false {/"
  "node has no per-message track cap|internal/node/node.go|s/if len\(tracks\) > maxEntries \{/if false {/"
  "node advances its clock only to the message stamp|internal/node/node.go|s/\t\t\thigh = t\.TS\n/\t\t\tcontinue\n/"
  "Ingest stamps without the lock|internal/node/node.go|s/\tn\.mu\.Lock\(\)\n\tdefer n\.mu\.Unlock\(\)\n\tn\.c\.ingestTotal\.Add\(1\)/\tn.c.ingestTotal.Add(1)/"
  "ingest_regress is never counted|internal/node/node.go|s/\t\tn\.c\.ingestRegress\.Add\(1\)\n//"
  "merge_regress is never counted|internal/node/node.go|s/\t\t\tn\.c\.mergeRegress\.Add\(1\)\n//"
  "Ord drops the origin terminator|internal/crdt/track.go|s/\tb = append\(b, ' '\) \/\/ below/\t\/\/ below/"
  "Ord treats negative numbers as positive|internal/crdt/track.go|s/\t\treturn \^u\n/\t\treturn u\n/"
  "Restore does not advance the clock|internal/node/node.go|s/\tn\.clock\.Update\(high\)\n\tn\.tracks\.MergeAll\(keep\)/\t_ = high\n\tn.tracks.MergeAll(keep)/"
  "Restore loads invalid tracks|internal/node/node.go|s/if checkTrack\(id, t, limit\) != nil \{/if false \&\& checkTrack(id, t, limit) != nil {/"
  "checksum ignores speed and heading|internal/crdt/track.go|s/o\.AltFt, o\.GSKt, o\.HeadingDeg\}/o.AltFt}/"
  "cloud: the write condition replaces any different track|cloud/peer.go|s/ord < :ord/ord <> :ord/"
  "cloud: a lost condition counts as an error|cloud/peer.go|s/if errors\.As\(err, &lost\) \{\n\t\treturn false, nil\n\t\}/if false \&\& errors.As(err, \&lost) {\n\t\treturn false, nil\n\t}/"
  "cloud: changed tracks are never written|cloud/peer.go|s/won, err := p\.save\(ctx, changed, replace\)/won, err := p.save(ctx, nil, replace)/"
  "cloud: the token is not checked|cloud/peer.go|s/if p\.token != \"\" && subtle/if false \&\& subtle/"
  "cloud: only the first page is read|cloud/peer.go|s/for pages := dynamodb\.NewScanPaginator\(p\.db, in\); pages\.HasMorePages\(\); \{/for pages, first := dynamodb.NewScanPaginator(p.db, in), true; pages.HasMorePages() \&\& first; first = false {/"
  "cloud: a stored ord is trusted without checking|cloud/peer.go|s/if attr\(item, \"ord\"\) != crdt\.Ord\(t\) \{/if false \&\& attr(item, \"ord\") != crdt.Ord(t) {/"
  "cloud: a bad item can't be replaced|cloud/peer.go|s/\tswitch \{\n\tcase bad && badOrd != \"\":/\tswitch {\n\tcase false \&\& bad \&\& badOrd != \"\":/"
  "edge: the cloud token goes to every peer|cmd/holdfast/main.go|s/if r\.URL\.Scheme != b\.scheme \|\| r\.URL\.Host != b\.host \{/if false {/"
  "edge: the cloud token may cross the network in the clear|cmd/holdfast/main.go|s/ \&\& \(u\.Scheme != \"http\" \|\| !private\)//"
  "harness: merge-order catch-up waits only on the lowest value|harness/run.py|s/due\[ac\] = max\(due\.get\(ac, merge_key\(tr\)\), merge_key\(tr\)\)/due[ac] = min(due.get(ac, merge_key(tr)), merge_key(tr))/"
  "harness: live propagation counts from the observation|harness/run.py|s/return receipts_polled\(run\.polls\)/return {}, []/"
  "harness: the sweep's own results mark the checkout dirty|harness/run.py|s/, \"--\", \"\.\", \":\(exclude\)results\"//"
  "adsb: backoff can undercut the interval|internal/adsb/adsb.go|s/return max\(min\(2\*last, maxBackoff\), every, retryAfter\)/return min(max(2*last, retryAfter), maxBackoff)/"
  "harness: full recovery ignores its floor|harness/run.py|s/if v > worst_floor:\n            start = None/if False:\n            start = None/"
  "harness: full recovery accepts a single dip|harness/run.py|s/if t - start >= SUSTAIN_MS:/if t - start >= 0:/"
  "harness: full recovery floor is the best pre-cut moment|harness/run.py|s/worst_floor = max\(before\)/worst_floor = min(before)/"
  "harness: p95 recovery ignores its floor|harness/run.py|s/if t >= heal and v <= p95_floor:/if t >= heal:/"
  "harness: p95 recovery floor is the best pre-cut moment|harness/run.py|s/p95_floor = max\(before\)/p95_floor = min(before)/"
  "harness: raw files drop the events|harness/run.py|s/doc\[\"polls\"\] = \[\[p\.t, p\.states\] for p in run\.polls\]/doc[\"polls\"] = [[p.t, p.states] for p in run.polls]; doc[\"events\"] = {}/"
  "harness: lag is always zero|harness/run.py|s/lags\.append\(max\(0, fresh - have\)\)/lags.append(0)/"
  "harness: behind is never counted|harness/run.py|s/behind \+= sum\(1 for x in lags if x > BEHIND_MS\)/behind += 0/"
  "harness: propagation counts a node's own reports|harness/run.py|s/ and r not in made\.get\(\(n, ac\), set\(\)\)//"
  "harness: merges during the cut count from the first poll|harness/run.py|s/inside = \[p for p in run\.polls if window\[0\] <= p\.t < window\[1\]\] if window else \[\]/inside = run.polls/"
  "harness: worst lag keeps the best poll|harness/run.py|s/else max\(lag_worst, worst\)/else min(lag_worst, worst)/"
  "harness: undelivered reports are dropped silently|harness/run.py|s/return out, pairs, sum\(len\(q\) for q in pending\.values\(\)\)/return out, pairs, 0/"
  "harness: staleness counts aircraft long gone quiet|harness/run.py|s/if ingesting and now - fresh <= ACTIVE_MS:/if ingesting:/"
  "harness: divergence treats a missing aircraft as agreement|harness/run.py|s/len\(\{value\(t, ac\) for t in held\}\) > 1/len({value(t, ac) for t in held} - {None}) > 1/"
  "harness: catch-up ignores what was reported by the heal|harness/run.py|s/\.get\(\"at\", 0\) >= at$/.get(\"at\", 0) >= 0/m"
  "harness: bytes per useful update divides by every report|harness/run.py|s/useful = round\(sent \/ totals\[\"merge_applied\"\], 1\)/useful = round(sent \/ ingest, 1)/"
  "harness: propagation ignores the first receipt|harness/run.py|s/out\.append\(now - q\.pop\(0\)\[1\]\)/out.append(now - q.pop(0)[0])/"
  "harness: receipts keep the last receipt, not the first|harness/run.py|s/first\[key\] = min\(first\.get\(key, got\), got\)/first[key] = got/"
  "harness: settle ignores unequal hashes|harness/run.py|s/and len\(\{p\.states\[n\]\[\"hash\"\] for n in run\.nodes\}\) == 1/and True/"
)

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

# The cloud peer's tests need DynamoDB Local (check.sh starts one).
if [[ -z "${HOLDFAST_DYNAMODB:-}" ]]; then
  echo "mutants: HOLDFAST_DYNAMODB is not set; the cloud mutants need DynamoDB Local (check.sh starts one)"
  exit 1
fi
export HOLDFAST_REQUIRE_DYNAMODB=1

copy() {
  rm -rf "$work/src"
  mkdir -p "$work/src"
  (cd "$root" && tar -cf - --exclude=cloud/terraform --exclude=cloud/build \
    go.mod cmd internal cloud harness/run.py harness/test_run.py) | (cd "$work/src" && tar -xf -)
}

# Every suite has to pass unmutated first. Otherwise a broken environment
# (DynamoDB down, say) would fail every mutant's tests and read as kills.
copy
if ! (cd "$work/src" && go test -vet=off -count=1 ./internal/... ./cmd/... >/dev/null 2>&1 \
      && "$PY" -m unittest discover -s harness >/dev/null 2>&1 \
      && (cd cloud && go test -vet=off -count=1 ./... >/dev/null 2>&1)); then
  echo "mutants: the unmutated tests fail, so no kill would mean anything; fix that first"
  exit 1
fi

failed=0
for m in "${mutants[@]}"; do
  IFS='|' read -r desc file sub <<<"$m"
  copy
  before="$(sha256sum "$work/src/$file" | cut -d' ' -f1)"
  perl -0pi -e "$sub" "$work/src/$file"
  after="$(sha256sum "$work/src/$file" | cut -d' ' -f1)"
  if [[ "$before" == "$after" ]]; then
    echo "MUTANT DID NOT APPLY: $desc ($file). Fix the substitution; a mutant that changes nothing proves nothing."
    failed=1
    continue
  fi
  # Compile first, and keep vet out of the Go test run, so "killed" can only
  # mean an assertion failed.
  if [[ "$file" == *.py ]]; then
    build=("$PY" -m py_compile "$file")
    test=("$PY" -m unittest discover -s harness)
  elif [[ "$file" == cmd/* ]]; then
    build=(go test -vet=off -count=1 -run '^$' ./cmd/...)
    test=(go test -vet=off -count=1 ./cmd/...)
  elif [[ "$file" == cloud/* ]]; then
    build=(bash -c 'cd cloud && go test -vet=off -count=1 -run "^$" ./...')
    test=(bash -c 'cd cloud && go test -vet=off -count=1 ./...')
  else
    build=(go test -vet=off -count=1 -run '^$' ./internal/...)
    test=(go test -vet=off -count=1 ./internal/...)
  fi
  if ! (cd "$work/src" && "${build[@]}" >/dev/null 2>&1); then
    echo "MUTANT DID NOT BUILD: $desc. A compile error is not a test catching the bug."
    failed=1
    continue
  fi
  if (cd "$work/src" && "${test[@]}" >/dev/null 2>&1); then
    echo "SURVIVED: $desc"
    failed=1
  else
    echo "killed:   $desc"
  fi
done

if [[ $failed -ne 0 ]]; then
  echo "mutants: FAIL"
  exit 1
fi
echo "mutants: all ${#mutants[@]} killed"
