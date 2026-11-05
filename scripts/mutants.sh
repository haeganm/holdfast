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
  "node accepts stamps from the far future|internal/node/node.go|s/if ts\.WallMS > limit \{/if false {/"
  "node accepts a logical part that wraps the clock|internal/node/node.go|s/if ts\.Logical > hlc\.MaxLogical \{/if false {/"
  "node has no per-message track cap|internal/node/node.go|s/if len\(tracks\) > maxEntries \{/if false {/"
  "node advances its clock only to the message stamp|internal/node/node.go|s/\t\t\thigh = t\.TS\n/\t\t\tcontinue\n/"
  "Ingest stamps without the lock|internal/node/node.go|s/\tn\.mu\.Lock\(\)\n\tdefer n\.mu\.Unlock\(\)\n\tn\.c\.ingestTotal\.Add\(1\)/\tn.c.ingestTotal.Add(1)/"
  "merge_regress is never counted|internal/node/node.go|s/\t\t\tn\.c\.mergeRegress\.Add\(1\)\n//"
  "checksum ignores speed and heading|internal/crdt/track.go|s/o\.AltFt, o\.GSKt, o\.HeadingDeg\}/o.AltFt}/"
  "harness: full recovery ignores its floor|harness/run.py|s/if v > worst_floor:\n            start = None/if False:\n            start = None/"
  "harness: full recovery accepts a single dip|harness/run.py|s/if t - start >= SUSTAIN_MS:/if t - start >= 0:/"
  "harness: full recovery floor is the best pre-cut moment|harness/run.py|s/worst_floor = max\(before\)/worst_floor = min(before)/"
  "harness: p95 recovery ignores its floor|harness/run.py|s/if t >= heal and v <= p95_floor:/if t >= heal:/"
  "harness: p95 recovery floor is the best pre-cut moment|harness/run.py|s/p95_floor = max\(before\)/p95_floor = min(before)/"
  "harness: raw files drop the events|harness/run.py|s/doc\[\"polls\"\] = \[\[p\.t, p\.states\] for p in run\.polls\]/doc[\"polls\"] = [[p.t, p.states] for p in run.polls]; doc[\"events\"] = {}/"
  "harness: lag is always zero|harness/run.py|s/lags\.append\(max\(0, fresh - have\)\)/lags.append(0)/"
  "harness: behind is never counted|harness/run.py|s/behind \+= sum\(1 for x in lags if x > BEHIND_MS\)/behind += 0/"
  "harness: propagation counts a node's own reports|harness/run.py|s/ and r not in made\.get\(\(n, ac\), set\(\)\)//"
  "harness: merges during the cut count from the first poll|harness/run.py|s/inside = \[p for p in run\.polls if lo <= p\.t < heal\]/inside = run.polls/"
  "harness: worst lag keeps the best poll|harness/run.py|s/else max\(lag_worst, worst\)/else min(lag_worst, worst)/"
  "harness: undelivered reports are dropped silently|harness/run.py|s/return out, pairs, sum\(len\(q\) for q in pending\.values\(\)\)/return out, pairs, 0/"
  "harness: settle ignores unequal hashes|harness/run.py|s/and len\(\{p\.states\[n\]\[\"hash\"\] for n in run\.nodes\}\) == 1/and True/"
)

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

failed=0
for m in "${mutants[@]}"; do
  IFS='|' read -r desc file sub <<<"$m"
  rm -rf "$work/src"
  mkdir -p "$work/src"
  (cd "$root" && tar -cf - go.mod cmd internal harness/run.py harness/test_run.py) | (cd "$work/src" && tar -xf -)
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
