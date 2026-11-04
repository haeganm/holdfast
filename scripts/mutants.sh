#!/usr/bin/env bash
# Plants known bugs in a scratch copy of the repo and checks that the tests
# catch every one. A test suite that stays green under a planted bug is not
# testing that behaviour.
#
# Each mutant: description | file | perl substitution applied to that file.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
PY="${PYTHON:-python3}"
command -v "$PY" >/dev/null 2>&1 || PY=python
mutants=(
  "Put keeps the smaller track|internal/crdt/track.go|s/if ok && Compare\(cur, t\) >= 0 \{/if ok \&\& Compare(cur, t) <= 0 {/"
  "Compare drops the content tie-break|internal/crdt/track.go|s/return cmp\.Or\(/return 0 * cmp.Or(/"
  "Diff ignores a stamp tie with different contents|internal/crdt/track.go|s/case mv\.Sum != theirs\.Sum:/case false:/"
  "Diff never asks for aircraft only the peer has|internal/crdt/track.go|s/want = append\(want, id\)\n\t\t\}\n\t\}\n\tslices/_ = id\n\t\t}\n\t}\n\tslices/"
  "HLC Update forgets the +1 when the remote is ahead|internal/hlc/hlc.go|s/Logical: m\.Logical \+ 1\}/Logical: m.Logical}/"
  "HLC Now reuses a stamp in the same millisecond|internal/hlc/hlc.go|s/\} else \{\n\t\tc\.last\.Logical\+\+\n\t\}\n\treturn c\.last/}\n\treturn c.last/"
  "node merges before advancing its clock|internal/node/node.go|s/n\.clock\.Update\(high\)/_ = high/"
  "node accepts stamps from the far future|internal/node/node.go|s/if ts\.WallMS > limit \{/if false {/"
  "node accepts a logical part that wraps the clock|internal/node/node.go|s/if ts\.Logical > maxLogical \{/if false {/"
  "node has no per-message track cap|internal/node/node.go|s/if len\(tracks\) > maxEntries \{/if false {/"
  "node advances its clock only to the message stamp|internal/node/node.go|s/\t\t\thigh = t\.TS\n/\t\t\tcontinue\n/"
  "Ingest stamps before taking the lock|internal/node/node.go|s/n\.mu\.Lock\(\)\n\tdefer n\.mu\.Unlock\(\)\n\twon, prev := n\.tracks\.Put\(id, crdt\.Track\{Obs: obs, TS: n\.clock\.Now\(\), Origin: n\.id\}\)/ts := n.clock.Now()\n\tn.mu.Lock()\n\tdefer n.mu.Unlock()\n\twon, prev := n.tracks.Put(id, crdt.Track{Obs: obs, TS: ts, Origin: n.id})/"
  "merge_regress is never counted|internal/node/node.go|s/\t\t\tn\.c\.mergeRegress\.Add\(1\)\n//"
  "checksum ignores speed and heading|internal/crdt/track.go|s/o\.AltFt, o\.GSKt, o\.HeadingDeg\}/o.AltFt}/"
  "harness: recovery ignores its floor|harness/run.py|s/if all\(v <= floor for _, v in after\[i:\]\):/if True:/"
  "harness: recovery accepts a single dip|harness/run.py|s/if all\(v <= floor for _, v in after\[i:\]\):/if after[i][1] <= floor:/"
  "harness: recovery floor is the best pre-cut moment|harness/run.py|s/floor = max\(before\)/floor = min(before)/"
  "harness: lag is always zero|harness/run.py|s/lags\.append\(max\(0, fresh - have\)\)/lags.append(0)/"
  "harness: behind is never counted|harness/run.py|s/behind \+= sum\(1 for x in lags if x > BEHIND_MS\)/behind += 0/"
  "harness: propagation counts a node's own reports|harness/run.py|s/ and r not in made\.get\(\(n, ac\), set\(\)\)//"
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
