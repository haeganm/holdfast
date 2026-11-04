#!/usr/bin/env bash
# Plants known bugs in a scratch copy of the repo and checks that the tests
# catch every one. A test suite that stays green under a planted bug is not
# testing that behaviour.
#
# Each mutant: description | file | perl substitution applied to that file.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
mutants=(
  "Put keeps the smaller track|internal/crdt/track.go|s/if ok && Compare\(cur, t\) >= 0 \{/if ok \&\& Compare(cur, t) <= 0 {/"
  "Compare drops the content tie-break|internal/crdt/track.go|s/return cmp\.Or\(/return 0 * cmp.Or(/"
  "Diff ignores a stamp tie with different contents|internal/crdt/track.go|s/case mv\.Sum != theirs\.Sum:/case false:/"
  "Diff never asks for aircraft only the peer has|internal/crdt/track.go|s/want = append\(want, id\)\n\t\t\}\n\t\}\n\tslices/_ = id\n\t\t}\n\t}\n\tslices/"
  "HLC Update forgets the +1 when the remote is ahead|internal/hlc/hlc.go|s/Logical: m\.Logical \+ 1\}/Logical: m.Logical}/"
  "HLC Now reuses a stamp in the same millisecond|internal/hlc/hlc.go|s/\} else \{\n\t\tc\.last\.Logical\+\+\n\t\}\n\treturn c\.last/}\n\treturn c.last/"
  "node merges before advancing its clock|internal/node/node.go|s/n\.clock\.Update\(high\)/_ = high/"
  "node accepts stamps from the far future|internal/node/node.go|s/if ts\.WallMS > limit \{/if false {/"
)

work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

failed=0
for m in "${mutants[@]}"; do
  IFS='|' read -r desc file sub <<<"$m"
  rm -rf "$work/src"
  mkdir -p "$work/src"
  (cd "$root" && tar --exclude=./results --exclude=./.git -cf - .) | (cd "$work/src" && tar -xf -)
  before="$(sha256sum "$work/src/$file" | cut -d' ' -f1)"
  perl -0pi -e "$sub" "$work/src/$file"
  after="$(sha256sum "$work/src/$file" | cut -d' ' -f1)"
  if [[ "$before" == "$after" ]]; then
    echo "MUTANT DID NOT APPLY: $desc ($file). Fix the substitution; a mutant that changes nothing proves nothing."
    failed=1
    continue
  fi
  # Compile the tests without running them, and keep vet out of the test run,
  # so "killed" can only mean an assertion failed.
  if ! (cd "$work/src" && go test -vet=off -count=1 -run '^$' ./internal/... >/dev/null 2>&1); then
    echo "MUTANT DID NOT BUILD: $desc. A compile error is not a test catching the bug."
    failed=1
    continue
  fi
  if (cd "$work/src" && go test -vet=off -count=1 ./internal/... >/dev/null 2>&1); then
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
