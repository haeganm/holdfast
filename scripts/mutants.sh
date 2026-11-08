#!/usr/bin/env bash
# Plants known bugs in a scratch copy of the repo and checks that the tests
# catch every one. A test suite that stays green under a planted bug is not
# testing that behaviour.
#
# Each mutant: description | file | perl substitution applied to that file.
# HOLDFAST_MUTANTS_ONLY=<regex> runs only the mutants whose description
# matches, through the same build-then-test steps as the full run.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
PY="${PY:-${PYTHON:-python3}}"  # check.sh exports PY
# Every mutant was checked to die at the simulation's default of 200
# schedules; the gate's larger run is for finding new bugs, not for these.
export HOLDFAST_SIM_RUNS=200
command -v "$PY" >/dev/null 2>&1 || PY=python
mutants=(
  "Put keeps the smaller track|internal/crdt/track.go|s/if ok && Merge\(cur, t\) == cur \{/if ok \&\& Merge(cur, t) != cur {/"
  "Compare drops the content tie-break|internal/crdt/track.go|s/return cmp\.Or\(/return 0 * cmp.Or(/"
  "Diff ignores a stamp tie with different contents|internal/crdt/track.go|s/case mv\.Sum != theirs\.Sum:/case false:/"
  "Diff never asks for aircraft only the peer has|internal/crdt/track.go|s/want = append\(want, id\)\n\t\t\}\n\t\}\n\tslices/_ = id\n\t\t}\n\t}\n\tslices/"
  "HLC Update forgets the +1 when the remote is ahead|internal/hlc/hlc.go|s/Logical: m\.Logical \+ 1\}/Logical: m.Logical}/"
  "HLC lets the logical part pass the bound|internal/hlc/hlc.go|s/if c\.last\.Logical > MaxLogical \{/if false {/"
  "HLC Now reuses a stamp in the same millisecond|internal/hlc/hlc.go|s/\} else \{\n\t\tc\.last\.Logical\+\+\n\t\}\n\tc\.rollOver\(\)/}\n\tc.rollOver()/"
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
  "Ingest stamps without the lock|internal/node/node.go|s/\tn\.mu\.Lock\(\)\n\tdefer n\.mu\.Unlock\(\)\n\tn\.c\.ingestTotal\.Add\(1\)/\tn.c.ingestTotal.Add(1)/"
  "ingest_regress is never counted|internal/node/node.go|s/\t\tn\.c\.ingestRegress\.Add\(1\)\n//"
  "merge_regress is never counted|internal/node/node.go|s/\t\t\tn\.c\.mergeRegress\.Add\(1\)\n//"
  "Ord drops the origin terminator|internal/crdt/track.go|s/\tb = append\(b, ' '\) \/\/ below/\t\/\/ below/"
  "Ord treats negative numbers as positive|internal/crdt/track.go|s/\t\treturn \^u\n/\t\treturn u\n/"
  "Restore does not advance the clock|internal/node/node.go|s/\tn\.clock\.Update\(high\)\n\tfor id, t := range keep \{/\t_ = high\n\tfor id, t := range keep {/"
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
  "edge: the cloud token may cross the network in the clear|cmd/holdfast/main.go|s/private := ip != nil/private := true || ip != nil/"
  "harness: merge-order catch-up waits only on the lowest value|harness/run.py|s/due\[ac\] = max\(due\.get\(ac, merge_key\(tr\)\), merge_key\(tr\)\)/due[ac] = min(due.get(ac, merge_key(tr)), merge_key(tr))/"
  "harness: live propagation counts from the observation|harness/run.py|s/return receipts_polled\(run\.polls\)/return {}, []/"
  "harness: the sweep's own results mark the checkout dirty|harness/run.py|s/, \"--\", \"\.\", \":\(exclude\)results\"//"
  "harness: a deployed peer's table always counts as matching|harness/run.py|s/matches = set\(check\[\"edges\"\]\.values\(\)\) == \{check\[\"cloud\"\]\[\"hash\"\]\}/matches = True/"
  "cloud: a failed write still answers success|cloud/peer.go|s/\tif err != nil \{\n\t\t\/\/ Nothing is lost/\tif false {\n\t\t\/\/ Nothing is lost/"
  "harness: a cloud peer left in the shell leaks into every run|harness/run.py|s/\"CLOUD_PEER\": \"\", \"CLOUD_TOKEN\": \"\",/\"CLOUD_UNUSED\": \"\",/"
  "harness: the trace model counts a write lost even when it was older|harness/trace_stats.py|s/ and observed\(r\) > observed\(other\):/:/"
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
  "harness: staleness counts aircraft long gone quiet|harness/run.py|s/if ingesting and now - heard\[ac\] <= ACTIVE_MS:/if ingesting:/"
  "harness: divergence treats a missing aircraft as agreement|harness/run.py|s/len\(\{value\(t, ac\) for t in held\}\) > 1/len({value(t, ac) for t in held} - {None}) > 1/"
  "harness: catch-up ignores what was reported by the heal|harness/run.py|s/\.get\(\"at\", 0\) >= at$/.get(\"at\", 0) >= 0/m"
  "harness: bytes per useful update divides by every report|harness/run.py|s/useful = round\(sent \/ merges, 1\)/useful = round(sent \/ ingest, 1)/"
  "harness: propagation ignores the first receipt|harness/run.py|s/out\.append\(max\(0, now - q\.pop\(0\)\[1\]\)\)/out.append(max(0, now - q.pop(0)[0]))/"
  "harness: receipts keep the last receipt, not the first|harness/run.py|s/first\[key\] = min\(first\.get\(key, got\), got\)/first[key] = got/"
  "harness: settle ignores unequal hashes|harness/run.py|s/and len\(\{p\.states\[n\]\[\"hash\"\] for n in run\.nodes\}\) == 1/and True/"
  # W3: the harness's published columns, its smoke gate, its labels and its reads of a node
  "harness: staleness counts an aircraft from its observation, not its receipt|harness/run.py|s/heard\[ac\] = max\(heard\.get\(ac, 0\), receipt\.get\(\(ac, own\), own\)\)/heard[ac] = max(heard.get(ac, 0), own)/"
  "harness: bytes per useful update counts the hub's relay merges|harness/run.py|s/for n in EDGES\) if upto else 0/for n in run.nodes) if upto else 0/"
  "harness: bytes per useful update counts merges after the bytes stopped|harness/run.py|s/upto = \[p\.states for p in run\.polls if p\.t <= run\.events\[\"tx:end\"\]\]/upto = [p.states for p in run.polls]/"
  "harness: propagation reaches a node before the report arrived|harness/run.py|s/out\.append\(max\(0, now - q\.pop\(0\)\[1\]\)\)/out.append(now - q.pop(0)[1])/"
  "harness: a poll cut off mid-body ends the sweep|harness/run.py|s/READ_ERRORS = \(OSError, http\.client\.HTTPException, ValueError\)/READ_ERRORS = (OSError, ValueError)/"
  "harness: a poll that isn't JSON ends the sweep|harness/run.py|s/READ_ERRORS = \(OSError, http\.client\.HTTPException, ValueError\)/READ_ERRORS = (OSError, http.client.HTTPException)/"
  "harness: --skip-build trusts the cloud peer's image|harness/run.py|s/if built != commit:/if built != commit and image == IMAGE:/"
  "harness: two different dirty trees share a label|harness/run.py|s/return f\"\{commit\}-dirty-\{changes_hash\(ROOT\)\}\" if dirty/return f\"{commit}-dirty\" if dirty/"
  "harness: the trace's late reports count receipts from before the window|harness/trace_stats.py|s/if lo <= r\[\"t_ms\"\] < hi:\n            best = seen\.get\(r\[\"icao\"\]\)\n            if best is not None/if r[\"t_ms\"] < hi:\n            best = seen.get(r[\"icao\"])\n            if lo <= r[\"t_ms\"] and best is not None/"
  "harness: prop_p95 reports the median|harness/run.py|s/\"prop_p95\": blank_pct\(prop, 95\)/\"prop_p95\": blank_pct(prop, 50)/"
  "harness: prop_p99 reports the p95|harness/run.py|s/\"prop_p99\": blank_pct\(prop, 99\)/\"prop_p99\": blank_pct(prop, 95)/"
  "harness: the sync error rate divides by successes only|harness/run.py|s/totals\.get\(\"sync_err\", 0\) \/ sync_total/totals.get(\"sync_err\", 0) \/ max(1, totals.get(\"sync_ok\", 0))/"
  "harness: diverge_mean is the max|harness/run.py|s/\"diverge_mean\": round\(sum\(diverge\) \/ len\(diverge\), 4\)/\"diverge_mean\": round(max(diverge), 4)/"
  "harness: p95 recovery reads the worst lag, not the pooled p95|harness/run.py|s/recovery_p95\(p95_by_poll, cut, heal\)/recovery_p95(worst_by_poll, cut, heal)/"
  "harness: a poll's p95 lag is its median|harness/run.py|s/p95_by_poll\.append\(\(p\.t, pct\(poll_lags, 95\)\)\)/p95_by_poll.append((p.t, pct(poll_lags, 50)))/"
  "harness: worst lag takes each node's best aircraft|harness/run.py|s/worst = max\(worst, lags\[-1\]\)/worst = max(worst, lags[0])/"
  "harness: a node holding nothing lags from the freshest report, not the first|harness/run.py|s/if ac in tracks else first_seen\[ac\]/if ac in tracks else fresh/"
  "harness: recorded receipts ignore the feed delay|harness/run.py|s/r\.get\(\"delay_ms\", 0\)\) for r in/0) for r in/"
  "harness: receipts read the trace from its start, not the window|harness/run.py|s/run\.trace_from_ms, run\.duration_ms, t0_abs\)/0, run.duration_ms, t0_abs)/"
  "harness: propagation counts warm-up reports|harness/run.py|s/lo, hi = t0_abs \+ WARMUP_MS, t0_abs/lo, hi = t0_abs, t0_abs/"
  "harness: the smoke gate passes a run that never settled|harness/run.py|s/if not row\[\"settled\"\] or row\[\"errors\"\]/if row[\"errors\"]/"
  "harness: the smoke gate passes a cut that never recovered|harness/run.py|s/and healed and row\[\"recover_p95_ms\"\] == \"\":/and False:/"
  "harness: the smoke gate passes a run that held reports at startup|harness/run.py|s/if row\[\"ingest_held\"\]:/if False:/"
  "harness: the smoke gate passes a cloud table that ended different|harness/run.py|s/if row\[\"cloud_matches\"\] is False:/if False:/"
  # W2: the edge, the feed, the trace reader and the cloud peer
  "cloud: a bad item whose ord isn't a string can't be replaced|cloud/peer.go|s/cond \+= \" OR NOT attribute_type\(ord, :s\)\"\n\t\tvalues\[\":s\"\] = &types\.AttributeValueMemberS\{Value: \"S\"\}/cond += \" OR attribute_not_exists(ord)\"/"
  "cloud: a valid item only ahead of this instance's clock counts as corrupt|cloud/peer.go|s/bad = append\(bad, corrupt\(stored, n\.Restore\(stored\)\)\.\.\.\)/bad = append(bad, n.Restore(stored)...)/"
  "cloud: an item no clock would accept is never replaced|cloud/peer.go|s/\tif len\(refused\) == 0 \{\n\t\treturn nil\n\t\}/\tif true {\n\t\treturn nil\n\t}/"
  "adsb: a Retry-After in seconds is not capped|internal/adsb/adsb.go|s/min\(max\(s, 0\), int\(maxRetryAfter\/time\.Second\)\)/max(s, 0)/"
  "adsb: a Retry-After date is not capped|internal/adsb/adsb.go|s/return min\(max\(at\.Sub\(now\), 0\), maxRetryAfter\)/return max(at.Sub(now), 0)/"
  "adsb: only a refusal backs off|internal/adsb/adsb.go|s/\t\tdefault:\n\t\t\twait = backoff\(wait, f\.Every, 0\)/\t\tdefault:\n\t\t\twait = f.Every/"
  "adsb: a slow failure eats its own backoff|internal/adsb/adsb.go|s/\t\t\tnext = time\.Now\(\)\.Add\(wait\)/\t\t\tnext = next.Add(wait)/"
  "adsb: the first poll waits a whole interval|internal/adsb/adsb.go|s/\twait := f\.Every\n\tfor next := time\.Now\(\); ; \{/\twait := f.Every\n\tselect {\n\tcase <-ctx.Done():\n\t\treturn\n\tcase <-time.After(wait):\n\t}\n\tfor next := time.Now(); ; {/"
  "edge: the cloud token is never wired into the client|cmd/holdfast/main.go|s/client\.Transport = bearer\{/_ = bearer{/"
  "edge: the cloud token goes through a proxy in the clear|cmd/holdfast/main.go|s/\t\t\tdirect\.Proxy = nil\n//"
  "edge: a trace offset past the last record replays nothing|cmd/holdfast/main.go|s/if len\(all\) == 0 \|\| \*from > all\[len\(all\)-1\]\.TMS \{/if false {/"
  "edge: a start long past makes every report due at once|cmd/holdfast/main.go|s/\*t0 != 0 && late > 5\*time\.Second/false/"
  "edge: an upload that outlasts the shutdown budget fails the stop|cmd/holdfast/main.go|s/\t\tsrv\.Close\(\)\n\t\treturn nil\n/\t\treturn err\n/"
  "edge: a recorded report's delay is added, not subtracted|cmd/holdfast/main.go|s/ObservedAtMS: start\.UnixMilli\(\) \+ r\.ObservedMS\(\)/ObservedAtMS: start.UnixMilli() + r.TMS + r.DelayMS/"
  "edge: nothing the live feed reports is ingested|cmd/holdfast/main.go|s/n\.Ingest\(r\.ICAO, r\.Obs\)/_ = r/"
  "cloud: a short token is accepted in Lambda|cloud/main.go|s/if inLambda && len\(p\.token\) < 32 \{/if false \&\& len(p.token) < 32 {/"
  "cloud: a table that can't be read answers with an empty picture|cloud/peer.go|s/http\.Error\(w, \"store unavailable\", http\.StatusServiceUnavailable\)\n\t\treturn\n\t\}\n\tif edge \{/stored, ords = map[crdt.ICAO]crdt.Track{}, map[crdt.ICAO]string{}\n\t}\n\tif edge {/"
  "replay: coverage lets a NaN through|internal/replay/trace.go|s/ok := c\.Lat >= -90 && c\.Lat <= 90 && c\.Lon >= -180 && c\.Lon <= 180 && c\.RadiusKm > 0/ok := !(c.Lat < -90 || c.Lat > 90 || c.Lon < -180 || c.Lon > 180) \&\& c.RadiusKm > 0/"
  "replay: coverage takes an infinite radius|internal/replay/trace.go|s/ && !math\.IsInf\(c\.RadiusKm, 1\)//"
  "replay: Read keeps a line's first record and drops the rest|internal/replay/trace.go|s/if strings\.TrimSpace\(text\[dec\.InputOffset\(\):\]\) != \"\" \{/if false {/"
  # The layering docs/DESIGN.md claims, held by internal/layering_test.go
  "the node imports a sensor package|internal/node/node.go|s/\nimport \(\n/\nimport (\n\t_ \"github.com\/haeganm\/holdfast\/internal\/replay\"\n/"
  # W1: the core's ord range, trust boundary, clock offset, counters, replica cap, hold and response refusals
  "Ord pads the logical part too short|internal/crdt/track.go|s/\"%016x%08x\"/\"%016x%x\"/"
  "node has no digest entry cap|internal/node/node.go|s/if len\(d\) > maxEntries \{/if false {/"
  "node accepts a negative stamp|internal/node/node.go|s/if ts\.WallMS < 0 \{/if false {/"
  "node accepts any longitude|internal/node/node.go|s/ \|\| o\.Lon < -180 \|\| o\.Lon > 180//"
  "node accepts any origin in a digest|internal/node/node.go|s/if !originRe\.MatchString\(v\.Origin\) \{/if false {/"
  "node checks drift against true time, not its own clock|internal/node/node.go|s/error \{\n\tlimit := n\.physNow\(\)/error {\n\tlimit := n.trueNow()/"
  "the startup hold ends on true time, not the node's own clock|internal/node/node.go|s/if n\.physNow\(\)\.UnixMilli\(\) < n\.holdUntilMS \{/if n.trueNow().UnixMilli() < n.holdUntilMS {/"
  "state reports the skewed clock as true time|internal/node/node.go|s/NowMS: n\.trueNow\(\)/NowMS: n.physNow()/"
  "ingest_lost counts an older report that lost|internal/node/node.go|s/case !won && prev\.Obs\.ObservedAtMS < obs\.ObservedAtMS:/case !won:/"
  "a stale merge counts as applied|internal/node/node.go|s/n\.c\.mergeStale\.Add\(1\)/n.c.mergeApplied.Add(1)/"
  "receive merges before advancing its clock|internal/node/node.go|s/\tn\.clock\.Update\(high\)\n\tfor id, t := range tracks \{/\t_ = high\n\tfor id, t := range tracks {/"
  "receive advances its clock only to the message stamp|internal/node/node.go|s/\tfor _, t := range tracks \{\n\t\tif t\.TS\.Compare\(high\) > 0 \{\n\t\t\thigh = t\.TS\n/\tfor _, t := range tracks {\n\t\tif t.TS.Compare(high) > 0 {\n\t\t\tcontinue\n/"
  "node lets a new aircraft into a full replica|internal/node/node.go|s/return n\.tracks\.Len\(\) >= maxEntries/return false \&\& n.tracks.Len() >= maxEntries/"
  "the hold stamps a held report over a fresher one|internal/node/node.go|s/ok && cur\.Obs\.ObservedAtMS > held\[id\]\.ObservedAtMS/ok \&\& false \&\& cur.Obs.ObservedAtMS > held[id].ObservedAtMS/"
  "node never counts a response it can't decode|internal/node/node.go|s/\t\tn\.c\.rejected\.Add\(1\)\n\t\treturn fmt\.Errorf\(\"decode response/\t\treturn fmt.Errorf(\"decode response/"
  "node never counts an oversized response|internal/node/node.go|s/\t\tn\.c\.rejected\.Add\(1\)\n\t\treturn fmt\.Errorf\(\"response from %s exceeds/\t\treturn fmt.Errorf(\"response from %s exceeds/"
  "node never counts a response that asks for too much|internal/node/node.go|s/\t\tn\.c\.rejected\.Add\(1\)\n\t\treturn fmt\.Errorf\(\"%s\/sync asked/\t\treturn fmt.Errorf(\"%s\/sync asked/"
  "node counts a cut-off response as a rejection|internal/node/node.go|s/\t\treturn fmt\.Errorf\(\"read response from/\t\tn.c.rejected.Add(1)\n\t\treturn fmt.Errorf(\"read response from/"
  "node merges a sync response before checking what it asks for|internal/node/node.go|s/\tif len\(resp\.Want\) > maxEntries \{/\t_ = n.receive(resp.HLC, resp.Tracks)\n\tif len(resp.Want) > maxEntries {/"
  # wrap-up: refusing trailing content
  "node accepts anything after a request|internal/node/node.go|s/\terr := decodeOne\(r\.Body, v\)\n/\tdec := json.NewDecoder(r.Body)\n\tdec.DisallowUnknownFields()\n\terr := dec.Decode(v)\n/"
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
    go.mod cmd internal cloud harness/run.py harness/trace_stats.py harness/test_run.py) | (cd "$work/src" && tar -xf -)
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

failed=0 ran=0
for m in "${mutants[@]}"; do
  IFS='|' read -r desc file sub <<<"$m"
  [[ -n "${HOLDFAST_MUTANTS_ONLY:-}" && ! "$desc" =~ $HOLDFAST_MUTANTS_ONLY ]] && continue
  ran=$((ran + 1))
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
if [[ $ran -eq ${#mutants[@]} ]]; then
  echo "mutants: all $ran killed"
else
  echo "mutants: all $ran selected by HOLDFAST_MUTANTS_ONLY killed ($ran of ${#mutants[@]})"
fi
