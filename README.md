# holdfast

Edge nodes that keep one shared picture of the aircraft around them when the network between them drops packets or cuts out, and a harness that measures how well that works.

Three nodes in Docker each ingest aircraft position reports, keep a replica of the fused picture, and sync by anti-entropy over HTTP while `tc netem` drops packets or cuts a node off. Every number below came out of [`harness/run.py`](harness/run.py), and the predictions it is graded against were committed before the first measured run: [docs/predictions.md](docs/predictions.md) (commit 45f5659).

## What it does

Each node keeps a map from aircraft to a last-writer-wins register stamped with a hybrid logical clock (HLC). Nodes sync by anti-entropy: send a digest, get back what you are missing, send what the peer is missing. The map is a CRDT, so replicas converge whatever order messages arrive in and however often they are lost or repeated, provided that:

- every pair of nodes eventually completes a sync round, directly or through other nodes
- the spread between the fastest and slowest physical clock stays under the max drift (5 s by default), since a node rejects stamps further ahead of its own clock than that
- the picture stays at or under 10,000 aircraft, the most one digest may carry

A lost exchange is never retried. The next tick is the retry, 1 s after a round ends (so up to 3 s after a round that hit the 2 s client timeout started), which is safe because every exchange is idempotent.

The two parts worth reading are [`internal/hlc`](internal/hlc/hlc.go) (the clock) and [`internal/crdt`](internal/crdt/track.go) (the map), with their property tests. `scripts/mutants.sh` plants 29 known bugs in a scratch copy (8 in the clock and the map, 8 in the node, 13 in the harness's own arithmetic) and checks that the tests catch every one.

## Setup

Three edge nodes, 40 synthetic aircraft, one report per aircraft per second, 60 km coverage circles (16 of the 40 aircraft are seen by two nodes), 50 s of ingest, then 10 s to settle. Partitions run from 20 s to 40 s. The harness polls every node's `/state` every 100 ms over a control network netem never touches. Two topologies:

- **mesh**: every edge syncs with every other edge
- **star**: edges sync only with a hub that has no coverage of its own, the central-server baseline

Metrics, defined in the [`run.py`](harness/run.py) docstring:

- **propagation latency**: for each report made after a 3 s warm-up and each other edge node, time until that node holds the report or something fresher (100 ms resolution). Percentiles are over the reports that arrived. **never delivered** is the share that had not arrived by the end of the run, and was added after the predictions, when a review found the percentiles were silently leaving those out.
- **settle**: after ingest stops, time until every node's replica hash is equal
- **recovery**: after a partition heals, time until the p95 lag over all nodes and aircraft is back at or below its worst value before the cut (the registered definition). `recover_all` is stricter (every node, every aircraft, held for 2 s) and was added after the predictions were registered.
- **wire bytes per report**: bytes the mesh interfaces sent during ingest, headers and retransmits included, divided by reports ingested. netem drops packets before the interface counts them, so under loss this counts what got onto the wire, not what was attempted.

## Results: sync under loss and partitions

Source: [`results/sweep-sync/summary.csv`](results/sweep-sync/summary.csv), commit 5b6bd65, HLC, no skew. Cells show the median, then min-max over repeats. Times are in ms. "never" means the event did not happen within the run. The sweep was killed at 51 of 55 runs when the PC ran low on memory, so four star configurations have 4 repeats instead of 5. The mean poll interval was 99.9 to 100.1 ms in every run. A median over 4 repeats is the mean of the middle two, hence the .5s.

| topology | scenario | settled | settle | prop p50 | prop p95 | prop p99 | never delivered | recovery | sync errors | wire B/report |
|---|---|---|---|---|---|---|---|---|---|---|
| mesh | no loss | 5/5 | 302 (250-842) | 271 (243-344) | 649 (483-818) | 750 (591-909) | 0 | | 0% | 720 (705-768) |
| mesh | 20% loss | 5/5 | 809 (382-1262) | 509 (468-520) | 1201 (1163-1284) | 1684 (1454-1806) | 0 | | 4.6% | 654 (642-680) |
| mesh | 20% bursty | 5/5 | 850 (319-1518) | 542 (525-568) | 1644 (1566-2038) | 2662 (2264-3537) | 0 | | 8.0% | 615 (609-642) |
| mesh | 50% loss | 1/5 | 4472 (1 run) | 4521 (3831-7903) | 16558 (9492-31464) | 20879 (12555-36076) | 7.9% (0-22.7%) | | 78.9% | 196 (171-212) |
| mesh | c cut 20 s | 5/5 | 259 (227-653) | 456 (421-577) | 18073 (16921-18400) | 20691 (19469-21051) | 0 | 1201 (322-1453) | 8.1% | 526 (525-534) |
| star | no loss | 5/5 | 1547 (1031-1875) | 1028 (989-1095) | 1589 (1521-1794) | 1788 (1665-1937) | 0 | | 0% | 566 (565-567) |
| star | 20% loss | 4/5 | 3383 (1653-9111) | 1634 (1555-1788) | 4868 (3929-8063) | 7701 (5594-10462) | 0 (0-0.3%) | | 3.4% | 413 (390-439) |
| star | 20% bursty | 4/4 | 4207 (1873-9221) | 2630 (1794-3848) | 9230 (6294-11759) | 11972.5 (7736-14203) | 0 | | 11.1% | 311 (263-373) |
| star | 50% loss | 0/4 | never | 16285 (12308-25069) | 29061.5 (22351-39043) | 32306.5 (23965-41043) | 35.6% (19.5-58.0%) | | 75.2% | 89 (68-111) |
| star | c cut 20 s | 4/4 | 1302 (999-1556) | 1240 (1076-1368) | 19266 (18991-19446) | 21859 (21529-21958) | 0 | 2117 (1710-2902) | 3.7% | 456 (455-458) |
| star | hub cut 20 s | 4/4 | 1215.5 (1137-1455) | 1494 (1425-1578) | 20462.5 (20271-20608) | 22173 (22101-22402) | 0 | 2214.5 (2115-2224) | 12.9% | 352 (351-354) |

Across all 51 runs: 0 lost local writes, 0 merges that replaced a fresher report with an older one, and 0 messages rejected at the trust boundary. 135 requests were cut off in flight by packet loss and counted as aborted (see prediction 7).

![Propagation latency by network condition](docs/sync-latency.png)

![Per-node lag during a partition](docs/partition-timeline.png)

What the data says:

- **A second hop costs 2.4x with no loss.** Star p95 is 1589 ms against mesh's 649 ms. The absolute gap widens under loss, since every report crosses two lossy links (the ratio peaks at 5.6x under bursty loss).
- **Bursty loss hurts more than the same average loss spread evenly.** At 20% average loss, Gilbert-Elliott bursts raised p95 1.4x in the mesh and 1.9x in the star over i.i.d. loss.
- **50% loss is where the design breaks.** About 79% of sync rounds fail, mesh p95 is 26x the no-loss value, and only 1 of 9 runs settled within 10 s. Those percentiles still flatter it: 7.9% of report deliveries in the mesh (median, up to 22.7%) and 35.6% in the star (up to 58.0%) never happened at all. Wire bytes per report fall to 27% of the no-loss value in the mesh and 16% in the star, partly because netem's drops are never counted and partly because failed rounds stop sending.
- **At 50% loss the limit is TCP itself, not the client timeout.** A diagnostic added after the predictions ([`harness/tcp_probe.py`](harness/tcp_probe.py), [`results/tcp-probe/summary.csv`](results/tcp-probe/summary.csv), commit ac9c857, three runs per setting) reran the mesh with the client timeout at 2 s and at 10 s, and read the kernel's TCP counters inside each node over 43 s of ingest. With no loss: 260 rounds completed, 0 failed, 0 new connections, 0 retransmits, because one keep-alive connection per peer carries everything. At 50% loss with the 2 s timeout: 11-12 completed, 77-78 failed, 56-60 new connections and 174-204 SYN retransmits, since a timed-out round drops its connection and the next round has to start with a fresh handshake. Raising the timeout to 10 s cut the failures to 17-21 and the new connections to 17-20, but it did not get more through: 5-14 rounds completed and propagation p95 was 16.5-29.6 s, against 14.7-19.3 s at 2 s. Across the 6 peer links that is about one completed round per link every 20 s at best. The timeout decides how failure shows up, not how much gets through.
- **During a partition the rest of the mesh keeps working.** While c is cut off, a and b keep merging each other's reports (468 merged tracks, median). When the star's hub is cut, the edges merge nothing at all for the whole cut: 0 in every run.

## Results: clock skew

Source: [`results/sweep-clock/summary.csv`](results/sweep-clock/summary.csv), commit 005ce57 (the node code is unchanged from 5b6bd65, only the harness moved), mesh, no loss, node c's physical clock offset by the skew. Five repeats each, all settled. Under each clock the same trace is replayed, and two nodes that cover the same aircraft ingest the same report at the same moment.

| clock | c's skew | lost local writes | older report won a merge | wrong final values | worst lag (ms) | prop p95 (ms) |
|---|---|---|---|---|---|---|
| HLC | -2000 ms | 0 | 0 | 0 | 1000 | 645 (600-913) |
| HLC | -500 ms | 0 | 0 | 0 | 1000 | 677 (627-901) |
| HLC | 0 | 0 | 0 | 0 | 1000 | 753 (525-880) |
| HLC | +2000 ms | 0 | 0 | 0 | 1000 | 597 (469-809) |
| wall | -2000 ms | 314 (314-315) | 0 | 0 | 2000 (2000-3000) | 607 (591-684) |
| wall | -500 ms | 0 | 0 | 0 | 1000 (1000-2000) | 760 (581-813) |
| wall | 0 | 0 | 0 | 0 | 1000 | 664 (538-767) |
| wall | +2000 ms | 312 | 0 | 0 | 1000 | 760 (739-818) |

Columns are `ingest_lost`, `merge_regress`, `wrong_final`, `lag_worst` and `prop_p95`. `lag_worst` is the worst lag of any node on any aircraft at any poll during ingest, and was added after the predictions. Lag moves in whole seconds, so 1000 is the floor.

![Lost writes by clock and skew](docs/clock-skew.png)

What the data says:

- **The HLC lost nothing at any skew.** That's the node's invariant (every stored stamp is at or below the clock), and it held in all 20 runs.
- **The wall clock is fine below the report interval and breaks above it.** At 500 ms of skew an older report is always more than 500 ms older, so nothing misorders. At 2 s, it loses 312 to 315 local writes in every run.
- **The node that loses is always the one whose clock is behind the peer it shares aircraft with.** At -2000 ms that is c, so every lost write is on c. At +2000 ms c runs ahead, so a, on true time, is the slow one: c's older report, stamped ahead, beats a's fresh one, and every lost write is on a. b never loses, because its coverage circle does not overlap c's (138 km apart, 60 km radii), and a loss needs a peer's report of the same aircraft. (Per node counts come from the first and last poll in `raw/*.json.gz`, all 10 runs.)
- **In this setup the losses cost freshness, not correctness.** No run ended with a wrong value under either clock, because a node that loses a write is holding a peer's report of the same aircraft, and the next report replaces it. At -2000 ms the worst aircraft went 2 to 3 s stale, against 1 s under the HLC. At +2000 ms the worst lag stayed at 1 s. With real sensors, two nodes would not report identical values, and a lost write would be a lost measurement. This trace can't show that.

## Predictions, graded

| # | prediction | result |
|---|---|---|
| 1 | every run settles within 10 s of ingest stopping | **failed.** All no-loss and partition runs settled. Under loss: star 20% settled 4/5, mesh 50% 1/5, star 50% 0/4. |
| 2 | propagation p95 rises with loss: none < 20% < 50% | **held** in both topologies: mesh 649 < 1201 < 16558 ms, star 1589 < 4868 < 29061.5 ms. The 50% figures leave out reports that never arrived (see the table), and counting those would only raise them, so the ordering stands. |
| 3 | bursty 20% costs more than i.i.d. 20% | **held** on medians: mesh 1644 vs 1201 ms (ranges do not overlap), star 9230 vs 4868 ms (ranges overlap) |
| 4 | star is slower than mesh with no loss | **held**: p95 1589 vs 649 ms, p50 1028 vs 271 ms, ranges do not overlap |
| 5 | a partitioned edge recovers within 2 s of the heal, in both topologies | **held for mesh** (median 1201 ms, worst 1453 ms). **Failed for star**: median 2117 ms, 3 of 4 runs over 2 s. One of the three is 2014 ms, close enough that the heal command's own time could cover it, so 2 of 4 are clearly over. All are upper bounds, timed from when the heal command starts (it takes 103-157 ms to apply). A hub cut, which this prediction does not cover, took 2115-2224 ms. |
| 6 | cutting the star's hub stops all propagation between edges; the mesh has no such single point | **held**: 0 tracks merged by any edge while the hub was cut, in 4/4 runs, against 468 (median) between a and b while the mesh lost c. `merged_cut` was added after the predictions. |
| 7 | zero rejected messages and zero lost local writes in every run | **failed as registered, then fixed in the counter.** Three earlier sweeps all counted rejections under loss. Two were stopped early to fix what independent reviews found, before any of their numbers were used: [21 runs at the pre-review code](results/aborted-sweep-sync-1009/summary.csv), 48 rejections, and [4 runs at ac7bf70](results/aborted2-sweep-sync/summary.csv), 20. The third ([commit 9679d12](results/partial-sweep-sync-9679d12/summary.csv)) was stopped at 35 of 55 runs to investigate this, with 72 rejections, 43 of them in mesh at 50% loss. The cause was the counter, not the peers: a request whose sender timed out mid-upload reached the receiver as a short body and was counted as a trust-boundary rejection. Reproduced with a test, fixed in 5b6bd65 by counting cut-off bodies as `aborted`. After the fix: 0 rejected and 0 lost local writes in all 51 runs. The fix was made after the predictions were registered and after the data showed the problem. |
| 8 | under the HLC, `ingest_lost` is 0 in every run at every skew | **held**: 0 in 20 of 20 runs |
| 9 | under the wall clock, -500 ms of skew causes no wrong-order merges | **held**: 0 lost writes and 0 regressions in 5 of 5 runs. One run had a 2 s worst lag with no misordered merge behind it, unexplained. |
| 10 | under the wall clock, -2000 and +2000 ms cause wrong-order events in every run, mostly on aircraft two nodes both cover, because the slow-clock node loses its fresh report | **held**: 312-315 lost writes in all 10 runs. The losses are always on the node whose clock is behind its peer's: c at -2000 ms, a at +2000 ms. That matches the wording, but only in a relative sense. The prediction was written with c as the slow node, and at +2000 ms the losing node is the one with the correct clock. "On shared aircraft" holds by construction (a loss needs a peer's report of the same aircraft), not by a per-aircraft count. |
| 11 | under the HLC at -2000 and +2000 ms, wrong-order events are at most a tenth of the wall clock's median | **held**: 0 against 314 and 312 |

**Kill criterion** (drop the HLC claim if it shows no advantage over the wall clock at any skew): **not triggered.** At 2 s of skew the HLC loses no writes where the wall clock loses about 313 per run, and at -2000 ms it keeps the worst aircraft at 1 s of lag instead of 2 to 3 s. The claim is scoped by the last bullet above. In this synthetic setup neither clock ended with a wrong picture.

## Design decisions

- **HLC as a struct of wall ms plus a 32-bit logical counter**, rather than packed into one uint64. Readable in JSON and in tests. It costs a few bytes per stamp, and packing stays possible later. The logical part is capped at 2^20: past that the clock moves to the next millisecond instead of wrapping, and a node rejects any stamp over the cap.
- **Stdlib HTTP and JSON, digest anti-entropy**, rather than gossip (memberlist) or a binary encoding. No dependencies, and every message can be read with curl. The cost shows at 50% loss: a request and response over TCP rarely finish when half the packets drop, whatever the timeout (measured above).
- **A synthetic trace**, rather than recorded ADS-B. Ground truth has to be exact for propagation and lag to mean anything, and committing community-collected traffic data brings share-alike licensing into the repo. A recorded-traffic adapter can emit the same JSONL later.

## Caveats

- All containers share one Docker VM clock, so clock error is injected with a flag (`-clock-offset-ms`), not observed.
- netem shapes egress only, so a loss rate applies once per hop in each direction.
- Five repeats per configuration (four for some star runs) is enough to see medians and spread, not to test significance.
- Recovery and propagation are upper bounds at 100 ms resolution, and lag moves in whole seconds because aircraft report once a second.
- No authentication or TLS between nodes, so any host that can reach a node can write to it. The digest checksum is 64-bit FNV, which is fine against accidents but not against a peer crafting collisions. Both belong with node identity, a later phase.

## Run it

```
./check.sh                                                 # gofmt, vet, race tests, mutants, harness tests, Docker smoke
python -I harness/run.py --family sync --repeats 5         # about 70 s per run
python -I harness/run.py --family clock --repeats 5
python -I harness/run.py --reanalyze results/sweep-sync    # recompute summary.csv from raw/, no Docker
python -I harness/tcp_probe.py --out results/tcp-probe loss0:2s loss50:2s loss50:10s   # kernel TCP counters mid-run
pip install -r harness/requirements.txt                    # matplotlib, for plot.py only
python harness/plot.py --sync results/sweep-sync --clock results/sweep-clock --out docs
```

`plot.py` also prints the results tables. Needs Go 1.27, Docker with `sch_netem`, and Python 3.12.

## License

MIT
