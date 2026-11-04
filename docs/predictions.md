# Predictions, written before the first measured run

Committed before `harness/run.py --family sync` or `--family clock` had ever run, so the git history shows these came first. The results section of the README is checked against this list, including the predictions that turn out wrong.

## Setup the predictions assume

Three edge nodes, 40 synthetic aircraft, one report per aircraft per second, 60 km coverage circles (16 of 40 aircraft seen by two nodes over the full trace), sync every 1 s per peer, 50 s of ingest then 10 s to settle, partitions from 20 s to 40 s. Five repeats per configuration.

## Sync under loss and partitions (hybrid logical clock, no skew)

1. Every run settles: all node hashes equal within 10 s of ingest stopping. Least sure at 50% loss, where TCP retransmits and the 2 s client timeout compete.
2. Propagation p95 rises with loss: no loss < 20% < 50%.
3. Bursty 20% loss (Gilbert-Elliott, same average) costs more than i.i.d. 20%, because a burst can take out every retransmit of one exchange.
4. Star is slower than mesh with no loss, since every report takes two hops through the hub.
5. A partitioned edge recovers (worst p95 lag back to its pre-cut level) within 2 s of the heal, in both topologies.
6. Cutting the hub of a star stops all propagation between edges for the cut; the mesh has no equivalent single point.
7. Zero rejected messages and zero lost local writes in every run.

## Clock skew (mesh, no loss)

Node c's physical clock is offset by 0, -500, -2000 or +2000 ms. Reports of an aircraft arrive once a second, so successive reports are 1 s apart.

8. Under the HLC, `ingest_lost` is 0 in every run at every skew. This is an invariant of the node (every stamp it holds is at or below its clock), so a single nonzero run is a bug, not a statistic.
9. Under the wall clock, skew smaller than the report interval (-500 ms) causes no wrong-order merges: an older report is always more than 500 ms older.
10. Under the wall clock, skew larger than the report interval (-2000 and +2000 ms) causes wrong-order events in every run, mostly on aircraft two nodes both cover: the node with the slow clock stamps its fresh report below a peer's previous one, and loses it.
11. Under the HLC at -2000 and +2000 ms, wrong-order events are at most a tenth of the wall clock's median.

## Kill criterion

If the HLC shows no measurable advantage over the wall clock at any skew, the HLC claim comes out of the README and the project is written up as a sync-under-loss study only.
