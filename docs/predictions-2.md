# Predictions for the real-traffic, live and cloud runs

Written and committed before `harness/run.py` ran any of the families below, so the git history shows these came first, as with [predictions.md](predictions.md). The README grades every one, including the ones that turn out wrong.

One disclosure. Before this file was committed, a few runs were made to check that the new code paths work, and their results were seen:

- Three cloud runs, one per scenario. The cloud predictions (10 to 12) were written before them and were not changed afterwards, though run 1 already suggests prediction 11 may fail: the cloud's p95 with no loss was 1542 ms against the in-memory hub's registered 1589 ms.
- Two runs on the first window of the recorded hour: the star with its hub cut, and the wall clock with c at -4000 ms. The second lost no local writes. That led to the split by direction under "Measured from the trace" below, to rewriting prediction 8 from the trace alone, and to moving the impaired clock runs from -4000 to +4000 ms.

None of these runs is part of the results.

## The runs

- **real-hour**: the recorded hour, [`traces/adsb-2026-11-05.jsonl.gz`](../traces/adsb-2026-11-05.jsonl.gz), mesh, hybrid clock, no skew, under 20% loss, 50% loss and a 20 s partition of node c. 60 runs per condition: run *r* replays minute *r* in full (60 s of ingest), so each condition replays the whole hour.
- **real-base**: the same trace, 8 windows of 50 s spread across the hour (every 7.5 minutes), each configuration on the same 8 windows:
  - mesh, no loss, the reference;
  - star, at no loss, 50% loss and with the hub cut;
  - the hybrid clock and the wall clock with c's clock skewed by -4000, -2000, +2000 and +4000 ms, no loss;
  - both clocks at +4000 ms, under 20% loss and across a partition.
- **cloud**: the synthetic trace, star topology with the hub replaced by the cloud peer on DynamoDB Local, at no loss, 20% loss and with c cut. 5 repeats each, so these compare directly with the registered star runs.
- **live**: the live feed, mesh at no loss, mesh with c cut, star with the hub cut. 3 repeats each.

## Measured from the trace before any run

- 108 aircraft in a median minute (95 to 131), 444 over the hour, 71% of them seen by two or more sensors.
- Each sensor observes about every 9 s or less often: 68 of the recorder's 601 polls were refused.
- The feed's delay from observation to receipt: 0.9 s median, 3.7 s at p90, 22 s at p99.
- Consecutive reports of one aircraft from different nodes are never closer than 2.7 s. By direction, which is what matters for a skewed clock: node c never reports less than 5.4 s after a or b does (the sensors take turns, a, b, c), while a or b reports less than 4 s after c 90% of the time, and less than 3 s after it 50% of the time.
- 178 of 27,716 reports (0.64%) arrive after a fresher observation of the same aircraft was already received by some node: 0 to 14 per minute, median 2.

## Predictions

Sync on real traffic:

1. Every no-loss and partition run settles within 10 s of ingest stopping. At 20% loss most do. At 50% loss fewer than half do (the synthetic trace settled 1 of 9, and digests here are 2.7 times larger).
2. Propagation p95, timed from the first node's receipt of a report, rises with loss: none < 20% < 50%. At 50% loss it is higher than the synthetic trace's 16.6 s. (The feed's own delay is reported separately, from the trace.)
3. After the partition heals, every node holds everything reported before the heal within 2 s (median catch-up).
4. Star is slower than mesh with no loss, and with the hub cut the edges merge nothing from each other for the whole cut.
5. `rejected` and `ingest_lost` are 0 in every hybrid-clock run.
6. Last-writer-wins keeps an older position when a sensor receives a report after a fresher one arrived elsewhere: `ingest_regress` plus `merge_regress` is above 0 in most windows, at about the rate the trace shows (median 2 per minute), under both clocks alike. The hybrid clock orders writes, not observations, so it does not fix this.

Clock skew on real traffic:

7. Under the wall clock at -2000 and +2000 ms, no run loses a local write: no two nodes report the same aircraft less than 2.7 s apart.
8. Under the wall clock at +4000 ms, every run loses local writes, on up to 90% of the handoffs from c to a or b. At -4000 ms, no run does: c never reports less than 5.4 s after another node. How much a skewed wall clock costs depends on which way it is skewed and on the traffic's timing, not only on the size of the skew.
9. Under the hybrid clock, `ingest_lost` is 0 at every skew. At +4000 ms under 20% loss and across a partition, a node can still write before c's latest report reaches it, and lose that race to it later. That shows as `merge_regress`, not as a lost write, and is predicted below a tenth of the wall clock's lost writes in the same windows.

Kill criterion: if on real traffic the hybrid clock shows no advantage over the wall clock at any skew, the HLC claim comes out of the README.

The cloud relay (synthetic trace, against the registered star runs):

10. Every run settles, with the table's contents (the cloud peer's hash) equal to the edges'.
11. Propagation p95 is higher than the in-memory hub's under the same conditions, because every round costs a table scan and conditional writes, but less than twice the hub's with no loss.
12. With no loss, the cloud peer makes at most 3 conditional writes per report ingested, and fewer than 1 in 10 lose their condition.

Live traffic:

13. Every live run settles with equal hashes. With c cut, every node catches up within 2 s of the heal (median). With the hub cut, the edges merge nothing from each other.

## Corrections, made after the runs

Kept apart so the text above stays as it was registered.

- "68 of the recorder's 601 polls were refused": the recorder made 669 requests, 601 answered and 68 failed. Its poll counter counted only answered requests. The trace measurements above are computed from the trace itself and don't depend on the count.
