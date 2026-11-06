"""Checks the harness's own arithmetic on hand-built polls, so a wrong
formula can't quietly produce plausible numbers.

    python -m unittest discover -s harness
"""

import http.server
import itertools
import json
import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest import mock

import run as h
import trace_stats as ts

T0 = 1_000_000  # absolute ms of the trace start


def state(now_rel, tracks, hash_="x", counters=None):
    return {"now_ms": T0 + now_rel, "hash": hash_, "counters": counters or {}, "tracks": tracks}


def poll(t, **states):
    return h.Poll(t, states)


class PercentileTest(unittest.TestCase):
    def test_nearest_rank(self):
        vals = list(range(1, 101))
        self.assertEqual(h.pct(vals, 50), 50)
        self.assertEqual(h.pct(vals, 95), 95)
        self.assertEqual(h.pct(vals, 100), 100)
        self.assertEqual(h.pct([7], 99), 7)


class AnalyzeTest(unittest.TestCase):
    def setUp(self):
        r = T0 + 5000  # one report, made by a at t=5000
        mine = {"AAAAAA": {"ts": {}, "origin": "a", "at": r, "own": r}}
        held = {"AAAAAA": {"ts": {}, "origin": "a", "at": r}}
        cfg = h.Config("test", "mesh", "loss0", "hlc", 0)
        self.exp = h.Run(cfg, 0, duration_ms=10_000, settle_ms_window=5000)
        c0 = {"sync_ok": 10, "sync_err": 4, "ingest_total": 0}
        c1 = {"sync_ok": 30, "sync_err": 4, "ingest_total": 9, "merge_applied": 3}
        self.exp.polls = [
            poll(0, a=state(0, {}, "e", c0), b=state(0, {}, "e", c0), c=state(0, {}, "e", c0)),
            poll(5000, a=state(5000, mine, "1"), b=state(5000, {}, "e"), c=state(5000, {}, "e")),
            poll(5300, a=state(5300, mine, "1"), b=state(5300, held, "1"), c=state(5300, {}, "e")),
            poll(5600, a=state(5600, mine, "1"), b=state(5600, held, "1"), c=state(5600, held, "1")),
            poll(10_400, a=state(10_400, mine, "1", c1), b=state(10_400, held, "1", c1), c=state(10_400, held, "1", c1)),
        ]

    def analyze(self):
        with tempfile.TemporaryDirectory() as d:
            return h.analyze(self.exp, Path(d) / "polls.csv")

    def test_propagation_counts_only_nodes_that_did_not_make_the_report(self):
        self.assertEqual(h.propagation(self.exp), ([300, 600], 2, 0))

    def test_propagation_counts_from_first_receipt_when_given(self):
        # The report was observed at 5000 but first received at 5200.
        receipt = {("AAAAAA", T0 + 5000): T0 + 5200}
        self.assertEqual(h.propagation(self.exp, receipt), ([100, 400], 2, 0))

    def test_a_node_already_holding_something_fresher_is_reached_at_the_receipt(self):
        # a's report of a position observed at 5000 reaches a at 7000, 2 s late.
        # b had reported a fresher one (6000) itself and c held b's by 6500, so
        # both count as reached when a's report arrives: at 0, never before it.
        run = h.Run(h.Config("test", "mesh", "loss0", "hlc", 0), 0, duration_ms=10_000, settle_ms_window=0)
        b_own = {"AAAAAA": {"ts": {}, "origin": "b", "at": T0 + 6000, "own": T0 + 6000}}
        fresher = {"AAAAAA": {"ts": {}, "origin": "b", "at": T0 + 6000}}
        a_late = {"AAAAAA": {"ts": {}, "origin": "a", "at": T0 + 5000, "own": T0 + 5000}}
        run.polls = [poll(6000, a=state(6000, {}), b=state(6000, b_own), c=state(6000, {})),
                     poll(6500, a=state(6500, fresher), b=state(6500, b_own), c=state(6500, fresher)),
                     poll(7000, a=state(7000, a_late), b=state(7000, b_own), c=state(7000, fresher))]
        receipt = {("AAAAAA", T0 + 5000): T0 + 7000, ("AAAAAA", T0 + 6000): T0 + 6000}
        self.assertEqual(h.propagation(run, receipt), ([0, 0, 500, 500], 4, 0))

    def test_a_report_that_never_arrives_is_counted_not_dropped(self):
        for p in self.exp.polls[3:]:
            p.states["c"] = dict(p.states["c"], tracks={})  # c never gets a's report
        self.assertEqual(h.propagation(self.exp), ([300], 2, 1))
        self.assertEqual(self.analyze()["prop_unresolved"], 0.5)

    def test_summary(self):
        row = self.analyze()
        self.assertTrue(row["settled"])
        self.assertEqual(row["settle_ms"], 400)  # first poll after ingest end with equal hashes
        self.assertEqual(row["prop_p50"], 300)
        self.assertEqual(row["prop_n"], 2)
        # counters are measured from the first poll: 3 nodes x (30-10) ok, 0 new errors
        self.assertEqual(row["sync_ok"], 60)
        self.assertEqual(row["sync_err"], 0)
        self.assertEqual(row["ingest_total"], 27)
        self.assertEqual(row["wrong_final"], 0)

    def test_bytes_per_report_and_per_useful_update(self):
        self.exp.tx = {"start": {n: 100 for n in h.EDGES}, "end": {n: 400 for n in h.EDGES}}
        self.exp.events = {"tx:start": 0, "tx:end": 10_400}
        row = self.analyze()
        self.assertEqual(row["wire_bytes_per_obs"], 33.3)  # 900 bytes over 27 reports
        self.assertEqual(row["wire_bytes_per_useful"], 100.0)  # 900 bytes over 9 winning merges

    def test_useful_updates_are_the_edges_merges_while_the_bytes_were_counted(self):
        # The hub relays, so its merges are no update anyone sees, and merges
        # after tx:end ride on bytes nobody counted.
        run = h.Run(h.Config("test", "star", "loss0", "hlc", 0), 0, duration_ms=10_000, settle_ms_window=5000)

        def at(t, edge, hub):
            return poll(t, hub=state(t, {}, counters={"merge_applied": hub}),
                        **{n: state(t, {}, counters={"merge_applied": edge}) for n in h.EDGES})

        run.polls = [at(-5000, 0, 0), at(9900, 2, 6), at(12_000, 3, 9)]
        run.events = {"tx:start": 0, "tx:end": 10_000}
        run.tx = {"start": dict.fromkeys(run.nodes, 0), "end": dict.fromkeys(run.nodes, 1000)}
        with tempfile.TemporaryDirectory() as d:
            row = h.analyze(run, Path(d) / "polls.csv")
        self.assertEqual(row["wire_bytes_per_useful"], 666.7)  # 4000 bytes over the edges' 6 merges by tx:end
        self.assertEqual(row["merge_applied"], 18)  # the column itself still counts every node to the last poll

    def test_unequal_hashes_never_settle(self):
        last = self.exp.polls[-1].states
        last["c"] = dict(last["c"], hash="2")
        row = self.analyze()
        self.assertFalse(row["settled"])
        self.assertEqual(row["settle_ms"], "")

    def test_stale_final_value_counts_as_wrong(self):
        newer = T0 + 9000
        last = self.exp.polls[-1].states
        last["b"] = dict(last["b"], tracks={"AAAAAA": {"ts": {}, "origin": "b", "at": newer, "own": newer}})
        self.assertEqual(self.analyze()["wrong_final"], 1)  # a still holds the 5000 report


class RecoveryAllTest(unittest.TestCase):
    # (t, worst lag of any edge node on any aircraft); worst before the cut: 2000
    BEFORE = [(3000, 1000), (4000, 2000)]

    def test_must_hold_at_or_below_the_pre_cut_worst_for_the_sustain_window(self):
        after = [(8000, 9000), (8200, 2000), (8400, 3000), (8600, 2000), (9000, 1000), (10_600, 2000)]
        # 8200 dips to the floor but 8400 goes back above it; from 8600 it holds 2 s.
        self.assertEqual(h.recovery_all(self.BEFORE + after, cut=5000, heal=8000, end=12_000), 600)

    def test_holding_until_ingest_stops_counts(self):
        after = [(8000, 9000), (9500, 1000)]
        self.assertEqual(h.recovery_all(self.BEFORE + after, cut=5000, heal=8000, end=10_000), 1500)

    def test_never_recovers(self):
        after = [(8000, 9000), (10_000, 2500)]
        self.assertEqual(h.recovery_all(self.BEFORE + after, cut=5000, heal=8000, end=10_000), "")

    def test_needs_a_cut_and_a_heal(self):
        self.assertEqual(h.recovery_all(self.BEFORE, cut=5000, heal=None, end=10_000), "")
        self.assertEqual(h.recovery_all(self.BEFORE, cut=None, heal=8000, end=10_000), "")


class MergedDuringCutTest(unittest.TestCase):
    def test_counts_only_merges_while_the_cut_is_fully_in_force(self):
        cfg = h.Config("test", "star", "hubdown", "hlc", 0)
        run = h.Run(cfg, 0, duration_ms=20_000, settle_ms_window=0)
        run.events = {"cut": 8000, "cut_exec": 300, "heal": 16_000}

        def at(t, merged):
            return poll(t, **{n: state(t, {}, counters={"merge_applied": merged}) for n in h.EDGES})

        # 5 merges per node before the grace ends, 2 more inside, 9 more after the heal.
        run.polls = [at(0, 0), at(9000, 5), at(10_300, 5), at(15_900, 7), at(16_000, 16)]
        self.assertEqual(h.merged_during_cut(run), 6)
        run.events.pop("heal")
        self.assertEqual(h.merged_during_cut(run), "")


class StalenessTest(unittest.TestCase):
    def test_age_of_what_each_node_holds_while_the_aircraft_is_active(self):
        cfg = h.Config("test", "mesh", "loss0", "hlc", 0)
        run = h.Run(cfg, 0, duration_ms=10_000, settle_ms_window=0)
        r = T0 + 4000
        mine = {"AAAAAA": {"ts": {}, "origin": "a", "at": r, "own": r}}
        older = {"AAAAAA": {"ts": {}, "origin": "a", "at": T0 + 3000}}
        held = {"AAAAAA": {"ts": {}, "origin": "a", "at": r}}
        run.polls = [
            # a just reported it; b holds a report 1 s older; c holds nothing,
            # so it counts from the aircraft's first report
            poll(4000, a=state(4000, mine), b=state(4000, older), c=state(4000, {})),
            poll(4600, a=state(4600, mine), b=state(4600, held), c=state(4600, {})),
            # 5.5 s after the last report the aircraft no longer counts
            poll(9500, a=state(9500, mine), b=state(9500, held), c=state(9500, held)),
        ]
        with tempfile.TemporaryDirectory() as d:
            row = h.analyze(run, Path(d) / "polls.csv")
        # samples: 0, 1000, 0 at 4000 and 600, 600, 600 at 4600
        self.assertEqual(row["stale_p50"], 600)
        self.assertEqual(row["stale_p99"], 1000)

    def test_a_late_position_counts_while_its_report_is_fresh(self):
        # Live: a's report of a position observed at 1000 first shows at the
        # 7000 poll, 6 s late. It was received under 5 s ago, so the aircraft
        # counts, at the age of what each node holds: 6 s, then 6.5 s.
        run = h.Run(h.Config("live", "mesh", "loss0", "hlc", 0, "live"), 0, duration_ms=10_000, settle_ms_window=0)
        mine = {"AAAAAA": {"ts": {}, "origin": "a", "at": T0 + 1000, "own": T0 + 1000}}
        held = {"AAAAAA": {"ts": {}, "origin": "a", "at": T0 + 1000}}
        run.polls = [poll(7000, a=state(7000, mine), b=state(7000, {}), c=state(7000, {})),
                     poll(7500, a=state(7500, mine), b=state(7500, held), c=state(7500, held))]
        with tempfile.TemporaryDirectory() as d:
            row = h.analyze(run, Path(d) / "polls.csv")
        self.assertEqual((row["stale_p50"], row["stale_p99"]), (6000, 6500))


class DivergenceTest(unittest.TestCase):
    SAME = {"ts": {"w": 1, "l": 0}, "origin": "a", "at": 1}
    OTHER = {"ts": {"w": 2, "l": 0}, "origin": "b", "at": 2}

    def states(self, a, b, c):
        return {"a": {"tracks": a}, "b": {"tracks": b}, "c": {"tracks": c}}

    def test_fraction_of_aircraft_the_edges_disagree_on(self):
        s, o = self.SAME, self.OTHER
        self.assertEqual(h.divergence(self.states({"X": s, "Y": s}, {"X": s, "Y": o}, {"X": s, "Y": s})), 0.5)
        self.assertEqual(h.divergence(self.states({"X": s}, {"X": s}, {"X": s})), 0.0)

    def test_an_aircraft_one_edge_lacks_counts_as_divergent(self):
        s = self.SAME
        self.assertEqual(h.divergence(self.states({"X": s}, {"X": s}, {})), 1.0)

    def test_nothing_held_is_not_a_sample(self):
        self.assertIsNone(h.divergence(self.states({}, {}, {})))


class CatchupTest(unittest.TestCase):
    def run_with(self, polls):
        run = h.Run(h.Config("test", "mesh", "partition", "hlc", 0), 0, duration_ms=20_000, settle_ms_window=0)
        run.events = {"cut": 4000, "cut_exec": 100, "heal": 8000}
        run.polls = polls
        return h.catchup(run)

    def held(self, at, own=0):
        tr = {"ts": {}, "origin": "a", "at": T0 + at}
        if own:
            tr["own"] = T0 + own
        return {"XXXXXX": tr}

    def test_time_until_every_edge_holds_what_was_reported_by_the_heal(self):
        polls = [
            poll(7900, a=state(7900, self.held(7500, 7500)), b=state(7900, self.held(7000)), c=state(7900, self.held(7000))),
            poll(8000, a=state(8000, self.held(7500, 7500)), b=state(8000, self.held(7000)), c=state(8000, self.held(7500))),
            # a has moved on to a newer report, which the heal can't be blamed for
            poll(8300, a=state(8300, self.held(8200, 8200)), b=state(8300, self.held(7500)), c=state(8300, self.held(7500))),
        ]
        self.assertEqual(self.run_with(polls), 300)

    def test_never(self):
        polls = [
            poll(7900, a=state(7900, self.held(7500, 7500)), b=state(7900, self.held(7000)), c=state(7900, self.held(7000))),
            poll(9000, a=state(9000, self.held(7500, 7500)), b=state(9000, self.held(7000)), c=state(9000, self.held(7500))),
        ]
        self.assertEqual(self.run_with(polls), "")


class CatchupMergeTest(unittest.TestCase):
    def held(self, w, origin, at, own=0):
        tr = {"ts": {"w": w}, "origin": origin, "at": T0 + at}
        if own:
            tr["own"] = T0 + own
        return {"XXXXXX": tr}

    def test_a_later_write_of_an_older_observation_does_not_hold_it_up(self):
        # a reported an observation made at 7500; b then wrote one made at 7498,
        # later, so last-writer-wins keeps b's. Once every edge holds b's
        # nothing from before the heal is missing, though nothing observed at
        # 7500 is anywhere, so catchup_ms never completes.
        polls = [
            poll(7900, a=state(7900, self.held(7600, "a", 7500, 7500)), b=state(7900, self.held(7700, "b", 7498, 7498)),
                 c=state(7900, self.held(7600, "a", 7500))),
            poll(8000, a=state(8000, self.held(7600, "a", 7500, 7500)), b=state(8000, self.held(7700, "b", 7498, 7498)),
                 c=state(8000, self.held(7600, "a", 7500))),
            poll(8400, a=state(8400, self.held(7700, "b", 7498, 7500)), b=state(8400, self.held(7700, "b", 7498, 7498)),
                 c=state(8400, self.held(7700, "b", 7498))),
        ]
        run = h.Run(h.Config("test", "mesh", "partition", "hlc", 0), 0, duration_ms=20_000, settle_ms_window=0)
        run.events = {"cut": 4000, "cut_exec": 100, "heal": 8000}
        run.polls = polls
        self.assertEqual(h.catchup(run), "")
        self.assertEqual(h.catchup_merge(run), 400)


class LiveReceiptTest(unittest.TestCase):
    def test_live_reports_count_from_the_first_poll_that_shows_them(self):
        run = h.Run(h.Config("live", "mesh", "loss0", "hlc", 0, "live"), 0, duration_ms=10_000, settle_ms_window=0)
        mine = {"AAAAAA": {"ts": {}, "origin": "a", "at": T0 + 400, "own": T0 + 400}}  # observed at 400
        run.polls = [
            poll(1000, a=state(1000, {}), b=state(1000, {}), c=state(1000, {})),
            poll(1100, a=state(1100, mine), b=state(1100, {}), c=state(1100, {})),
            poll(1200, a=state(1200, mine), b=state(1200, {}), c=state(1200, {})),
        ]
        self.assertEqual(h.receipts(run, T0), ({("AAAAAA", T0 + 400): T0 + 1100}, [700]))
        # read later in a poll than the others: the receipt is the poll's first read, never after a receiver's
        late = [poll(1100, a=state(1103, mine), b=state(1100, {}), c=state(1101, {}))]
        run.polls = late
        self.assertEqual(h.receipts(run, T0)[0], {("AAAAAA", T0 + 400): T0 + 1100})
        run.polls = [poll(1000, a=state(1000, {}), b=state(1000, {}), c=state(1000, {})),
                     poll(1100, a=state(1100, mine), b=state(1100, {}), c=state(1100, {}))]
        synthetic = h.Run(h.Config("test", "mesh", "loss0", "hlc", 0), 0, duration_ms=10_000, settle_ms_window=0)
        synthetic.polls = run.polls
        self.assertEqual(h.receipts(synthetic, T0), ({}, []))


class AwsTest(unittest.TestCase):
    def run_with_hashes(self, edges, cloud):
        run = h.Run(h.Config("aws", "aws", "loss0", "hlc", 0, "live"), 0, 1000, 0)
        run.extra["cloud_check"] = {"tries": 2, "edges": dict(zip(h.EDGES, edges)), "cloud": {"hash": cloud, "n": 0}}
        cols = h.cloud_columns(run, 10)
        self.assertEqual(cols["cloud_check_tries"], 2)
        return cols["cloud_matches"]

    def test_the_deployed_peer_is_never_polled_and_must_end_equal(self):
        run = h.Run(h.Config("aws", "aws", "loss0", "hlc", 0, "live"), 0, 1000, 0)
        self.assertEqual(run.nodes, h.EDGES)
        self.assertEqual(h.peers("a", "aws"), "")
        self.assertIs(self.run_with_hashes(["x", "x", "x"], "x"), True)
        self.assertIs(self.run_with_hashes(["x", "x", "x"], "y"), False)
        self.assertIs(self.run_with_hashes(["x", "x", "z"], "x"), False)


class PollErrorTest(unittest.TestCase):
    def test_a_cut_off_or_garbled_poll_is_a_poll_error_not_the_end_of_the_sweep(self):
        # One stub answers for every node. After the three readiness reads,
        # its 5th answer is cut off mid-body and its 6th isn't JSON.
        served = itertools.count(1)

        class Node(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                body = json.dumps({"now_ms": h.now_ms(), "hash": "x", "counters": {}, "tracks": {}}).encode()
                length, body = {5: (len(body), body[:10]), 6: (6, b"<html>")}.get(next(served), (len(body), body))
                self.send_response(200)
                self.send_header("Content-Length", str(length))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Node)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        self.addCleanup(srv.server_close)
        self.addCleanup(srv.shutdown)
        run = h.Run(h.Config("test", "mesh", "loss0", "hlc", 0), 0, duration_ms=500, settle_ms_window=0)
        with mock.patch.object(h, "sh", lambda *args, env=None: "0"), mock.patch.object(h, "LEAD_MS", 0), \
                mock.patch.object(h, "PORT", dict.fromkeys(h.PORT, srv.server_address[1])):
            h.execute(run)
        self.assertEqual(len(run.errors), 2, run.errors)
        self.assertIn("IncompleteRead", run.errors[0])
        self.assertIn("Expecting value", run.errors[1])
        self.assertTrue(run.polls, "the run stopped at the bad polls")


class RunEnvTest(unittest.TestCase):
    def test_a_cloud_peer_left_in_the_shell_cannot_reach_other_runs(self):
        run = lambda topology: h.Run(h.Config("t", topology, "loss0", "hlc", 0), 0, 1000, 0)  # noqa: E731
        with mock.patch.dict(os.environ, {"CLOUD_PEER": "https://elsewhere.example", "CLOUD_TOKEN": "x" * 64}):
            mesh, cloud = h.run_env(run("mesh"), 0), h.run_env(run("cloud"), 0)
        self.assertEqual((mesh["CLOUD_PEER"], mesh["CLOUD_TOKEN"]), ("", ""))
        self.assertEqual(cloud["CLOUD_PEER"], f"http://{h.IP['cloud']}:8080")
        self.assertEqual(len(cloud["CLOUD_TOKEN"]), 64)
        self.assertNotEqual(cloud["CLOUD_TOKEN"], "x" * 64)


class TraceStatsTest(unittest.TestCase):
    def rec(self, t, node, delay=0):
        return {"t_ms": t, "icao": "AAAAAA", "node": node, "delay_ms": delay}

    def test_a_fast_clock_costs_the_fresher_reports_that_follow_it_closely(self):
        recs = [self.rec(0, "c"), self.rec(3000, "a"),                   # 3 s after c and fresher: lost at +4 s only
                self.rec(10_000, "c"), self.rec(13_000, "a", delay=4000),  # 3 s after, but observed before c's
                self.rec(20_000, "c"), self.rec(25_000, "a")]              # 5 s after: wins even at +4 s
        self.assertEqual(ts.expected_wall_losses(recs, 0, 30_000, 4000), 1)
        self.assertEqual(ts.expected_wall_losses(recs, 0, 30_000, 2000), 0)

    def test_a_slow_clock_costs_its_own_reports_that_follow_another_closely(self):
        recs = [self.rec(0, "a"), self.rec(3000, "c"), self.rec(10_000, "a"), self.rec(16_000, "c")]
        self.assertEqual(ts.expected_wall_losses(recs, 0, 20_000, -4000), 1)
        self.assertEqual(ts.expected_wall_losses(recs, 0, 20_000, 4000), 0)

    def test_late_reports(self):
        recs = [self.rec(0, "a"), self.rec(1000, "b", delay=2000), self.rec(2000, "c")]
        self.assertEqual(ts.late_reports(recs, 0, 3000), 1)  # b's, observed at -1000, after a's at 0
        # a run replaying [1000, 3000) starts empty, so a's receipt at 0 is nothing b's can be late against
        self.assertEqual(ts.late_reports(recs, 1000, 3000), 0)

    def test_handoffs_count_the_next_report_after_c_by_another_node(self):
        recs = [self.rec(0, "c"), self.rec(1000, "c"), self.rec(2000, "a"), self.rec(3000, "c"), self.rec(4000, "b")]
        self.assertEqual(ts.handoffs_from(recs, 0, 5000), 2)


class CommitLabelTest(unittest.TestCase):
    def test_only_source_changes_make_a_checkout_dirty(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            h.sh("git", "-C", d, "init", "-q")
            (root / "results" / "sweep").mkdir(parents=True)
            (root / "results" / "sweep" / "summary.csv").write_text("run_id\n")
            self.assertFalse(h.dirty(root), "a sweep's own results marked the checkout dirty")
            (root / "new.go").write_text("package x\n")
            self.assertTrue(h.dirty(root), "an untracked source file went unnoticed")


class RecoveryP95Test(unittest.TestCase):
    BEFORE = [(3000, 1000), (4000, 2000)]

    def test_first_poll_back_at_the_pre_cut_worst(self):
        after = [(8000, 5000), (8300, 2000), (8500, 9000)]
        self.assertEqual(h.recovery_p95(self.BEFORE + after, cut=5000, heal=8000), 300)

    def test_never(self):
        self.assertEqual(h.recovery_p95(self.BEFORE + [(8000, 5000)], cut=5000, heal=8000), "")

    def test_short_runs_still_get_a_pre_cut_window(self):
        # A cut before the warm-up ends uses the half of the time before the cut.
        self.assertEqual(h.recovery_p95([(1500, 1000), (8000, 900)], cut=2800, heal=8000), 0)


class LagTest(unittest.TestCase):
    def test_lag_and_behind(self):
        r1, r2 = T0 + 4000, T0 + 8000
        cfg = h.Config("test", "mesh", "loss0", "hlc", 0)
        exp = h.Run(cfg, 0, duration_ms=10_000, settle_ms_window=0)
        held = lambda r: {"AAAAAA": {"ts": {}, "origin": "a", "at": r}}  # noqa: E731
        mine = lambda r: {"AAAAAA": {"ts": {}, "origin": "a", "at": r, "own": r}}  # noqa: E731
        exp.polls = [
            poll(4000, a=state(4000, mine(r1)), b=state(4000, held(r1)), c=state(4000, held(r1))),
            # a has made a newer report; b is 4 s behind it, c has caught up
            poll(8000, a=state(8000, mine(r2)), b=state(8000, held(r1)), c=state(8000, held(r2))),
            # after ingest stops: b 6 s behind, which lag_worst must not count
            poll(10_400, a=state(10_400, mine(T0 + 10_000)), b=state(10_400, held(T0 + 4000)),
                 c=state(10_400, held(T0 + 10_000))),
        ]
        with tempfile.TemporaryDirectory() as d:
            row = h.analyze(exp, Path(d) / "polls.csv")
            with (Path(d) / "polls.csv").open() as f:
                b_last = [line for line in f if line.startswith("8000,b,")][0].strip().split(",")
        self.assertEqual(row["lag_p95"], 4000)  # six samples, one of them 4000
        self.assertEqual(row["lag_worst"], 4000)
        self.assertAlmostEqual(row["behind_frac"], round((0 / 3 + 1 / 3) / 2, 4))
        self.assertEqual(b_last[-2:], ["4000", "1"])  # b's worst lag, and one aircraft over 2 s behind


class RawRoundTripTest(unittest.TestCase):
    def test_reloaded_raw_gives_the_same_summary(self):
        r = T0 + 5000
        mine = {"AAAAAA": {"ts": {"w": 1, "l": 0}, "origin": "a", "at": r, "own": r}}
        cfg = h.Config("test", "mesh", "partition", "hlc", 0)
        exp = h.Run(cfg, 2, duration_ms=10_000, settle_ms_window=5000, commit="abc1234")
        exp.polls = [poll(t, a=state(t, mine, "1"), b=state(t, mine, "1"), c=state(t, mine, "1"))
                     for t in (0, 4000, 9000, 10_400)]
        exp.events = {"tx:start": 0, "cut": 4000, "heal": 8000, "tx:end": 10_000}
        exp.tx = {"start": {"a": 10, "b": 10, "c": 10}, "end": {"a": 110, "b": 210, "c": 310}}
        with tempfile.TemporaryDirectory() as d:
            raw = Path(d) / "raw" / "x.json.gz"
            h.save_raw(exp, raw)
            again = h.load_raw(raw)
            self.assertEqual(h.analyze(exp, Path(d) / "p1.csv"), h.analyze(again, Path(d) / "p2.csv"))
            self.assertEqual(again.cfg, cfg)


if __name__ == "__main__":
    unittest.main()


class ReceiptTest(unittest.TestCase):
    def test_first_receipt_of_each_observation_in_the_window(self):
        records = [
            (1000, "AAAAAA", 400),  # observed at 600, received at 1000
            (1500, "AAAAAA", 900),  # the same observation, a second sensor 500 ms later
            (1600, "BBBBBB", 0),
            (900, "CCCCCC", 0),     # before the window
            (61_000, "AAAAAA", 0),  # after it
        ]
        first, delays = h.receipts_in(records, 1000, 60_000, T0)
        self.assertEqual(first, {("AAAAAA", T0 - 400): T0, ("BBBBBB", T0 + 600): T0 + 600})
        self.assertEqual(delays, [0, 400, 900])

    def test_windows_are_per_family(self):
        cfg = lambda f: h.Config(f, "mesh", "loss0", "hlc", 0)  # noqa: E731
        self.assertEqual(h.window_ms(cfg("real-hour"), 3), 3 * h.REAL_HOUR_S * 1000)
        self.assertEqual(h.window_ms(cfg("real-base"), 2), 900_000)
        self.assertEqual(h.window_ms(cfg("sync"), 4), 0)
        last = h.window_ms(cfg("real-hour"), 59) + h.REAL_HOUR_S * 1000
        self.assertLessEqual(last, 3_582_000, "the last real-hour window runs past the recording")

    def test_cloud_columns(self):
        run = h.Run(h.Config("cloud", "cloud", "loss0", "hlc", 0), 0, 1000, 0)
        self.assertEqual(h.cloud_columns(run, 10)["cloud_writes"], "")
        run.extra["peer"] = {"requests": 4, "writes": 12, "won": 11, "failed": 1}
        cols = h.cloud_columns(run, 10)
        self.assertEqual((cols["cloud_writes_per_report"], cols["cloud_won"], cols["cloud_failed"]), (1.2, 11, 1))


class SourceTest(unittest.TestCase):
    def test_sources_map_to_node_flags(self):
        self.assertEqual(h.source_flag("seed1"), "-trace=/traces/seed1.jsonl")
        self.assertEqual(h.source_flag("live"), f"-live-url={h.FEED}")

    def test_a_trace_must_be_a_file_under_traces(self):
        for bad in ("../harness/run.py", "nope.jsonl", "sub/dir.jsonl"):
            with self.assertRaises(RuntimeError):
                h.source_flag(bad)
