"""Checks the harness's own arithmetic on hand-built polls, so a wrong
formula can't quietly produce plausible numbers.

    python -m unittest discover -s harness
"""

import tempfile
import unittest
from pathlib import Path

import run as h

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
        c1 = {"sync_ok": 30, "sync_err": 4, "ingest_total": 9}
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
        self.assertEqual(h.propagation(self.exp), [300, 600])

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


if __name__ == "__main__":
    unittest.main()
