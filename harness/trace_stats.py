"""Facts about a recorded trace, computed from the trace alone with no run
involved: the ones docs/predictions-2.md and the README rest on, and what a
skewed wall clock must lose, so the harness's measurements can be checked
against a model of the mechanism.

    python -I harness/trace_stats.py traces/adsb-2026-11-05.jsonl.gz
"""

from __future__ import annotations

import gzip
import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # python -I leaves the script's folder off the path
import run as h


def windows(family: str, n: int, length_ms: int) -> list[tuple[int, int]]:
    """[lo, hi) in the trace of a family's first n windows, as run.py replays them."""
    cfg = h.Config(family, "mesh", "loss0", "hlc", 0)
    return [(h.window_ms(cfg, w), h.window_ms(cfg, w) + length_ms) for w in range(n)]


def load(path: Path) -> list[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        return sorted((json.loads(line) for line in f if line.strip()), key=lambda r: r["t_ms"])


def observed(r: dict) -> int:
    return r["t_ms"] - r.get("delay_ms", 0)


def late_reports(recs: list[dict], lo: int, hi: int) -> int:
    """Reports received in [lo, hi) after some node had already received a
    fresher observation of the same aircraft in [lo, hi): what a run that
    replays the window, starting empty, can see."""
    seen: dict[str, int] = {}
    late = 0
    for r in recs:
        if lo <= r["t_ms"] < hi:
            best = seen.get(r["icao"])
            if best is not None and best > observed(r):
                late += 1
            seen[r["icao"]] = observed(r) if best is None else max(best, observed(r))
    return late


def handoffs_from(recs: list[dict], lo: int, hi: int, node: str = "c") -> int:
    """Times in [lo, hi) that the next report of an aircraft after one by
    `node` came from a different node."""
    last: dict[str, str] = {}
    n = 0
    for r in recs:
        if lo <= r["t_ms"] < hi:
            n += last.get(r["icao"]) == node and r["node"] != node
            last[r["icao"]] = r["node"]
    return n


def expected_wall_losses(recs: list[dict], lo: int, hi: int, skew_ms: int, skewed: str = "c") -> int:
    """Writes a wall-clock node must lose in [lo, hi) when node `skewed` runs
    skew_ms off true time. A write loses when the value it meets carries a
    later stamp yet an older observation. With the skewed node fast, that is a
    report by another node less than skew_ms after the skewed node's report of
    the same aircraft, and fresher than it; with it slow, a report by the
    skewed node less than |skew_ms| after another node's, and fresher. Every
    node replays at true time, and every report reaches the others before the
    next one (the trace's closest handoff is 2.37 s, sync takes well under 1 s)."""
    by_ac: dict[str, list[dict]] = defaultdict(list)
    for r in recs:
        if lo <= r["t_ms"] < hi:
            by_ac[r["icao"]].append(r)
    lost = 0
    for rs in by_ac.values():
        last = {True: None, False: None}  # the latest report by the skewed node, and by any other
        for r in rs:
            mine = r["node"] == skewed
            other = last[not mine]
            if other is not None:
                gap = r["t_ms"] - other["t_ms"]
                ahead = skew_ms if not mine else -skew_ms  # how far the stamp it meets runs ahead of its own
                if gap < ahead and observed(r) > observed(other):
                    lost += 1
                    continue  # it lost, so the value it met is still the one held
            last[mine] = r
    return lost


def facts(recs: list[dict]) -> None:
    span = recs[-1]["t_ms"]
    minutes = defaultdict(set)
    nodes_of = defaultdict(set)
    for r in recs:
        minutes[r["t_ms"] // 60_000].add(r["icao"])
        nodes_of[r["icao"]].add(r["node"])
    full = [len(minutes[m]) for m in range(span // 60_000)]
    multi = sum(len(ns) >= 2 for ns in nodes_of.values())
    print(f"{len(recs)} reports, {len(nodes_of)} aircraft, {multi} ({multi / len(nodes_of):.0%}) seen by two or more sensors")
    print(f"aircraft per full minute: median {statistics.median(full)}, {min(full)} to {max(full)}")
    d = sorted(r.get("delay_ms", 0) for r in recs)
    print(f"feed delay: median {statistics.median(d) / 1000:.2f} s, p99 {d[int(0.99 * len(d))] / 1000:.2f} s")
    for n in sorted({r["node"] for r in recs}):
        ts = sorted({r["t_ms"] for r in recs if r["node"] == n})
        gaps = sorted(b - a for a, b in zip(ts, ts[1:]))
        print(f"sensor {n}: {len(ts)} polls, gap median {statistics.median(gaps) / 1000:.1f} s, "
              f"mean {statistics.mean(gaps) / 1000:.1f} s, max {gaps[-1] / 1000:.1f} s")
    print(f"late reports: {late_reports(recs, 0, span + 1)} of {len(recs) - len(nodes_of)} after each aircraft's first")
    quiet = quiet_share(recs, 6000)
    print(f"time in gaps over 6 s with no report from any sensor: {quiet:.0%} (about {60 * quiet:.0f} s a minute)")
    old = [r for r in recs if r.get("delay_ms", 0) > 20_000]
    slow = sum(r.get("alt") == 0 or (r.get("gs") or 0) < 40 for r in old)
    print(f"reports over 20 s old: {len(old)}, {slow / len(old):.0%} of them on the ground or under 40 kt")
    print("closest handoffs, one sensor's report of an aircraft after another's: " +
          ", ".join(f"{k} {v / 1000:.2f} s" for k, v in sorted(min_handoffs(recs).items())))


def quiet_share(recs: list[dict], over_ms: int) -> float:
    """Share of the trace's span spent in gaps longer than over_ms with no report at all."""
    span = recs[-1]["t_ms"]
    ts = sorted({r["t_ms"] for r in recs})
    return sum(g for g in (b - a for a, b in zip([0, *ts], ts)) if g > over_ms) / span


def min_handoffs(recs: list[dict]) -> dict[str, int]:
    """The shortest time, per ordered pair of sensors, from one's report of an
    aircraft to the other's next one: below a wall clock's skew, a handoff can lose."""
    last: dict[str, dict] = {}
    best: dict[str, int] = {}
    for r in recs:
        p = last.get(r["icao"])
        if p is not None and p["node"] != r["node"]:
            k = f"{p['node']}->{r['node']}"
            best[k] = min(best.get(k, r["t_ms"] - p["t_ms"]), r["t_ms"] - p["t_ms"])
        last[r["icao"]] = r
    return best


def main() -> int:
    if len(sys.argv) != 2 or sys.argv[1].startswith("-"):
        print(__doc__.strip())
        return 2
    recs = load(Path(sys.argv[1]))
    facts(recs)
    late = [late_reports(recs, lo, hi) for lo, hi in windows("real-hour", 60, h.REAL_HOUR_S * 1000)]
    print(f"\nreal-hour windows: late reports in {sum(n > 0 for n in late)} of {len(late)}, {sum(late)} in all")
    # A run starts empty, so it sees a window's aircraft; a node running all hour
    # would hold everything reported in the last TTL (5 min).
    seen = [len({r["icao"] for r in recs if lo <= r["t_ms"] < hi})
            for lo, hi in windows("real-hour", 60, h.REAL_HOUR_S * 1000)]
    held = [len({r["icao"] for r in recs if hi - 300_000 <= r["t_ms"] < hi})
            for _, hi in windows("real-hour", 60, h.REAL_HOUR_S * 1000) if hi >= 300_000]
    print(f"aircraft a window sees: median {statistics.median(seen)}; a node's full picture at each window's end, "
          f"past the first 5 min: median {statistics.median(held)}, {min(held)} to {max(held)}")
    print("\nreal-base windows: late reports, and the wall clock's expected lost writes by c's skew")
    for lo, hi in windows("real-base", 8, h.INGEST_S * 1000):
        losses = {k: expected_wall_losses(recs, lo, hi, k) for k in (-4000, -2000, 2000, 4000)}
        print(f"  from {lo // 1000:4d} s: late {late_reports(recs, lo, hi)}, handoffs from c {handoffs_from(recs, lo, hi)}, lost " +
              ", ".join(f"{k:+d} ms: {v}" for k, v in losses.items()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
