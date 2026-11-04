"""Run holdfast experiments in Docker and write the measurements.

    python harness/run.py --smoke                 # two short runs; exits 1 unless both converge
    python harness/run.py --family sync --repeats 5
    python harness/run.py --family clock --repeats 5

Each run starts fresh containers, replays the same trace on every edge node,
impairs the mesh link with tc netem on a schedule, and polls every node's
/state about every 100 ms on a control network netem never touches.

Outputs, under --out (default results/<utc stamp>-<family>):
    summary.csv        one row per run
    polls/<run>.csv    per poll, per node: picture size, hash, lag percentiles

Definitions used throughout:
    truth(aircraft)    the freshest report of it that any edge node has made itself
    lag(node, ac)      truth(ac) minus the observation time of what the node holds
                       for it; if the node holds nothing, minus the first time any
                       node reported it
    prop               propagation latency: for each report and each edge node that did
                       not make it, time until the node holds it or something fresher
    settle_ms          after ingest stops, time until every node's hash is equal
    recover_ms         after a partition heals, time until the worst per-poll p95 lag
                       is back at or below its worst value before the partition
Only edge nodes count toward lag; the star hub is a relay and has no coverage.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import math
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ROOT / "harness" / "compose.yml"
IMAGE = "holdfast:dev"

IP = {"hub": "172.30.0.10", "a": "172.30.0.11", "b": "172.30.0.12", "c": "172.30.0.13"}
PORT = {"a": 8081, "b": 8082, "c": 8083, "hub": 8084}
EDGES = ("a", "b", "c")

POLL_MS = 100
BEHIND_MS = 2000  # two sync intervals
WARMUP_MS = 3000  # ignore the first seconds while every node is still filling up

# Static impairments applied to every node for the whole run.
LOSS = {
    "loss0": None,
    "loss20": ["loss", "20%"],
    "loss50": ["loss", "50%"],
    # Gilbert-Elliott: 10% chance to enter the bad state, 40% to leave it, all
    # packets lost while bad. Stationary loss 0.1 / (0.1 + 0.4) = 20%, same as
    # loss20, but in bursts averaging 2.5 packets.
    "burst20": ["loss", "gemodel", "10%", "40%"],
}


@dataclass
class Config:
    family: str
    topology: str  # mesh: edges peer with each other; star: edges peer only with the hub
    scenario: str  # a LOSS key, "partition" (cut c), or "hubdown" (cut the hub; star only)
    clock: str  # hlc or wall
    skew_ms: int  # applied to node c's physical clock

    def run_id(self, repeat: int) -> str:
        return f"{self.family}-{self.topology}-{self.scenario}-{self.clock}-skew{self.skew_ms}-r{repeat}"


def experiments(family: str) -> list[Config]:
    sync = [Config("sync", t, s, "hlc", 0) for t in ("mesh", "star")
            for s in ("loss0", "loss20", "loss50", "burst20", "partition")]
    sync.append(Config("sync", "star", "hubdown", "hlc", 0))
    # Skews below and above the 1 s report interval. Prediction, written before
    # the first run: the wall clock only mis-orders reports when the skew is
    # larger than the interval between successive reports of an aircraft.
    clock = [Config("clock", "mesh", "loss0", c, k) for c in ("hlc", "wall") for k in (0, -500, -2000, 2000)]
    return {"sync": sync, "clock": clock, "all": sync + clock}[family]


SMOKE = [Config("smoke", "mesh", "loss0", "hlc", 0), Config("smoke", "mesh", "partition", "hlc", 0)]


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def sh(*args: str, env: dict | None = None) -> str:
    p = subprocess.run(args, env=env, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"{' '.join(args)} exited {p.returncode}: {p.stderr.strip() or p.stdout.strip()}")
    return p.stdout


def compose(env: dict, *args: str) -> None:
    sh("docker", "compose", "-f", str(COMPOSE), "--profile", "star", *args, env=env)


def netem(node: str, args: list[str]) -> None:
    out = sh("docker", "exec", "-u", "0", f"hf-{node}", "netem", *args)
    active = "netem" in out  # netem.sh prints the qdisc it left in place
    if active == (args == ["clear"]):
        raise RuntimeError(f"netem {args} on {node} did not take effect: {out.strip()}")


def get_state(node: str) -> dict:
    with urllib.request.urlopen(f"http://127.0.0.1:{PORT[node]}/state", timeout=2) as r:
        return json.load(r)


def pct(sorted_vals: list[float], q: float) -> float:
    """Nearest-rank percentile of an already sorted list."""
    if not sorted_vals:
        return float("nan")
    k = max(0, math.ceil(q / 100 * len(sorted_vals)) - 1)
    return sorted_vals[k]


@dataclass
class Poll:
    t: int  # ms since t0, VM clock
    states: dict[str, dict]


@dataclass
class Run:
    cfg: Config
    repeat: int
    duration_ms: int
    settle_ms_window: int
    heal: bool = True
    polls: list[Poll] = field(default_factory=list)
    events: dict[str, int] = field(default_factory=dict)  # name -> ms since t0, VM clock
    vm_offset_ms: int = 0  # VM clock minus host clock
    errors: list[str] = field(default_factory=list)

    @property
    def nodes(self) -> tuple[str, ...]:
        return EDGES + (("hub",) if self.cfg.topology == "star" else ())

    @property
    def part_start(self) -> int:
        return int(self.duration_ms * 0.4)

    @property
    def part_end(self) -> int:
        return int(self.duration_ms * 0.8)


def peers(node: str, topology: str) -> str:
    if node == "hub":
        return ""
    others = [n for n in EDGES if n != node] if topology == "mesh" else ["hub"]
    return ",".join(f"http://{IP[n]}:8080" for n in others)


def execute(run: Run) -> None:
    cfg = run.cfg
    t0 = now_ms() + 8000
    env = os.environ | {
        "T0": str(t0),
        "CLOCK": cfg.clock,
        "TRACE_MS": str(run.duration_ms),
        "C_OFFSET": str(cfg.skew_ms),
        **{f"{n.upper()}_PEERS": peers(n, cfg.topology) for n in EDGES},
    }
    services = list(run.nodes)
    compose(env, "down", "-t", "2", "--remove-orphans")
    try:
        sh("docker", "compose", "-f", str(COMPOSE), *(["--profile", "star"] if cfg.topology == "star" else []),
           "up", "-d", "--force-recreate", *services, env=env)

        # Wait until every node answers, and measure the VM clock against ours.
        offsets = []
        while True:
            try:
                for n in run.nodes:
                    before = now_ms()
                    st = get_state(n)
                    offsets.append(st["now_ms"] - (before + now_ms()) // 2)
                break
            except (OSError, urllib.error.URLError):
                if now_ms() > t0 - 2000:
                    raise RuntimeError("nodes not ready 2 s before the trace starts")
                time.sleep(0.2)
        run.vm_offset_ms = sorted(offsets)[len(offsets) // 2]

        if LOSS.get(cfg.scenario):
            for n in run.nodes:
                netem(n, LOSS[cfg.scenario])

        target = "hub" if cfg.scenario == "hubdown" else "c"
        schedule = []
        if cfg.scenario in ("partition", "hubdown"):
            schedule.append(("cut", run.part_start, target, ["loss", "100%"]))
            if run.heal:
                schedule.append(("heal", run.part_end, target, ["clear"]))

        def fire(name: str, node: str, args: list[str]) -> None:
            try:
                netem(node, args)
                run.events[name] = now_ms() + run.vm_offset_ms - t0
            except RuntimeError as e:
                run.errors.append(str(e))

        threads = []
        end = run.duration_ms + run.settle_ms_window
        next_poll = now_ms()
        while True:
            host_rel = now_ms() + run.vm_offset_ms - t0
            if host_rel > end:
                break
            while schedule and host_rel >= schedule[0][1]:
                name, _, node, args = schedule.pop(0)
                th = threading.Thread(target=fire, args=(name, node, args))
                th.start()
                threads.append(th)
            try:
                states = {n: get_state(n) for n in run.nodes}
                run.polls.append(Poll(states[run.nodes[0]]["now_ms"] - t0, states))
            except (OSError, urllib.error.URLError) as e:
                run.errors.append(f"poll at {host_rel} ms: {e}")
            next_poll += POLL_MS
            time.sleep(max(0, next_poll - now_ms()) / 1000)
        for th in threads:
            th.join()
    finally:
        compose(env, "down", "-t", "2", "--remove-orphans")


def propagation(run: Run) -> list[float]:
    """Propagation latency, sorted: for every report made during the ingest
    window and every edge node that did not make that report itself, the time
    from the report until the node first holds it or something fresher.
    Resolution is the poll interval."""
    reports: dict[str, set[int]] = {}
    made: dict[tuple[str, str], set[int]] = {}
    for p in run.polls:
        for n in EDGES:
            for ac, tr in p.states[n]["tracks"].items():
                own = tr.get("own", 0)
                if own:
                    reports.setdefault(ac, set()).add(own)
                    made.setdefault((n, ac), set()).add(own)
    t0_abs = run.polls[0].states[run.nodes[0]]["now_ms"] - run.polls[0].t if run.polls else 0
    lo, hi = t0_abs + WARMUP_MS, t0_abs + run.duration_ms
    pending = {(n, ac): sorted(r for r in rs if lo <= r <= hi and r not in made.get((n, ac), set()))
               for ac, rs in reports.items() for n in EDGES}
    out: list[float] = []
    for p in run.polls:
        for n in EDGES:
            now = p.states[n]["now_ms"]
            for ac, tr in p.states[n]["tracks"].items():
                q = pending.get((n, ac))
                while q and q[0] <= tr["at"]:
                    out.append(now - q.pop(0))
    out.sort()
    return out


def analyze(run: Run, polls_path: Path) -> dict:
    first_seen: dict[str, int] = {}
    samples: list[float] = []
    behind_fracs: list[float] = []
    p95_by_poll: list[tuple[int, float]] = []
    settled_at = None
    polls_path.parent.mkdir(parents=True, exist_ok=True)
    with polls_path.open("w", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(["t_ms", "node", "n_tracks", "hash", "aircraft", "lag_p50", "lag_p95", "lag_max", "behind"])
        for p in run.polls:
            truth: dict[str, int] = {}
            for n in EDGES:
                for ac, tr in p.states[n]["tracks"].items():
                    own: int = tr.get("own", 0)
                    if own:
                        truth[ac] = max(truth.get(ac, 0), own)
                        first_seen[ac] = min(first_seen.get(ac, own), own)
            poll_lags: list[float] = []
            behind = 0
            for n in run.nodes:
                tracks = p.states[n]["tracks"]
                lags = []
                if n in EDGES:
                    for ac, fresh in truth.items():
                        have = tracks[ac]["at"] if ac in tracks else first_seen[ac]
                        lags.append(max(0, fresh - have))
                    lags.sort()
                    poll_lags.extend(lags)
                    behind += sum(1 for x in lags if x > BEHIND_MS)
                w.writerow([p.t, n, len(tracks), p.states[n]["hash"][:12], len(truth),
                            pct(lags, 50) if lags else "", pct(lags, 95) if lags else "",
                            lags[-1] if lags else "", sum(1 for x in lags if x > BEHIND_MS) if lags else ""])
            in_window = WARMUP_MS <= p.t <= run.duration_ms
            if poll_lags:
                poll_lags.sort()
                p95_by_poll.append((p.t, pct(poll_lags, 95)))
                if in_window:
                    samples.extend(poll_lags)
                    behind_fracs.append(behind / len(poll_lags))
            if settled_at is None and p.t >= run.duration_ms and len({p.states[n]["hash"] for n in run.nodes}) == 1:
                settled_at = p.t

    samples.sort()
    prop = propagation(run)
    final = run.polls[-1].states if run.polls else {}
    first = run.polls[0].states if run.polls else {}
    # Counters from the first poll on, so sync attempts made while the other
    # containers were still starting don't count as errors.
    totals: dict[str, int] = {}
    for n in run.nodes:
        start = first.get(n, {}).get("counters", {})
        for k, v in final.get(n, {}).get("counters", {}).items():
            totals[k] = totals.get(k, 0) + v - start.get(k, 0)
    own_by: dict[str, int] = {}
    truth_end: dict[str, int] = {}
    for n in EDGES:
        for ac, tr in final.get(n, {}).get("tracks", {}).items():
            if tr.get("own"):
                own_by[ac] = own_by.get(ac, 0) + 1
                truth_end[ac] = max(truth_end.get(ac, 0), tr["own"])
    ref = final.get("a", {}).get("tracks", {})
    wrong_final = sum(1 for ac, fresh in truth_end.items() if ac not in ref or ref[ac]["at"] < fresh)

    recover = ""
    heal_t = run.events.get("heal")
    if heal_t is not None:
        floor = max((v for t, v in p95_by_poll if WARMUP_MS <= t < run.events.get("cut", run.part_start)), default=None)
        after = [t for t, v in p95_by_poll if t >= heal_t and floor is not None and v <= floor]
        recover = after[0] - heal_t if after else ""

    gaps = [b.t - a.t for a, b in zip(run.polls, run.polls[1:])]
    ingest = totals.get("ingest_total", 0)
    sync_total = totals.get("sync_ok", 0) + totals.get("sync_err", 0)
    c = run.cfg
    return {
        "run_id": c.run_id(run.repeat), "family": c.family, "topology": c.topology, "scenario": c.scenario,
        "clock": c.clock, "skew_ms": c.skew_ms, "repeat": run.repeat, "duration_ms": run.duration_ms,
        "settled": settled_at is not None,
        "settle_ms": "" if settled_at is None else settled_at - run.duration_ms,
        "recover_ms": recover,
        "cut_at_ms": run.events.get("cut", ""), "heal_at_ms": run.events.get("heal", ""),
        "prop_p50": pct(prop, 50), "prop_p95": pct(prop, 95), "prop_p99": pct(prop, 99), "prop_n": len(prop),
        "lag_p95": pct(samples, 95),
        "behind_frac": round(sum(behind_fracs) / len(behind_fracs), 4) if behind_fracs else "",
        "aircraft": len(own_by), "overlap_frac": round(sum(1 for v in own_by.values() if v >= 2) / len(own_by), 3) if own_by else "",
        "ingest_total": ingest, "ingest_lost": totals.get("ingest_lost", 0),
        "merge_applied": totals.get("merge_applied", 0), "merge_regress": totals.get("merge_regress", 0),
        "wrong_final": wrong_final,
        "sync_ok": totals.get("sync_ok", 0), "sync_err": totals.get("sync_err", 0),
        "sync_err_rate": round(totals.get("sync_err", 0) / sync_total, 4) if sync_total else "",
        "rejected": totals.get("rejected", 0),
        "bytes_total": totals.get("bytes_out", 0) + totals.get("bytes_in", 0),
        "bytes_per_obs": round((totals.get("bytes_out", 0) + totals.get("bytes_in", 0)) / ingest, 1) if ingest else "",
        "polls": len(run.polls), "poll_ms_mean": round(sum(gaps) / len(gaps), 1) if gaps else "",
        "vm_offset_ms": run.vm_offset_ms, "errors": " | ".join(run.errors),
    }


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--smoke", action="store_true", help="two short runs; exit 1 unless both converge")
    ap.add_argument("--family", choices=("sync", "clock", "all"))
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--duration", type=int, help="seconds of ingest (default 50, smoke 20)")
    ap.add_argument("--settle", type=int, default=10, help="seconds to keep polling after ingest stops")
    ap.add_argument("--out", type=Path)
    ap.add_argument("--no-heal", action="store_true",
                    help="never heal partitions; exists to prove the smoke gate fails when nodes stay apart")
    ap.add_argument("--skip-build", action="store_true")
    args = ap.parse_args()
    if args.smoke == bool(args.family):
        ap.error("give exactly one of --smoke or --family")

    configs = SMOKE if args.smoke else experiments(args.family)
    repeats = 1 if args.smoke else args.repeats
    duration_ms = (args.duration or (20 if args.smoke else 50)) * 1000
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = args.out or ROOT / "results" / f"{stamp}-{'smoke' if args.smoke else args.family}"
    out.mkdir(parents=True, exist_ok=True)

    if not args.skip_build:
        print(f"building {IMAGE}", flush=True)
        sh("docker", "build", "-q", "-t", IMAGE, "-f", str(ROOT / "harness" / "Dockerfile"), str(ROOT))

    summary_path = out / "summary.csv"
    failures = []
    total = len(configs) * repeats
    i = 0
    writer = None
    with summary_path.open("w", newline="") as f:
        for repeat in range(repeats):
            for cfg in configs:
                i += 1
                run = Run(cfg, repeat, duration_ms, args.settle * 1000, heal=not args.no_heal)
                started = time.time()
                try:
                    execute(run)
                except RuntimeError as e:
                    run.errors.append(str(e))
                row = analyze(run, out / "polls" / f"{cfg.run_id(repeat)}.csv")
                if writer is None:
                    writer = csv.DictWriter(f, fieldnames=list(row), lineterminator="\n")
                    writer.writeheader()
                writer.writerow(row)
                f.flush()
                print(f"[{i}/{total}] {row['run_id']}: settled={row['settled']} settle_ms={row['settle_ms']} "
                      f"recover_ms={row['recover_ms']} prop_p50/p95={row['prop_p50']}/{row['prop_p95']} "
                      f"ingest_lost={row['ingest_lost']} "
                      f"regress={row['merge_regress']} err_rate={row['sync_err_rate']} "
                      f"({time.time() - started:.0f}s){' ERRORS: ' + row['errors'] if row['errors'] else ''}",
                      flush=True)
                if not row["settled"] or row["errors"] or row["rejected"]:
                    failures.append(row["run_id"])
                if cfg.scenario in ("partition", "hubdown") and not args.no_heal and row["recover_ms"] == "":
                    failures.append(row["run_id"] + " (never recovered)")

    print(f"wrote {summary_path}")
    if failures:
        print("FAILED: " + ", ".join(failures))
        return 1 if args.smoke else 0  # in a full sweep a failed run is data, not a crash
    return 0


if __name__ == "__main__":
    sys.exit(main())
