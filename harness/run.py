"""Run holdfast experiments in Docker and write the measurements.

    python harness/run.py --smoke                 # three short runs; exits 1 unless all converge
    python harness/run.py --family sync --repeats 5
    python harness/run.py --family clock --repeats 5
    python harness/run.py --reanalyze results/<dir>   # recompute summary.csv from raw/
    python harness/run.py --smoke --obs           # same, with Prometheus and Grafana watching
    python harness/run.py --family real-hour --repeats 60   # the recorded hour, every minute of it
    python harness/run.py --family real-base --repeats 8    # baselines on 8 windows of it
    python harness/run.py --family cloud --repeats 5        # the cloud peer as the star's hub

Each run starts fresh containers, replays the same trace on every edge node,
impairs the mesh link with tc netem on a schedule, and polls every node's
/state about every 100 ms on a control network netem never touches.

Outputs, under --out (default results/<utc stamp>-<family>):
    raw/<run>.json.gz  every poll as received, plus the run's events: the source
                       of truth, so a metric can be redefined and recomputed
                       without rerunning anything
    summary.csv        one row per run, computed from raw/
    polls/<run>.csv    per poll, per node: picture size, hash, lag

Definitions used throughout:
    truth(aircraft)    the freshest report of it that any edge node has made itself
    lag(node, ac)      truth(ac) minus the observation time of what the node holds
                       for it; if the node holds nothing, minus the first time any
                       node reported it. Reports land on a 1 s grid per aircraft,
                       so lag moves in whole seconds.
    prop               propagation latency: for each report made after the warm-up and
                       each edge node that did not make it, time until the node holds it
                       or something fresher. Percentiles are over delivered pairs only.
                       On the synthetic trace a report counts from its observation, which
                       is also when its node received it. On a recorded trace it counts
                       from its first receipt by any node, from the trace; live, from the
                       first poll that shows a node holding it (added after the live runs).
    feed_delay_p50/p99 recorded or live: observation to first receipt, the feed's own
                       delay, which no sync design can shorten
    prop_unresolved    fraction of those (report, node) pairs never delivered by the last
                       poll; they are not in the percentiles, so a high value means the
                       percentiles flatter the run. Added after the predictions.
    settle_ms          after ingest stops, time until every node's hash is equal
    recover_p95_ms     prediction 5 as registered: after a partition heals, time until
                       the p95 lag over all edge nodes and aircraft is back at or below
                       its worst value before the cut
    recover_all_ms     stricter, added after the predictions were registered: time until
                       every edge node holds every aircraft at most as stale as the worst
                       moment before the cut, and stays there for 2 s (or until ingest
                       stops)
                       Both are timed from when the heal command starts, so both are
                       upper bounds.
    wire_bytes_per_obs bytes sent on every node's mesh link during ingest (headers and
                       retransmits included, read from the interface counters) divided
                       by reports ingested. netem drops packets before the interface
                       counts them, so under loss this is what got onto the wire, not
                       what was attempted.
    lag_worst          the worst lag of any edge node on any aircraft at any poll during
                       ingest (after the warm-up); added after the predictions
    stale_p50/p99      staleness, as age of information: at every poll during ingest,
                       for every edge node and every aircraft some edge reported in the
                       last 5 s, the node's clock minus the observation time of what it
                       holds (or of the aircraft's first report, if it holds nothing).
                       Continuous, unlike lag, which moves in whole report intervals.
                       Added after the predictions.
    diverge_*          at each poll, the fraction of aircraft on which the edge nodes do
                       not all hold the same value (missing counts as different): mean
                       and max over ingest, and mean while a cut is fully in force.
                       Added after the predictions.
    catchup_ms         after a heal, time until every edge node holds, for every
                       aircraft, a report at least as fresh as the freshest one any edge
                       had made by the heal. It has no floor taken from before the cut,
                       so irregular reports can't make it look instant. Added after the
                       predictions.
    catchup_merge_ms   catchup_ms in the merge's own order: time until every edge node
                       holds, for every aircraft, a value at least as high in merge order
                       as the highest any edge held at the heal. Last-writer-wins can keep
                       a later write of an older observation, which catchup_ms waits on
                       until a fresher report comes; this asks only that nothing from
                       before the heal is missing. Added after predictions-2.
    wire_bytes_per_useful  wire bytes during ingest divided by merges that won (updates
                       that changed some node's picture). Added after the predictions.
    merged_cut         received tracks the edge nodes merged while a cut was fully in
                       force (one client timeout after the cut, until the heal); added
                       after the predictions
Only edge nodes count toward lag; the star hub is a relay and has no coverage.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import functools
import gzip
import json
import math
import os
import re
import secrets
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ROOT / "harness" / "compose.yml"
IMAGE = "holdfast:dev"
COMMIT_LABEL = "holdfast.commit"

IP = {"hub": "172.30.0.10", "a": "172.30.0.11", "b": "172.30.0.12", "c": "172.30.0.13", "cloud": "172.30.0.14"}
PORT = {"a": 8081, "b": 8082, "c": 8083, "hub": 8084, "cloud": 8085}
CLOUD_IMAGE = "holdfast-cloud:dev"
TRACE = "adsb-2026-11-05.jsonl.gz"  # the recorded hour
EDGES = ("a", "b", "c")

POLL_MS = 100
BEHIND_MS = 2000  # two sync intervals
WARMUP_MS = 3000  # ignore the first seconds while every node is still filling up
SUSTAIN_MS = 2000  # how long recover_all_ms must hold
CUT_GRACE_MS = 2000  # one client timeout: a sync under way at the cut can still finish this late
ACTIVE_MS = 5000  # an aircraft counts toward staleness while some edge reported it this recently

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
    topology: str  # mesh: edges peer with each other; star: edges peer only with the hub;
    # cloud: the star with the cloud peer (on DynamoDB Local) as the hub
    scenario: str  # a LOSS key, "partition" (cut c), or "hubdown" (cut the hub or the cloud relay)
    clock: str  # hlc or wall
    skew_ms: int  # applied to node c's physical clock
    # Where reports come from: "seed1" (the committed synthetic trace), "live"
    # (each edge polls the ADS-B feed for its own coverage), or the name of a
    # recorded trace under traces/. Old raw files predate it and were seed1.
    source: str = "seed1"

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
    # Live traffic, inside the same harness: each edge polls the feed for its
    # own circle every 18 s, the three 6 s apart, so one request every 6 s in
    # all, half the rate the recorder kept up. Never run alongside a recording.
    live = [Config("live", t, s, "hlc", 0, "live") for t, s in
            (("mesh", "loss0"), ("mesh", "partition"), ("star", "hubdown"))]
    # The cloud relay against the registered star runs: same trace, same
    # scenarios, only the hub replaced.
    cloud = [Config("cloud", "cloud", s, "hlc", 0) for s in ("loss0", "loss20", "partition")]
    # The recorded hour (predictions in docs/predictions-2.md). real-hour
    # replays every window of it under each condition the README names;
    # real-base runs the baselines on 8 windows spread across it.
    real_hour = [Config("real-hour", "mesh", s, "hlc", 0, TRACE) for s in ("loss20", "loss50", "partition")]
    real_base = ([Config("real-base", "mesh", "loss0", "hlc", 0, TRACE)]
                 + [Config("real-base", "star", s, "hlc", 0, TRACE) for s in ("loss0", "loss50", "hubdown")]
                 + [Config("real-base", "mesh", "loss0", c, k, TRACE) for c in ("hlc", "wall")
                    for k in (-4000, -2000, 2000, 4000)]
                 # +4000: the trace's handoffs from c are the ones under 4 s
                 + [Config("real-base", "mesh", s, c, 4000, TRACE) for c in ("hlc", "wall")
                    for s in ("loss20", "partition")])
    return {"sync": sync, "clock": clock, "live": live, "cloud": cloud, "real-hour": real_hour,
            "real-base": real_base, "all": sync + clock}[family]


REAL_HOUR_S = 59  # 60 windows of 59 s cover 3540 s of the 3582 s recording


def window_ms(cfg: Config, repeat: int) -> int:
    """Where in the trace a run starts: the same windows for every
    configuration of a family, so differences are not traffic differences."""
    if cfg.family == "real-hour":
        return repeat * REAL_HOUR_S * 1000
    if cfg.family == "real-base":
        return repeat * 450_000  # every 7.5 minutes
    return 0


# A clean run, a partition that has to heal, and the cloud relay with its token.
SMOKE = [Config("smoke", "mesh", "loss0", "hlc", 0), Config("smoke", "mesh", "partition", "hlc", 0),
         Config("smoke", "cloud", "loss0", "hlc", 0)]


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def sh(*args: str, env: dict | None = None) -> str:
    p = subprocess.run(args, env=env, capture_output=True, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"{' '.join(args)} exited {p.returncode}: {p.stderr.strip() or p.stdout.strip()}")
    return p.stdout


NODES = ("a", "b", "c", "hub", "cloud", "ddb")
OBS = ("prometheus", "grafana")
GRAFANA = "http://127.0.0.1:3000/d/holdfast"
LEAD_MS = 10_000  # nodes start this long before the trace, so their startup hold is over first
FEED = "https://api.adsb.lol/v2/point"


def source_flag(source: str) -> str:
    if source == "live":
        return f"-live-url={FEED}"
    if source == "seed1":
        return "-trace=/traces/seed1.jsonl"
    if "/" in source or "\\" in source or not (ROOT / "traces" / source).is_file():
        raise RuntimeError(f"trace {source!r} is not a file under traces/")
    return f"-trace=/traces/{source}"


def compose(env: dict, *args: str) -> None:
    sh("docker", "compose", "-f", str(COMPOSE), "--profile", "star", "--profile", "obs", "--profile", "cloud",
       *args, env=env)


def teardown(env: dict, keep_obs: bool) -> None:
    """Remove every node. Prometheus and Grafana stay up across runs when
    they are watching, so the dashboard shows the whole sweep."""
    if keep_obs:
        compose(env, "rm", "--stop", "--force", *NODES)
    else:
        compose(env, "down", "-t", "2", "--remove-orphans")


def placeholder_env() -> dict:
    """compose.yml needs every variable set even to start Prometheus."""
    return os.environ | {"T0": "0", "CLOCK": "hlc", "TRACE_MS": "0", **{f"{n.upper()}_PEERS": "" for n in EDGES}}


def netem(node: str, args: list[str]) -> str:
    out = sh("docker", "exec", "-u", "0", f"hf-{node}", "netem", *args)
    if args == ["txbytes"]:
        return out.strip()
    active = "netem" in out  # netem.sh prints the qdisc it left in place
    if active == (args == ["clear"]):
        raise RuntimeError(f"netem {args} on {node} did not take effect: {out.strip()}")
    return out


def authorized(url: str, token: str) -> urllib.request.Request:
    """A GET of url, carrying the cloud peer's bearer token if there is one."""
    return urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"} if token else {})


def get_state(node: str, token: str = "") -> dict:
    with urllib.request.urlopen(authorized(f"http://127.0.0.1:{PORT[node]}/state", token), timeout=2) as r:
        return json.load(r)


def pct(sorted_vals: list[float], q: float) -> float:
    """Nearest-rank percentile of an already sorted list."""
    if not sorted_vals:
        return float("nan")
    k = max(0, math.ceil(q / 100 * len(sorted_vals)) - 1)
    return sorted_vals[k]


def blank_pct(sorted_vals: list[float], q: float) -> float | str:
    """pct, but blank rather than NaN for a summary cell with no samples."""
    return pct(sorted_vals, q) if sorted_vals else ""


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
    tx: dict[str, dict[str, int]] = field(default_factory=dict)  # "start"/"end" -> node -> mesh tx bytes
    vm_offset_ms: int = 0  # VM clock minus host clock
    trace_from_ms: int = 0  # where in the trace this run's window starts
    extra: dict = field(default_factory=dict)  # the cloud peer's own counters, from GET /peer
    commit: str = ""  # commit the node image was built from
    errors: list[str] = field(default_factory=list)

    @property
    def nodes(self) -> tuple[str, ...]:
        relay = {"star": ("hub",), "cloud": ("cloud",)}
        return EDGES + relay.get(self.cfg.topology, ())

    @property
    def part_start(self) -> int:
        return int(self.duration_ms * 0.4)

    @property
    def part_end(self) -> int:
        return int(self.duration_ms * 0.8)


def save_raw(run: Run, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    doc = asdict(run)
    doc["polls"] = [[p.t, p.states] for p in run.polls]
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(doc, f, separators=(",", ":"))


def load_raw(path: Path) -> Run:
    with gzip.open(path, "rt", encoding="utf-8") as f:
        doc = json.load(f)
    doc["cfg"] = Config(**doc["cfg"])
    doc["polls"] = [Poll(t, states) for t, states in doc["polls"]]
    return Run(**doc)


def peers(node: str, topology: str) -> str:
    if node in ("hub", "cloud"):
        return ""
    others = {"mesh": [n for n in EDGES if n != node], "star": ["hub"], "cloud": []}[topology]  # cloud: CLOUD_PEER
    return ",".join(f"http://{IP[n]}:8080" for n in others)


def execute(run: Run, keep_obs: bool = False) -> None:
    cfg = run.cfg
    t0 = now_ms() + LEAD_MS
    env = os.environ | {
        "T0": str(t0),
        "CLOCK": cfg.clock,
        "SOURCE": source_flag(cfg.source),
        "TRACE_FROM_MS": str(run.trace_from_ms),
        "TRACE_MS": str(run.duration_ms),
        "C_OFFSET": str(cfg.skew_ms),
        **{f"{n.upper()}_PEERS": peers(n, cfg.topology) for n in EDGES},
    }
    if cfg.topology == "cloud":
        # A fresh token, as Lambda requires, which the edges send through
        # -cloud-peer: every cloud run also tests the edges' side of auth.
        env |= {"CLOUD_PEER": f"http://{IP['cloud']}:8080", "CLOUD_TOKEN": secrets.token_hex(32)}
    auth = {"cloud": env.get("CLOUD_TOKEN", "")}
    services = list(run.nodes)
    teardown(env, keep_obs)
    try:
        if cfg.topology == "cloud":
            services.append("ddb")
        sh("docker", "compose", "-f", str(COMPOSE), "--profile", cfg.topology,
           "up", "-d", "--force-recreate", *services, env=env)

        # Wait until every node answers, and measure the VM clock against ours.
        offsets = []
        while True:
            try:
                for n in run.nodes:
                    before = now_ms()
                    st = get_state(n, auth.get(n, ""))
                    offsets.append(st["now_ms"] - (before + now_ms()) // 2)
                break
            except (OSError, urllib.error.URLError):
                if now_ms() > t0 - 2000:
                    raise RuntimeError("nodes not ready 2 s before the trace starts")
                time.sleep(0.2)
        run.vm_offset_ms = sorted(offsets)[len(offsets) // 2]
        # t0 is our clock but the nodes read it on the Docker VM's clock. A big
        # gap means the schedule below fires at the wrong moments relative to
        # ingest, so refuse rather than measure something else.
        if abs(run.vm_offset_ms) > 2000:
            raise RuntimeError(f"Docker VM clock is {run.vm_offset_ms} ms off the host clock; resync it (wsl --shutdown) and rerun")

        if LOSS.get(cfg.scenario):
            for n in run.nodes:
                netem(n, LOSS[cfg.scenario])

        target = {"star": "hub", "cloud": "cloud"}.get(cfg.topology, "hub") if cfg.scenario == "hubdown" else "c"
        schedule: list[tuple[str, int, str | None, list[str]]] = [
            ("tx:start", 0, None, ["txbytes"]),
            ("tx:end", run.duration_ms, None, ["txbytes"]),
        ]
        if cfg.scenario in ("partition", "hubdown"):
            schedule.append(("cut", run.part_start, target, ["loss", "100%"]))
            if run.heal:
                schedule.append(("heal", run.part_end, target, ["clear"]))
        schedule.sort(key=lambda e: e[1])

        def fire(name: str, node: str | None, args: list[str]) -> None:
            # A change lands somewhere between docker exec starting and
            # returning. Timing from the start makes recovery an upper bound.
            started = now_ms()
            try:
                if name.startswith("tx:"):
                    run.tx[name[3:]] = {n: int(netem(n, args)) for n in run.nodes}
                else:
                    netem(node or "", args)
            except (RuntimeError, ValueError) as e:
                run.errors.append(f"{name}: {e}")
                return
            run.events[name] = started + run.vm_offset_ms - t0
            run.events[name + "_exec"] = now_ms() - started

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
                states = {n: get_state(n, auth.get(n, "")) for n in run.nodes}
                run.polls.append(Poll(states[run.nodes[0]]["now_ms"] - t0, states))
            except (OSError, urllib.error.URLError) as e:
                run.errors.append(f"poll at {host_rel} ms: {e}")
            next_poll += POLL_MS
            time.sleep(max(0, next_poll - now_ms()) / 1000)
        for th in threads:
            th.join()
        if cfg.topology == "cloud":
            try:
                with urllib.request.urlopen(authorized(f"http://127.0.0.1:{PORT['cloud']}/peer", auth["cloud"]), timeout=5) as r:
                    run.extra["peer"] = json.load(r)
            except (OSError, urllib.error.URLError) as e:
                run.errors.append(f"cloud peer counters: {e}")
    finally:
        teardown(env, keep_obs)


@functools.lru_cache(maxsize=2)
def trace_records(name: str) -> tuple[tuple[int, str, int], ...]:
    """(t_ms, icao, delay_ms) of every record in a recorded trace."""
    opener = gzip.open if name.endswith(".gz") else open
    with opener(ROOT / "traces" / name, "rt", encoding="utf-8") as f:
        return tuple((r["t_ms"], r["icao"], r.get("delay_ms", 0)) for r in (json.loads(line) for line in f if line.strip()))


def receipts(run: Run, t0_abs: int) -> tuple[dict[tuple[str, int], int], list[int]]:
    """For a recorded trace or live: when the first node received each report, keyed
    by (aircraft, observation time), and the feed delays in the window. A
    report reaches the first node only after the feed's own delay, which no
    sync design can shorten, so on recorded traffic propagation is timed from
    that first receipt and the feed's delay is reported on its own."""
    if run.cfg.source == "seed1":
        return {}, []  # synthetic: a report is received when it is observed
    if run.cfg.source == "live":
        return receipts_polled(run.polls)
    return receipts_in(trace_records(run.cfg.source), run.trace_from_ms, run.duration_ms, t0_abs)


def receipts_in(records, start: int, length: int, t0_abs: int) -> tuple[dict[tuple[str, int], int], list[int]]:
    """receipts, for records (t_ms, icao, delay_ms) replayed from start."""
    lo, hi = start, start + length
    first: dict[tuple[str, int], int] = {}
    delays = []
    for t, icao, delay in records:
        if lo <= t < hi:
            got = t0_abs + t - lo
            key = (icao, got - delay)
            first[key] = min(first.get(key, got), got)
            delays.append(delay)
    return first, sorted(delays)


def receipts_polled(polls: list[Poll]) -> tuple[dict[tuple[str, int], int], list[int]]:
    """receipts, live, where no trace says when a report arrived: the first
    poll that shows a node holding its own report of an observation, so up to
    one poll interval late."""
    first: dict[tuple[str, int], int] = {}
    for p in polls:
        for n in EDGES:
            now = p.states[n]["now_ms"]
            for ac, tr in p.states[n]["tracks"].items():
                if own := tr.get("own", 0):
                    first[(ac, own)] = min(first.get((ac, own), now), now)
    return first, sorted(got - obs for (_, obs), got in first.items())


def propagation(run: Run, receipt: dict[tuple[str, int], int] | None = None) -> tuple[list[float], int, int]:
    """Propagation latency, sorted: for every report made during the ingest
    window and every edge node that did not make that report itself, the time
    from the report until the node first holds it or something fresher.
    A report counts from its observation, or from its first receipt when
    receipt maps one. Resolution is the poll interval. Also returns how many
    such pairs there were and how many were never delivered."""
    receipt = receipt or {}
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
    # (observation, when the report counts from), in observation order
    pending = {(n, ac): sorted((r, receipt.get((ac, r), r)) for r in rs
                               if lo <= receipt.get((ac, r), r) <= hi and r not in made.get((n, ac), set()))
               for ac, rs in reports.items() for n in EDGES}
    pairs = sum(len(q) for q in pending.values())
    out: list[float] = []
    for p in run.polls:
        for n in EDGES:
            now = p.states[n]["now_ms"]
            for ac, tr in p.states[n]["tracks"].items():
                q = pending.get((n, ac))
                while q and q[0][0] <= tr["at"]:
                    out.append(now - q.pop(0)[1])
    out.sort()
    return out, pairs, sum(len(q) for q in pending.values())


def before_cut(series: list[tuple[int, float]], cut: int) -> list[float]:
    """Values from the warm-up (or half the time to the cut, if that's
    shorter) until the cut."""
    lo = min(WARMUP_MS, cut // 2)
    return [v for t, v in series if lo <= t < cut]


def recovery_p95(p95_by_poll: list[tuple[int, float]], cut: int | None, heal: int | None) -> int | str:
    """Prediction 5 as registered: ms from the heal until the pooled p95 lag
    is at or below its worst value before the cut. Empty if it never is."""
    if cut is None or heal is None:
        return ""
    before = before_cut(p95_by_poll, cut)
    if not before:
        return ""
    p95_floor = max(before)
    for t, v in p95_by_poll:
        if t >= heal and v <= p95_floor:
            return t - heal
    return ""


def recovery_all(worst_by_poll: list[tuple[int, float]], cut: int | None, heal: int | None, end: int) -> int | str:
    """Stricter: ms from the heal until every edge node holds every aircraft
    at most as stale as the worst moment before the cut, and keeps that for
    SUSTAIN_MS or until ingest stops. Empty if it never does."""
    if cut is None or heal is None:
        return ""
    before = before_cut(worst_by_poll, cut)
    if not before:
        return ""
    worst_floor = max(before)
    start = None
    for t, v in worst_by_poll:
        if not heal <= t <= end:
            continue
        if v > worst_floor:
            start = None
            continue
        if start is None:
            start = t
        if t - start >= SUSTAIN_MS:
            return start - heal
    return "" if start is None else start - heal  # held until ingest stopped


def merged_during_cut(run: Run) -> int | str:
    """Received tracks the edge nodes merged while the cut was fully in force:
    from one client timeout after the cut command returned until the heal.
    For a hub cut, anything above 0 means edges still reached each other."""
    window = cut_window(run)
    inside = [p for p in run.polls if window[0] <= p.t < window[1]] if window else []
    if len(inside) < 2:
        return ""
    first, last = inside[0].states, inside[-1].states
    return sum(last[n]["counters"].get("merge_applied", 0) - first[n]["counters"].get("merge_applied", 0)
               for n in EDGES)


def cut_window(run: Run) -> tuple[int, int] | None:
    """The part of a cut that is fully in force: from one client timeout after
    the cut command returned until the heal."""
    cut, heal = run.events.get("cut"), run.events.get("heal")
    if cut is None or heal is None:
        return None
    return cut + run.events.get("cut_exec", 0) + CUT_GRACE_MS, heal


def divergence(states: dict[str, dict]) -> float | None:
    """Fraction of the aircraft any edge holds on which the edges do not all
    hold the same value. None if no edge holds anything."""
    held = [states[n]["tracks"] for n in EDGES]
    aircraft = set().union(*held)
    if not aircraft:
        return None

    def value(tracks: dict, ac: str) -> tuple | None:
        tr = tracks.get(ac)
        return None if tr is None else merge_key(tr)

    differ = sum(1 for ac in aircraft if len({value(t, ac) for t in held}) > 1)
    return differ / len(aircraft)


def merge_key(tr: dict) -> tuple[int, int, str]:
    """A held value's place in the merge's order: its stamp, then its origin."""
    return tr["ts"].get("w", 0), tr["ts"].get("l", 0), tr["origin"]


def catchup_merge(run: Run) -> int | str:
    """catchup in the merge's order: ms from the heal until every edge holds,
    for every aircraft, a value at least as high in merge order as the highest
    any edge held at the heal. Empty if it never does."""
    heal = run.events.get("heal")
    before = [p for p in run.polls if p.t <= heal] if heal is not None else []
    if not before:
        return ""
    due: dict[str, tuple[int, int, str]] = {}
    for n in EDGES:
        for ac, tr in before[-1].states[n]["tracks"].items():
            due[ac] = max(due.get(ac, merge_key(tr)), merge_key(tr))
    for p in run.polls:
        if p.t >= heal and all(ac in p.states[n]["tracks"] and merge_key(p.states[n]["tracks"][ac]) >= k
                               for n in EDGES for ac, k in due.items()):
            return p.t - heal
    return ""


def catchup(run: Run) -> int | str:
    """ms from the heal until every edge holds, for every aircraft, a report
    at least as fresh as the freshest one any edge had made by the heal.
    Empty if it never does."""
    heal = run.events.get("heal")
    before = [p for p in run.polls if p.t <= heal] if heal is not None else []
    if not before:
        return ""
    due: dict[str, int] = {}
    for n in EDGES:
        for ac, tr in before[-1].states[n]["tracks"].items():
            if tr.get("own"):
                due[ac] = max(due.get(ac, 0), tr["own"])
    for p in run.polls:
        if p.t >= heal and all(p.states[n]["tracks"].get(ac, {}).get("at", 0) >= at
                               for n in EDGES for ac, at in due.items()):
            return p.t - heal
    return ""


def analyze(run: Run, polls_path: Path) -> dict:
    first_seen: dict[str, int] = {}
    samples: list[float] = []
    behind_fracs: list[float] = []
    p95_by_poll: list[tuple[int, float]] = []  # pooled over edge nodes and aircraft
    worst_by_poll: list[tuple[int, float]] = []  # the worst edge node's worst aircraft
    settled_at = None
    lag_worst = None
    stale: list[float] = []
    diverge: list[float] = []  # during ingest
    diverge_cut: list[float] = []  # while a cut is fully in force
    window = cut_window(run)
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
            worst = 0.0
            for n in run.nodes:
                tracks = p.states[n]["tracks"]
                lags = []
                if n in EDGES:
                    now = p.states[n]["now_ms"]
                    ingesting = WARMUP_MS <= p.t <= run.duration_ms
                    for ac, fresh in truth.items():
                        have = tracks[ac]["at"] if ac in tracks else first_seen[ac]
                        lags.append(max(0, fresh - have))
                        if ingesting and now - fresh <= ACTIVE_MS:
                            stale.append(max(0, now - have))
                    lags.sort()
                    poll_lags.extend(lags)
                    behind += sum(1 for x in lags if x > BEHIND_MS)
                    if lags:
                        worst = max(worst, lags[-1])
                w.writerow([p.t, n, len(tracks), p.states[n]["hash"][:12], len(truth),
                            pct(lags, 50) if lags else "", pct(lags, 95) if lags else "",
                            lags[-1] if lags else "", sum(1 for x in lags if x > BEHIND_MS) if lags else ""])
            if poll_lags:
                poll_lags.sort()
                p95_by_poll.append((p.t, pct(poll_lags, 95)))
                worst_by_poll.append((p.t, worst))
                if WARMUP_MS <= p.t <= run.duration_ms:
                    samples.extend(poll_lags)
                    behind_fracs.append(behind / len(poll_lags))
                    lag_worst = worst if lag_worst is None else max(lag_worst, worst)
            d = divergence(p.states)
            if d is not None and WARMUP_MS <= p.t <= run.duration_ms:
                diverge.append(d)
            if d is not None and window and window[0] <= p.t < window[1]:
                diverge_cut.append(d)
            if settled_at is None and p.t >= run.duration_ms and len({p.states[n]["hash"] for n in run.nodes}) == 1:
                settled_at = p.t

    samples.sort()
    stale.sort()
    t0_abs = run.polls[0].states[run.nodes[0]]["now_ms"] - run.polls[0].t if run.polls else 0
    receipt, feed_delays = receipts(run, t0_abs)
    prop, prop_pairs, prop_lost = propagation(run, receipt)
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

    cut, heal = run.events.get("cut"), run.events.get("heal")
    ingest = totals.get("ingest_total", 0)
    wire = useful = ""
    if "start" in run.tx and "end" in run.tx:
        sent = sum(run.tx["end"][n] - run.tx["start"][n] for n in run.nodes)
        if ingest:
            wire = round(sent / ingest, 1)
        if totals.get("merge_applied"):
            useful = round(sent / totals["merge_applied"], 1)

    gaps = [b.t - a.t for a, b in zip(run.polls, run.polls[1:])]
    sync_total = totals.get("sync_ok", 0) + totals.get("sync_err", 0)
    c = run.cfg
    return {
        "run_id": c.run_id(run.repeat), "family": c.family, "topology": c.topology, "scenario": c.scenario,
        "clock": c.clock, "skew_ms": c.skew_ms, "source": c.source, "trace_from_ms": run.trace_from_ms,
        "repeat": run.repeat, "duration_ms": run.duration_ms,
        "settled": settled_at is not None,
        "settle_ms": "" if settled_at is None else settled_at - run.duration_ms,
        "recover_p95_ms": recovery_p95(p95_by_poll, cut, heal),
        "recover_all_ms": recovery_all(worst_by_poll, cut, heal, run.duration_ms),
        "cut_at_ms": "" if cut is None else cut, "heal_at_ms": "" if heal is None else heal,
        "heal_exec_ms": run.events.get("heal_exec", ""),
        "merged_cut": merged_during_cut(run),  # added after the predictions
        "catchup_ms": catchup(run),  # added after the predictions
        "catchup_merge_ms": catchup_merge(run),  # added after predictions-2
        "diverge_mean": round(sum(diverge) / len(diverge), 4) if diverge else "",
        "diverge_max": round(max(diverge), 4) if diverge else "",
        "diverge_cut": round(sum(diverge_cut) / len(diverge_cut), 4) if diverge_cut else "",
        "prop_p50": blank_pct(prop, 50), "prop_p95": blank_pct(prop, 95), "prop_p99": blank_pct(prop, 99),
        "prop_n": len(prop),
        "prop_unresolved": round(prop_lost / prop_pairs, 4) if prop_pairs else "",  # added after the predictions
        "lag_p95": blank_pct(samples, 95),
        "lag_worst": "" if lag_worst is None else lag_worst,  # added after the predictions
        "stale_p50": blank_pct(stale, 50), "stale_p99": blank_pct(stale, 99),  # added after the predictions
        "behind_frac": round(sum(behind_fracs) / len(behind_fracs), 4) if behind_fracs else "",
        "aircraft": len(own_by), "overlap_frac": round(sum(1 for v in own_by.values() if v >= 2) / len(own_by), 3) if own_by else "",
        "ingest_total": ingest, "ingest_lost": totals.get("ingest_lost", 0),
        "ingest_held": totals.get("ingest_held", 0),
        "ingest_regress": totals.get("ingest_regress", 0),  # added after the predictions
        "expired": totals.get("expired", 0), "merge_expired": totals.get("merge_expired", 0),
        "sensor_errors": totals.get("sensor_errors", 0),
        "merge_applied": totals.get("merge_applied", 0), "merge_regress": totals.get("merge_regress", 0),
        "wrong_final": wrong_final,
        "sync_ok": totals.get("sync_ok", 0), "sync_err": totals.get("sync_err", 0),
        "sync_err_rate": round(totals.get("sync_err", 0) / sync_total, 4) if sync_total else "",
        "rejected": totals.get("rejected", 0),
        "aborted": totals.get("aborted", 0),  # cut off in flight; added after the predictions
        "wire_bytes_per_obs": wire,
        "wire_bytes_per_useful": useful,  # added after the predictions
        "feed_delay_p50": blank_pct(feed_delays, 50), "feed_delay_p99": blank_pct(feed_delays, 99),
        **cloud_columns(run, ingest),
        "polls": len(run.polls), "poll_ms_mean": round(sum(gaps) / len(gaps), 1) if gaps else "",
        "vm_offset_ms": run.vm_offset_ms, "commit": run.commit, "errors": " | ".join(run.errors),
    }


def cloud_columns(run: Run, ingest: int) -> dict:
    """The cloud peer's own counters for a run, and its writes per report."""
    peer = run.extra.get("peer")
    if not peer:
        return {"cloud_requests": "", "cloud_writes": "", "cloud_won": "", "cloud_failed": "", "cloud_writes_per_report": ""}
    return {"cloud_requests": peer["requests"], "cloud_writes": peer["writes"], "cloud_won": peer["won"],
            "cloud_failed": peer["failed"],
            "cloud_writes_per_report": round(peer["writes"] / ingest, 3) if ingest else ""}


def dirty(root: Path) -> bool:
    """Whether the checkout differs from its commit, the harness's own output
    aside: a sweep's results must not mark the code it measures as changed."""
    return bool(sh("git", "-C", str(root), "status", "--porcelain", "--", ".", ":(exclude)results").strip())


def git_commit() -> str:
    try:
        commit = sh("git", "-C", str(ROOT), "rev-parse", "--short", "HEAD").strip()
        return commit + "-dirty" if dirty(ROOT) else commit
    except (RuntimeError, OSError):
        return "unknown"


def image_commit() -> str:
    return sh("docker", "image", "inspect", IMAGE, "--format",
              "{{index .Config.Labels \"" + COMMIT_LABEL + "\"}}").strip()


class Summary:
    """summary.csv, written a row at a time so a crash keeps what finished."""

    def __init__(self, path: Path):
        self.f = path.open("w", newline="")
        self.w: csv.DictWriter | None = None

    def add(self, row: dict) -> None:
        if self.w is None:
            self.w = csv.DictWriter(self.f, fieldnames=list(row), lineterminator="\n")
            self.w.writeheader()
        self.w.writerow(row)
        self.f.flush()

    def close(self) -> None:
        self.f.close()


def failures_of(row: dict, scenario: str, healed: bool) -> list[str]:
    out = []
    if not row["settled"] or row["errors"] or row["rejected"]:
        out.append(row["run_id"])
    if scenario in ("partition", "hubdown") and healed and row["recover_p95_ms"] == "":
        out.append(row["run_id"] + " (never recovered)")
    if row["ingest_held"]:
        out.append(row["run_id"] + " (reports held at startup: the run measured the startup hold)")
    return out


def reanalyze(out: Path) -> int:
    raws = sorted((out / "raw").glob("*.json.gz"))
    if not raws:
        print(f"no raw runs under {out / 'raw'}")
        return 1
    summary = Summary(out / "summary.csv")
    for path in raws:
        run = load_raw(path)
        summary.add(analyze(run, out / "polls" / f"{run.cfg.run_id(run.repeat)}.csv"))
    summary.close()
    print(f"recomputed {len(raws)} runs into {out / 'summary.csv'}")
    return 0


def main() -> int:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--smoke", action="store_true", help="three short runs (clean, partition, cloud); exit 1 unless all converge")
    ap.add_argument("--family", choices=("sync", "clock", "live", "cloud", "real-hour", "real-base", "all"))
    ap.add_argument("--reanalyze", type=Path, metavar="DIR", help="recompute DIR/summary.csv from DIR/raw")
    ap.add_argument("--repeats", type=int, default=5)
    ap.add_argument("--duration", type=int, help="seconds of ingest (default 50, smoke 20)")
    ap.add_argument("--settle", type=int, default=10, help="seconds to keep polling after ingest stops")
    ap.add_argument("--out", type=Path)
    ap.add_argument("--no-heal", action="store_true",
                    help="never heal partitions; exists to prove the smoke gate fails when nodes stay apart")
    ap.add_argument("--skip-build", action="store_true", help="reuse holdfast:dev if it was built from this commit")
    ap.add_argument("--obs", action="store_true",
                    help=f"run Prometheus and Grafana alongside and leave them up afterwards ({GRAFANA})")
    ap.add_argument("--only", metavar="REGEX", help="run only the family's configurations whose run id matches")
    ap.add_argument("--resume", action="store_true",
                    help="keep runs whose raw file already exists under --out instead of rerunning them")
    args = ap.parse_args()
    if sum([args.smoke, bool(args.family), bool(args.reanalyze)]) != 1:
        ap.error("give exactly one of --smoke, --family or --reanalyze")
    if args.reanalyze:
        return reanalyze(args.reanalyze)

    configs = SMOKE if args.smoke else experiments(args.family)
    if args.only:
        configs = [c for c in configs if re.search(args.only, c.run_id(0))]
        if not configs:
            ap.error(f"--only {args.only!r} matches no configuration of {args.family}")
    repeats = 1 if args.smoke else args.repeats
    default_s = 20 if args.smoke else REAL_HOUR_S if args.family == "real-hour" else 50
    duration_ms = (args.duration or default_s) * 1000
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = args.out or ROOT / "results" / f"{stamp}-{'smoke' if args.smoke else args.family}"
    out.mkdir(parents=True, exist_ok=True)

    # Results are credited to the commit the node image was built from, and a
    # reused image must match the checkout, or the label would be a lie.
    commit = git_commit()
    if args.skip_build:
        built = image_commit()
        if built != commit:
            print(f"{IMAGE} was built from {built or 'an unlabeled tree'}, the checkout is {commit}; drop --skip-build")
            return 1
    else:
        print(f"building {IMAGE} at {commit}", flush=True)
        sh("docker", "build", "-q", "-t", IMAGE, "--label", f"{COMMIT_LABEL}={commit}",
           "-f", str(ROOT / "harness" / "Dockerfile"), str(ROOT))
        if any(c.topology == "cloud" for c in configs):
            print(f"building {CLOUD_IMAGE} at {commit}", flush=True)
            sh("docker", "build", "-q", "-t", CLOUD_IMAGE, "--label", f"{COMMIT_LABEL}={commit}",
               "-f", str(ROOT / "cloud" / "Dockerfile"), str(ROOT))

    if args.obs:
        compose(placeholder_env(), "up", "-d", *OBS)
        print(f"dashboard: {GRAFANA}", flush=True)
    summary = Summary(out / "summary.csv")
    failures = []
    total = len(configs) * repeats
    i = 0
    for repeat in range(repeats):
        for cfg in configs:
            i += 1
            raw = out / "raw" / f"{cfg.run_id(repeat)}.json.gz"
            started = time.time()
            if args.resume and raw.exists():
                run = load_raw(raw)
                if run.commit != commit:
                    print(f"{raw} was measured at {run.commit}, the checkout is {commit}; a sweep must not mix commits")
                    return 1
            else:
                run = Run(cfg, repeat, duration_ms, args.settle * 1000, heal=not args.no_heal, commit=commit,
                          trace_from_ms=window_ms(cfg, repeat))
                try:
                    execute(run, keep_obs=args.obs)
                except RuntimeError as e:
                    run.errors.append(str(e))
                save_raw(run, raw)
            row = analyze(run, out / "polls" / f"{cfg.run_id(repeat)}.csv")
            summary.add(row)
            print(f"[{i}/{total}] {row['run_id']}: settled={row['settled']} settle_ms={row['settle_ms']} "
                  f"recover_p95/all={row['recover_p95_ms']}/{row['recover_all_ms']} "
                  f"prop_p50/p95={row['prop_p50']}/{row['prop_p95']} ingest_lost={row['ingest_lost']} "
                  f"regress={row['merge_regress']} err_rate={row['sync_err_rate']} wire_B/obs={row['wire_bytes_per_obs']} "
                  f"({time.time() - started:.0f}s){' ERRORS: ' + row['errors'] if row['errors'] else ''}",
                  flush=True)
            failures += failures_of(row, cfg.scenario, not args.no_heal)
    summary.close()

    print(f"wrote {out / 'summary.csv'}")
    if args.obs:
        print(f"Prometheus and Grafana are still up at {GRAFANA}; stop them with "
              f"docker compose -f {COMPOSE} --profile obs down")
    if failures:
        print("FAILED: " + ", ".join(failures))
        return 1 if args.smoke else 0  # in a full sweep a failed run is data, not a crash
    return 0


if __name__ == "__main__":
    sys.exit(main())
