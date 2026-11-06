"""Why does 50% loss break sync? Runs the mesh with a chosen client timeout
and reads the kernel's own TCP counters inside every edge node mid-run.

    python -I harness/tcp_probe.py --out results/tcp-probe loss0:2s loss50:2s loss50:10s

Each argument is scenario:timeout. Every run is the sweep's own run (same
trace, same 50 s ingest, same analysis); on top, /proc/net/snmp and
/proc/net/netstat are read in each edge node from 5 s into ingest until 2 s
before it ends, and so are the sync counters over the same window. Writes
summary.csv, one row per argument. Added after the predictions, as a
diagnostic, not part of the registered experiment.
"""

from __future__ import annotations

import argparse
import os
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))  # python -I leaves the script's folder off the path
import run as h  # noqa: E402

KEYS = {
    "Tcp": ["ActiveOpens", "RetransSegs", "OutSegs"],
    "TcpExt": ["TCPSynRetrans", "TCPTimeouts"],
}


def tcp_counters(node: str) -> dict[str, int]:
    out = h.sh("docker", "exec", f"hf-{node}", "cat", "/proc/net/snmp", "/proc/net/netstat").splitlines()
    got = {}
    for head, vals in zip(out[::2], out[1::2]):  # each table is a header line, then a value line
        name, *cols = head.split()
        _, *nums = vals.split()
        for k, v in zip(cols, nums):
            if k in KEYS.get(name.rstrip(":"), []):
                got[k] = int(v)
    return got


def probe(spec: str, out: Path, commit: str) -> dict:
    scenario, _, timeout = spec.partition(":")
    os.environ["SYNC_TIMEOUT"] = timeout or "2s"  # compose.yml passes it to -sync-timeout
    run = h.Run(h.Config("probe", "mesh", scenario, "hlc", 0), 0, 50_000, 10_000, commit=commit)
    th = threading.Thread(target=h.execute, args=(run,))
    th.start()
    time.sleep(h.LEAD_MS / 1000 + 5)  # execute starts the trace LEAD_MS out; skip the first 5 s of ingest
    first = {n: (tcp_counters(n), h.get_state(n)["counters"]) for n in h.EDGES}
    time.sleep(50 - 5 - 2)
    last = {n: (tcp_counters(n), h.get_state(n)["counters"]) for n in h.EDGES}
    th.join()
    row = h.analyze(run, out / "polls" / f"{spec.replace(':', '-')}-{time.time_ns()}.csv")
    window = {k: sum(last[n][0][k] - first[n][0][k] for n in h.EDGES) for k in last["a"][0]}
    for k in ("sync_ok", "sync_err"):
        window[k] = sum(last[n][1][k] - first[n][1][k] for n in h.EDGES)
    return {"spec": spec, "scenario": scenario, "sync_timeout": timeout or "2s",
            **{k: row[k] for k in ("settled", "settle_ms", "prop_p50", "prop_p95", "prop_unresolved",
                                    "sync_err_rate", "commit", "errors")},
            **{f"window_{k}": v for k, v in window.items()}}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("specs", nargs="+", metavar="scenario:timeout")
    args = ap.parse_args()
    commit = h.git_commit()
    h.sh("docker", "build", "-q", "-t", h.IMAGE, "--label", f"{h.COMMIT_LABEL}={commit}",
         "-f", str(h.ROOT / "harness" / "Dockerfile"), str(h.ROOT))
    args.out.mkdir(parents=True, exist_ok=True)
    summary = h.Summary(args.out / "summary.csv")
    for i, spec in enumerate(args.specs):
        row = {"i": i, **probe(spec, args.out, commit)}
        summary.add(row)
        print(" ".join(f"{k}={v}" for k, v in row.items() if k != "errors"), flush=True)
    summary.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
