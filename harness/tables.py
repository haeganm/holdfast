"""The README's results tables, built from summary.csv files with one cell
format, exactly as the README shows them. plot.py prints them beside its
charts, and test_tables.py checks the README against them. Stdlib only, so
that check runs where matplotlib isn't installed.
"""

from __future__ import annotations

import csv
import statistics
from pathlib import Path

SCENARIOS = ["loss0", "loss20", "burst20", "loss50", "partition", "hubdown"]
LABELS = {"loss0": "no loss", "loss20": "20% loss", "burst20": "20% bursty", "loss50": "50% loss",
          "partition": "c cut 20 s", "hubdown": "hub cut 20 s"}
SKEWS = [-2000, -500, 0, 2000]


def rows(path: Path) -> list[dict]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def num(v: str | None) -> float | None:
    return None if v in ("", None) else float(v)


def fmt(x: float, kind: str) -> str:
    if kind == "pct":
        return f"{x * 100:.1f}%".replace(".0%", "%")
    return f"{round(x)}" if kind == "int" else f"{x:g}"


def cell(rs: list[dict], col: str, kind: str = "ms", agg: str = "median") -> str:
    """Median (or total) over runs, then min-max; runs where the metric never
    happened are counted after it, not dropped."""
    got = sorted(v for v in (num(r[col]) for r in rs) if v is not None)
    never = len(rs) - len(got)
    if not got:
        return "never" if never else ""
    if agg == "sum":
        return f"{fmt(sum(got), kind)} ({fmt(got[0], kind)}-{fmt(got[-1], kind)})"
    m = statistics.median(got)
    out = fmt(m, kind) if got[0] == got[-1] else f"{fmt(m, kind)} ({fmt(got[0], kind)}-{fmt(got[-1], kind)})"
    return out + (f", {never} never" if never else "")


def pick(rs: list[dict], **kw) -> list[dict]:
    return [r for r in rs if all(r[k] == str(v) for k, v in kw.items())]


def settled(rs: list[dict]) -> str:
    return f"{sum(r['settled'] == 'True' for r in rs)}/{len(rs)}"


def md(header: list[str], lines: list[list[str]]) -> list[str]:
    return ["| " + " | ".join(header) + " |", "|" + "---|" * len(header),
            *("| " + " | ".join(line) + " |" for line in lines), ""]


def synthetic(sync_dir: Path, clock_dir: Path) -> list[str]:
    sync, clock = rows(sync_dir / "summary.csv"), rows(clock_dir / "summary.csv")
    lines = []
    for topo in ("mesh", "star"):
        for sc in SCENARIOS:
            rs = pick(sync, topology=topo, scenario=sc)
            if not rs:
                continue  # the mesh has no hub to cut
            cut = sc in ("partition", "hubdown")
            lines.append([topo, LABELS[sc], settled(rs), cell(rs, "settle_ms"), cell(rs, "prop_p50"),
                          cell(rs, "prop_p95"), cell(rs, "prop_p99"), cell(rs, "prop_unresolved", "pct"),
                          cell(rs, "recover_p95_ms") if cut else "", cell(rs, "sync_err_rate", "pct"),
                          cell(rs, "wire_bytes_per_obs", "int")])
    out = ["", f"{sync_dir / 'summary.csv'}", ""]
    out += md(["topology", "scenario", "settled", "settle", "prop p50", "prop p95", "prop p99", "never delivered",
               "recovery", "sync errors", "wire B/report"], lines)
    out.append(f"across all {len(sync)} runs: " + ", ".join(f"{sum(int(r[col]) for r in sync)} {col}" for col in
                                                         ("ingest_lost", "merge_regress", "rejected", "aborted")))
    out += ["merged during the cut: " + ", ".join(f"{topo} {LABELS[sc]} {cell(pick(sync, topology=topo, scenario=sc), 'merged_cut')}"
                                                  for topo, sc in (("mesh", "partition"), ("star", "hubdown"))), ""]
    lines = []
    for clk in ("hlc", "wall"):
        for k in SKEWS:
            rs = pick(clock, clock=clk, skew_ms=k)
            lines.append(["HLC" if clk == "hlc" else "wall", f"{k:+d} ms" if k else "0",
                          *(cell(rs, col, "n") for col in ("ingest_lost", "merge_regress", "wrong_final", "lag_worst")),
                          cell(rs, "prop_p95")])
    out += [f"{clock_dir / 'summary.csv'}", ""]
    out += md(["clock", "c's skew", "lost local writes", "older report won a merge", "wrong final values",
               "worst lag (ms)", "prop p95 (ms)"], lines)
    return out


def real(hour_dir: Path | None, base_dir: Path | None, cloud_dir: Path | None, control_dir: Path | None,
         live_dir: Path | None, sync_dir: Path) -> list[str]:
    """The real-traffic, live and cloud tables, with the facts the README's
    text around them quotes."""
    out = []
    if hour_dir and base_dir:
        hour, base = rows(hour_dir / "summary.csv"), rows(base_dir / "summary.csv")
        out += ["", f"{hour_dir / 'summary.csv'}, the no-loss row from {base_dir / 'summary.csv'}", ""]
        conds = [("no loss (8 windows, below)", pick(base, topology="mesh", scenario="loss0", clock="hlc", skew_ms=0))]
        conds += [(label, pick(hour, scenario=sc)) for label, sc in
                  (("20% loss", "loss20"), ("50% loss", "loss50"), ("c cut 23.6 s", "partition"))]
        lines = []
        for label, rs in conds:
            live = [r for r in rs if num(r["prop_n"])]  # windows with any report to propagate
            cut = label.startswith("c cut")
            lines.append([label, settled(rs), cell(rs, "settle_ms"), cell(live, "prop_p50"), cell(live, "prop_p95"),
                          cell(live, "prop_p99"), cell(live, "prop_unresolved", "pct"), cell(live, "stale_p50"),
                          cell(live, "stale_p99"), cell(live, "diverge_mean", "pct"),
                          cell(rs, "catchup_merge_ms") if cut else "", cell(rs, "merged_cut") if cut else "",
                          cell(rs, "sync_err_rate", "pct"), cell(live, "wire_bytes_per_useful", "int")])
            out.append(f"{label}: {len(rs) - len(live)} windows with nothing to propagate")
        out.append("")
        out += md(["network", "settled", "settle", "prop p50", "prop p95", "prop p99", "never delivered", "staleness p50",
                   "staleness p99", "divergence", "catch-up", "merged during the cut", "sync errors",
                   "bytes per useful update"], lines)
        fd = [r for r in pick(hour, scenario="loss20") if num(r["feed_delay_p50"]) is not None]
        out.append(f"feed delay over the {len(fd)} windows with reports: p50 {cell(fd, 'feed_delay_p50')}, "
                   f"p99 {cell(fd, 'feed_delay_p99')}")
        reg = [num(r["catchup_ms"]) for r in pick(hour, scenario="partition")]
        out.append(f"registered catchup_ms: {sum(v is None for v in reg)} of {len(reg)} never completed; median counting "
                   f"those as never: {statistics.median(float('inf') if v is None else v for v in reg)}")
    if base_dir:
        base = rows(base_dir / "summary.csv")
        out += ["", f"{base_dir / 'summary.csv'}: the central server", ""]
        lines = []
        for label, topo, sc in (("mesh, no loss", "mesh", "loss0"), ("star, no loss", "star", "loss0"),
                                ("star, 50% loss", "star", "loss50"), ("star, hub cut 20 s", "star", "hubdown")):
            rs = pick(base, topology=topo, scenario=sc, clock="hlc", skew_ms=0)
            lines.append([label, settled(rs), cell(rs, "prop_p50"), cell(rs, "prop_p95"),
                          cell(rs, "prop_unresolved", "pct"), cell(rs, "stale_p50"), cell(rs, "stale_p99"),
                          cell(rs, "diverge_mean", "pct"), cell(rs, "merged_cut") if sc == "hubdown" else "",
                          cell(rs, "sync_err_rate", "pct"), cell(rs, "wire_bytes_per_useful", "int")])
        out += md(["configuration", "settled", "prop p50", "prop p95", "never delivered", "staleness p50",
                   "staleness p99", "divergence", "merged during the cut", "sync errors", "bytes per useful update"], lines)
        out += [f"{base_dir / 'summary.csv'}: the clocks, totals over the eight windows, range per window", ""]
        names = {-4000: "4 s slow", -2000: "2 s slow", 2000: "2 s fast", 4000: "4 s fast"}
        nets = {"loss0": "no loss", "loss20": "20% loss", "partition": "c cut 20 s"}
        lines = []
        for label, clock, skews, sc in (("hybrid", "hlc", (-4000, -2000, 2000, 4000), "loss0"),
                                        ("wall", "wall", (-4000, -2000, 2000), "loss0"), ("wall", "wall", (4000,), "loss0"),
                                        ("hybrid", "hlc", (4000,), "loss20"), ("wall", "wall", (4000,), "loss20"),
                                        ("hybrid", "hlc", (4000,), "partition"), ("wall", "wall", (4000,), "partition")):
            per = [pick(base, clock=clock, skew_ms=k, scenario=sc) for k in skews]
            cells = []
            for col in ("ingest_lost", "merge_regress", "wrong_final"):
                # one row stands for several skews only if they agree window by window
                if len({tuple(r[col] for r in sorted(rs, key=lambda r: r["run_id"][-3:])) for rs in per}) != 1:
                    raise ValueError(f"{label} at {skews} under {sc}: the skews differ on {col}, so they can't share a row")
                cells.append(cell(per[0], col, "n", "sum"))
            lines.append([label, ", ".join(names[k] for k in skews), nets[sc], *cells])
        out += md(["clock", "c's clock", "network", "lost local writes", "merges an older report won",
                   "aircraft wrong at the end"], lines)
    if cloud_dir and control_dir:
        cloud, control = rows(cloud_dir / "summary.csv"), rows(control_dir / "summary.csv")
        reg = [r for r in rows(sync_dir / "summary.csv") if r["topology"] == "star"]
        out += ["", f"{cloud_dir / 'summary.csv'} against the hub: {control_dir / 'summary.csv'} at the same commit, "
                f"and the star rows of {sync_dir / 'summary.csv'}", ""]
        lines = []
        for sc, label in (("loss0", "no loss"), ("loss20", "20% loss"), ("partition", "c cut 20 s")):
            for who, rs in (("in-memory hub, same commit", pick(control, scenario=sc)),
                            ("cloud peer on DynamoDB Local", pick(cloud, scenario=sc)),
                            (f"in-memory hub, registered ({reg[0]['commit']})", pick(reg, scenario=sc))):
                lines.append([label, who, settled(rs), cell(rs, "prop_p50"), cell(rs, "prop_p95"), cell(rs, "prop_p99"),
                              cell(rs, "cloud_writes_per_report", "n") if "cloud" in who else ""])
        out += md(["network", "relay", "settled", "prop p50", "prop p95", "prop p99", "conditional writes per report"], lines)
    if live_dir:
        live = rows(live_dir / "summary.csv")
        out += ["", f"{live_dir / 'summary.csv'}", ""]
        lines = []
        for topo, sc, label in (("mesh", "loss0", "mesh, no loss"), ("mesh", "partition", "mesh, c cut 20 s"),
                                ("star", "hubdown", "star, hub cut 20 s")):
            rs = pick(live, topology=topo, scenario=sc)
            cut = sc != "loss0"
            lines.append([label, settled(rs), cell(rs, "aircraft", "n"), cell(rs, "prop_p50"), cell(rs, "prop_p95"),
                          cell(rs, "feed_delay_p50"), cell(rs, "stale_p50"), cell(rs, "stale_p99"),
                          cell(rs, "catchup_merge_ms") if cut else "", cell(rs, "merged_cut") if cut else ""])
        out += md(["configuration", "settled", "aircraft", "prop p50", "prop p95", "feed delay p50", "staleness p50",
                   "staleness p99", "catch-up", "merged during the cut"], lines)
    return out


def report(sync_dir: Path, clock_dir: Path, hour_dir: Path | None = None, base_dir: Path | None = None,
           cloud_dir: Path | None = None, control_dir: Path | None = None, live_dir: Path | None = None) -> list[str]:
    """Every table, as lines to print."""
    return synthetic(sync_dir, clock_dir) + real(hour_dir, base_dir, cloud_dir, control_dir, live_dir, sync_dir)
