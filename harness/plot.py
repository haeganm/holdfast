"""Draw the README figures from harness results.

    python harness/plot.py --sync results/<sync run dir> --clock results/<clock run dir> --out docs

Writes partition-timeline.png, sync-latency.png and clock-skew.png, and
prints the README's results tables (median, then min-max over repeats). Every
point drawn and every number printed is a row of summary.csv or polls/*.csv in
the given directories; nothing is smoothed or filtered.
"""

from __future__ import annotations

import argparse
import csv
import statistics
from collections.abc import Sequence
from pathlib import Path

import matplotlib
import matplotlib.ticker

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# Reference categorical palette, light mode, fixed order (dataviz skill).
BLUE, ORANGE, AQUA = "#2a78d6", "#eb6834", "#1baf7a"
SURFACE, INK, INK2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3de"

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK2, "xtick.color": INK2, "ytick.color": INK2,
    "text.color": INK, "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.8,
    "axes.spines.top": False, "axes.spines.right": False, "font.size": 10,
    "axes.titlesize": 12, "axes.titleweight": "bold", "axes.titlelocation": "left",
    "legend.frameon": False,
})


def plain_log(ax) -> None:
    """A log axis labelled 0.5, 1, 2, 5 rather than 10^0."""
    fmt = matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:g}")
    ax.yaxis.set_major_formatter(fmt)
    # label the 2s and 5s by their first significant digit, so 0.5 and 0.2 get one too
    ax.yaxis.set_minor_formatter(matplotlib.ticker.FuncFormatter(
        lambda v, _: f"{v:g}" if f"{v:g}".lstrip("0.")[:1] in ("2", "5") else ""))


def rows(path: Path) -> list[dict]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def spread(vals: Sequence[float]) -> tuple[float, float, float]:
    return statistics.median(vals), min(vals), max(vals)


def timeline(sync_dir: Path, out: Path) -> None:
    run_id = "sync-mesh-partition-hlc-skew0-r0"
    summary = {r["run_id"]: r for r in rows(sync_dir / "summary.csv")}[run_id]
    polls = rows(sync_dir / "polls" / f"{run_id}.csv")
    fig, ax = plt.subplots(figsize=(8, 4))
    cut, heal = float(summary["cut_at_ms"]) / 1000, float(summary["heal_at_ms"]) / 1000
    ax.axvspan(cut, heal, color=GRID, alpha=0.6, lw=0)
    ax.text((cut + heal) / 2, 0.97, "c cut off", transform=ax.get_xaxis_transform(),
            ha="center", va="top", color=INK2)
    for node, color in (("a", BLUE), ("b", ORANGE), ("c", AQUA)):
        pts = [(int(p["t_ms"]) / 1000, float(p["lag_p95"]) / 1000) for p in polls if p["node"] == node and p["lag_p95"]]
        xs, ys = zip(*pts)
        ax.plot(xs, ys, color=color, lw=2, drawstyle="steps-post", label=f"node {node}")
    end = float(summary["duration_ms"]) / 1000
    ax.axvline(end, color=INK2, lw=0.8, ls=":")
    ax.text(end, 0.97, " ingest stops", transform=ax.get_xaxis_transform(), va="top", color=INK2)
    ax.set_xlabel("seconds since the trace started")
    ax.set_ylabel("p95 lag behind the freshest report (s)")
    ax.set_title("Lag grows 1 s per second of the cut and clears 1.2 s after the heal")
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(out / "partition-timeline.png", dpi=150)
    plt.close(fig)


SCENARIOS = ["loss0", "loss20", "burst20", "loss50", "partition", "hubdown"]
LABELS = {"loss0": "no loss", "loss20": "20% loss", "burst20": "20% bursty", "loss50": "50% loss",
          "partition": "c cut 20 s", "hubdown": "hub cut 20 s"}


def sync_latency(sync_dir: Path, out: Path) -> None:
    data = rows(sync_dir / "summary.csv")
    fig, ax = plt.subplots(figsize=(8, 4.2))
    for topo, color, dx in (("mesh", BLUE, -0.12), ("star", ORANGE, 0.12)):
        xs, med, lo, hi = [], [], [], []
        for x, sc in enumerate(SCENARIOS):
            vals = [float(r["prop_p95"]) / 1000 for r in data if r["topology"] == topo and r["scenario"] == sc]
            if not vals:
                continue
            m, a, b = spread(vals)
            xs.append(x + dx)
            med.append(m)
            lo.append(m - a)
            hi.append(b - m)
        ax.errorbar(xs, med, yerr=[lo, hi], fmt="o", ms=8, color=color, ecolor=color, elinewidth=2,
                    capsize=0, mec=SURFACE, mew=2, label=topo, zorder=3)
    ax.set_yscale("log")
    plain_log(ax)
    ax.set_xticks(range(len(SCENARIOS)), [LABELS[s] for s in SCENARIOS])
    ax.set_ylabel("p95 propagation latency (s, log scale)")
    ax.set_title("A second hop costs 2.4x; 50% loss costs the mesh 26x")
    ax.legend(title="median, range over repeats", loc="upper left", alignment="left")
    fig.tight_layout()
    fig.savefig(out / "sync-latency.png", dpi=150)
    plt.close(fig)


SKEWS = [-2000, -500, 0, 2000]


def clock_skew(clock_dir: Path, out: Path) -> None:
    data = rows(clock_dir / "summary.csv")
    fig, ax = plt.subplots(figsize=(8, 4))
    for clock, color, dx in (("hlc", BLUE, -0.1), ("wall", ORANGE, 0.1)):
        xs, med, lo, hi = [], [], [], []
        for x, sk in enumerate(SKEWS):
            vals = [int(r["ingest_lost"]) + int(r["merge_regress"]) for r in data
                    if r["clock"] == clock and int(r["skew_ms"]) == sk]
            if not vals:
                continue
            m, a, b = spread(vals)
            xs.append(x + dx)
            med.append(m)
            lo.append(m - a)
            hi.append(b - m)
        ax.errorbar(xs, med, yerr=[lo, hi], fmt="o", ms=8, color=color, ecolor=color, elinewidth=2,
                    capsize=0, mec=SURFACE, mew=2, label="hybrid logical clock" if clock == "hlc" else "wall clock",
                    zorder=3)
    ax.set_xticks(range(len(SKEWS)), [f"{s:+d} ms" if s else "0" for s in SKEWS])
    ax.set_xlabel("node c's clock error (reports arrive once a second)")
    ax.set_ylabel("reports that lost to an older one, per run")
    ax.set_title("Wall clock: ~313 lost writes a run at 2 s skew. HLC: none at any skew")
    ax.legend(title="median, range over repeats", loc="center", alignment="left")
    fig.tight_layout()
    fig.savefig(out / "clock-skew.png", dpi=150)
    plt.close(fig)


def cell(vals: list[str | None], n: int) -> str:
    """Median and min-max of a summary column over repeats; blank cells are
    runs where the metric never happened, and are counted, not dropped."""
    if all(v is None for v in vals):
        return "n/a"  # a column this sweep predates
    if vals and not all(v == "" or v.replace(".", "", 1).lstrip("-").isdigit() for v in vals):
        return " ".join(sorted(set(vals)))  # text, such as the commit
    got = [float(v) for v in vals if v != ""]
    if not got:
        return "never"
    m, a, b = spread(got)
    def fmt(x: float) -> str:
        return f"{x:g}"  # a median of an even count can end in .5; show it, don't truncate
    out = fmt(m) if a == b else f"{fmt(m)} ({fmt(a)}-{fmt(b)})"
    return out if len(got) == n else f"{out}, {n - len(got)}/{n} never"


# Columns that only mean something when a run has a cut; blank otherwise, not "never".
CUT_COLS = {"merged_cut", "catchup_ms", "catchup_merge_ms", "diverge_cut", "recover_p95_ms", "recover_all_ms"}


def table(header: list[str], keys: list[tuple[str, ...]], cols: list[str],
          data: list[dict], key_cols: list[str]) -> None:
    print("| " + " | ".join(header) + " |")
    print("|" + "---|" * len(header))
    for k in keys:
        rs = [r for r in data if tuple(r[c] for c in key_cols) == k]
        if not rs:
            continue
        settled = sum(r["settled"] == "True" for r in rs)
        cut = rs[0]["scenario"] in ("partition", "hubdown")
        print("| " + " | ".join([*k, f"{settled}/{len(rs)}", *("" if c in CUT_COLS and not cut else cell([r.get(c) for r in rs], len(rs))
                                                            for c in cols)]) + " |")


def tables(sync_dir: Path, clock_dir: Path) -> None:
    sync = rows(sync_dir / "summary.csv")
    cols = ["settle_ms", "prop_p50", "prop_p95", "prop_p99", "prop_unresolved", "recover_p95_ms", "recover_all_ms",
            "merged_cut", "sync_err_rate", "wire_bytes_per_obs", "ingest_lost", "rejected", "aborted"]
    keys = [(t, s) for t in ("mesh", "star") for s in SCENARIOS]
    print(f"{sync_dir / 'summary.csv'}\n")
    table(["topology", "scenario", "settled", *cols], keys, cols, sync, ["topology", "scenario"])
    clock = rows(clock_dir / "summary.csv")
    cols = ["ingest_lost", "merge_regress", "wrong_final", "prop_p95", "rejected", "aborted"]
    keys = [(c, str(k)) for c in ("hlc", "wall") for k in SKEWS]
    print(f"\n{clock_dir / 'summary.csv'}\n")
    table(["clock", "skew_ms", "settled", *cols], keys, cols, clock, ["clock", "skew_ms"])


# The real-traffic, live and cloud tables, printed exactly as the README shows them.

def num(v: str | None) -> float | None:
    return None if v in ("", None) else float(v)


def fmt(x: float, kind: str) -> str:
    if kind == "pct":
        return f"{x * 100:.1f}%".replace(".0%", "%")
    return f"{round(x)}" if kind == "int" else f"{x:g}"


def rcell(rs: list[dict], col: str, kind: str = "ms", agg: str = "median") -> str:
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


def md(header: list[str], lines: list[list[str]]) -> None:
    print("| " + " | ".join(header) + " |")
    print("|" + "---|" * len(header))
    for line in lines:
        print("| " + " | ".join(line) + " |")
    print()


def real_tables(hour_dir: Path | None, base_dir: Path | None, cloud_dir: Path | None, control_dir: Path | None,
                live_dir: Path | None, sync_dir: Path) -> None:
    if hour_dir and base_dir:
        hour, base = rows(hour_dir / "summary.csv"), rows(base_dir / "summary.csv")
        print(f"\n{hour_dir / 'summary.csv'}, the no-loss row from {base_dir / 'summary.csv'}\n")
        conds = [("no loss (8 windows, below)", pick(base, topology="mesh", scenario="loss0", clock="hlc", skew_ms=0))]
        conds += [(label, pick(hour, scenario=sc)) for label, sc in
                  (("20% loss", "loss20"), ("50% loss", "loss50"), ("c cut 23.6 s", "partition"))]
        lines = []
        for label, rs in conds:
            live = [r for r in rs if num(r["prop_n"])]  # windows with any report to propagate
            cut = label.startswith("c cut")
            lines.append([label, settled(rs), rcell(rs, "settle_ms"), rcell(live, "prop_p50"), rcell(live, "prop_p95"),
                          rcell(live, "prop_p99"), rcell(live, "prop_unresolved", "pct"), rcell(live, "stale_p50"),
                          rcell(live, "stale_p99"), rcell(live, "diverge_mean", "pct"),
                          rcell(rs, "catchup_merge_ms") if cut else "", rcell(rs, "merged_cut") if cut else "",
                          rcell(rs, "sync_err_rate", "pct"), rcell(live, "wire_bytes_per_useful", "int")])
            print(f"{label}: {len(rs) - len(live)} windows with nothing to propagate")
        print()
        md(["network", "settled", "settle", "prop p50", "prop p95", "prop p99", "never delivered", "staleness p50",
            "staleness p99", "divergence", "catch-up", "merged during the cut", "sync errors", "bytes per useful update"],
           lines)
        fd = [r for r in pick(hour, scenario="loss20") if num(r["feed_delay_p50"]) is not None]
        print(f"feed delay over the {len(fd)} windows with reports: p50 {rcell(fd, 'feed_delay_p50')}, "
              f"p99 {rcell(fd, 'feed_delay_p99')}")
        reg = [num(r["catchup_ms"]) for r in pick(hour, scenario="partition")]
        print(f"registered catchup_ms: {sum(v is None for v in reg)} of {len(reg)} never completed; median counting "
              f"those as never: {statistics.median(float('inf') if v is None else v for v in reg)}")
    if base_dir:
        base = rows(base_dir / "summary.csv")
        print(f"\n{base_dir / 'summary.csv'}: the central server\n")
        lines = []
        for label, topo, sc in (("mesh, no loss", "mesh", "loss0"), ("star, no loss", "star", "loss0"),
                                ("star, 50% loss", "star", "loss50"), ("star, hub cut 20 s", "star", "hubdown")):
            rs = pick(base, topology=topo, scenario=sc, clock="hlc", skew_ms=0)
            lines.append([label, settled(rs), rcell(rs, "prop_p50"), rcell(rs, "prop_p95"),
                          rcell(rs, "prop_unresolved", "pct"), rcell(rs, "stale_p50"), rcell(rs, "stale_p99"),
                          rcell(rs, "diverge_mean", "pct"), rcell(rs, "merged_cut") if sc == "hubdown" else "",
                          rcell(rs, "sync_err_rate", "pct"), rcell(rs, "wire_bytes_per_useful", "int")])
        md(["configuration", "settled", "prop p50", "prop p95", "never delivered", "staleness p50", "staleness p99",
            "divergence", "merged during the cut", "sync errors", "bytes per useful update"], lines)
        print(f"{base_dir / 'summary.csv'}: the clocks, totals over the eight windows, range per window\n")
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
                assert len({tuple(r[col] for r in sorted(rs, key=lambda r: r["run_id"][-3:])) for rs in per}) == 1, \
                    (label, sc, col)
                cells.append(rcell(per[0], col, "n", "sum"))
            lines.append([label, ", ".join(names[k] for k in skews), nets[sc], *cells])
        md(["clock", "c's clock", "network", "lost local writes", "merges an older report won",
            "aircraft wrong at the end"], lines)
    if cloud_dir and control_dir:
        cloud, control = rows(cloud_dir / "summary.csv"), rows(control_dir / "summary.csv")
        reg = [r for r in rows(sync_dir / "summary.csv") if r["topology"] == "star"]
        print(f"\n{cloud_dir / 'summary.csv'} against the hub: {control_dir / 'summary.csv'} at the same commit, "
              f"and the star rows of {sync_dir / 'summary.csv'}\n")
        lines = []
        for sc, label in (("loss0", "no loss"), ("loss20", "20% loss"), ("partition", "c cut 20 s")):
            for who, rs in (("in-memory hub, same commit", pick(control, scenario=sc)),
                            ("cloud peer on DynamoDB Local", pick(cloud, scenario=sc)),
                            (f"in-memory hub, registered ({reg[0]['commit']})", pick(reg, scenario=sc))):
                lines.append([label, who, settled(rs), rcell(rs, "prop_p50"), rcell(rs, "prop_p95"), rcell(rs, "prop_p99"),
                              rcell(rs, "cloud_writes_per_report", "n") if "cloud" in who else ""])
        md(["network", "relay", "settled", "prop p50", "prop p95", "prop p99", "conditional writes per report"], lines)
    if live_dir:
        live = rows(live_dir / "summary.csv")
        print(f"\n{live_dir / 'summary.csv'}\n")
        lines = []
        for topo, sc, label in (("mesh", "loss0", "mesh, no loss"), ("mesh", "partition", "mesh, c cut 20 s"),
                                ("star", "hubdown", "star, hub cut 20 s")):
            rs = pick(live, topology=topo, scenario=sc)
            cut = sc != "loss0"
            lines.append([label, settled(rs), rcell(rs, "aircraft", "n"), rcell(rs, "prop_p50"), rcell(rs, "prop_p95"),
                          rcell(rs, "feed_delay_p50"), rcell(rs, "stale_p50"), rcell(rs, "stale_p99"),
                          rcell(rs, "catchup_merge_ms") if cut else "", rcell(rs, "merged_cut") if cut else ""])
        md(["configuration", "settled", "aircraft", "prop p50", "prop p95", "feed delay p50", "staleness p50",
            "staleness p99", "catch-up", "merged during the cut"], lines)


def strip(ax, x: float, vals: Sequence[float], color: str, label: str | None) -> None:
    """Every run as a dot, jittered across a narrow band, plus its median."""
    if not vals:
        return
    n = len(vals)
    xs = [x + (0.3 * i / (n - 1) - 0.15 if n > 1 else 0) for i in range(n)]  # evenly across the band
    ax.scatter(xs, vals, s=14, color=color, alpha=0.45, linewidths=0, zorder=2)
    ax.scatter([x], [statistics.median(vals)], s=90, color=color, edgecolors=SURFACE, linewidths=2,
               zorder=3, label=label)


def save(fig, out: Path, name: str, source: str) -> None:
    """Save with a source note under the plot, so the image stands on its own."""
    fig.text(0.01, 0.015, source, color=INK2, fontsize=8, ha="left", va="bottom", linespacing=1.4)
    fig.tight_layout(rect=(0, 0.015 + 0.035 * (source.count("\n") + 1), 1, 1))
    fig.savefig(out / name, dpi=150)
    plt.close(fig)


def commit_of(data: list[dict]) -> str:
    return data[0]["commit"].removesuffix("-dirty")  # the README says why the real-traffic runs carry -dirty


def real_latency(hour_dir: Path, base_dir: Path, out: Path) -> None:
    """Propagation p95 on the recorded hour: one dot per window."""
    hour, base = rows(hour_dir / "summary.csv"), rows(base_dir / "summary.csv")
    conds = [("no loss", base, "mesh", "loss0"), ("20% loss", hour, "mesh", "loss20"),
             ("50% loss", hour, "mesh", "loss50"), ("c cut 23.6 s", hour, "mesh", "partition"),
             ("no loss", base, "star", "loss0"), ("50% loss", base, "star", "loss50"),
             ("hub cut 20 s", base, "star", "hubdown")]
    fig, ax = plt.subplots(figsize=(9, 4.6))
    for x, (_, data, topo, sc) in enumerate(conds):
        vals = [float(r["prop_p95"]) / 1000 for r in pick(data, topology=topo, scenario=sc, clock="hlc", skew_ms=0)
                if r["prop_p95"] != ""]  # a window with nothing to propagate has no p95
        first = x == 0 or conds[x - 1][2] != topo
        strip(ax, x, vals, BLUE if topo == "mesh" else ORANGE,
              ("mesh" if topo == "mesh" else "star, through a hub") if first else None)
    ax.axvline(3.5, color=GRID, lw=1)
    ax.set_yscale("log")
    plain_log(ax)
    ax.set_xticks(range(len(conds)), [c[0] for c in conds])
    ax.set_ylabel("p95 propagation latency (s, log scale)")
    ax.set_title(REAL_LATENCY_TITLE)
    ax.legend(title="median; small dots are windows", loc="upper left", alignment="left")
    save(fig, out, "real-latency.png",
         f"Source: results/real-hour and results/real-base summary.csv, commit {commit_of(hour)}.\nNo loss and the "
         "star: the 8 baseline windows. 20%, 50% and the cut: 58 of the hour's 60 windows (2 had nothing to propagate).")


REAL_SKEWS = [-4000, -2000, 2000, 4000]


def real_clock(base_dir: Path, out: Path) -> None:
    """Lost local writes per window on the recorded hour, by clock and skew."""
    data = rows(base_dir / "summary.csv")
    fig, ax = plt.subplots(figsize=(9, 4.4))
    for clock, color, dx, label in (("hlc", BLUE, -0.18, "hybrid logical clock"), ("wall", ORANGE, 0.18, "wall clock")):
        for x, sk in enumerate(REAL_SKEWS):
            rs = sorted(pick(data, clock=clock, skew_ms=sk, scenario="loss0"), key=lambda r: r["run_id"][-3:])
            vals = [int(r["ingest_lost"]) for r in rs]
            strip(ax, x + dx, vals, color, label if x == 0 else None)
            if sum(vals):
                ax.annotate(f"{sum(vals)} in all; by window\n{', '.join(map(str, vals))}",
                            xy=(x + dx - 0.18, statistics.median(vals)), ha="right", va="center", color=INK2, fontsize=9)
    ax.set_xticks(range(len(REAL_SKEWS)), [f"{s // 1000:+d} s" for s in REAL_SKEWS])
    ax.set_xlabel("node c's clock error")
    ax.set_ylabel("local writes lost, per window")
    ax.set_title(REAL_CLOCK_TITLE)
    ax.legend(title="median; small dots are windows", loc="upper left", alignment="left")
    save(fig, out, "real-clock.png",
         f"Source: results/real-base/summary.csv (ingest_lost), commit {commit_of(data)}.\nThe mesh with no loss, on "
         "the same 8 windows of the recorded hour in every configuration.")


def cloud_relay(cloud_dir: Path, hub_dir: Path, out: Path) -> None:
    """The cloud peer against the in-memory hub at the same commit, same trace and scenarios."""
    cloud, hub = rows(cloud_dir / "summary.csv"), rows(hub_dir / "summary.csv")
    scens = [("loss0", "no loss"), ("loss20", "20% loss"), ("partition", "c cut 20 s")]
    fig, ax = plt.subplots(figsize=(9, 4.4))
    for x, (sc, _) in enumerate(scens):
        for vals, dx, color, label in (([float(r["prop_p95"]) / 1000 for r in pick(hub, scenario=sc)], -0.17, ORANGE,
                                        "in-memory hub"),
                                       ([float(r["prop_p95"]) / 1000 for r in pick(cloud, scenario=sc)], 0.17, AQUA,
                                        "cloud peer, on DynamoDB Local")):
            strip(ax, x + dx, vals, color, label if x == 0 else None)
            if x == 0:  # name the medians the title compares, outside each band; aqua is too light for color alone
                name, side = ("hub", -1) if color == ORANGE else ("cloud peer", 1)
                ax.annotate(f"{name} {statistics.median(vals):.2f} s", xy=(x + dx + side * 0.2, statistics.median(vals)),
                            ha="left" if side > 0 else "right", va="center", color=INK2, fontsize=9)
    ax.set_xlim(-0.8, 2.45)  # room for the hub's label left of its band
    ax.set_yscale("log")
    plain_log(ax)
    ax.set_xticks(range(len(scens)), [s[1] for s in scens])
    ax.set_ylabel("p95 propagation latency (s, log scale)")
    ax.set_title(CLOUD_TITLE)
    ax.legend(title="median; small dots are runs", loc="upper left", alignment="left")
    save(fig, out, "cloud-relay.png",
         f"Source: results/cloud and results/star-control summary.csv, commit {commit_of(cloud)}. The synthetic trace, "
         "star topology, 5 runs each.")


# Titles state what the data shows: each number in one matches its README table.
REAL_LATENCY_TITLE = "Real traffic: p95 0.62 s with no loss, 1.37 s at 20% loss, 24.8 s at 50%"
REAL_CLOCK_TITLE = "Clock 4 s fast: the wall clock lost 485 writes in 8 windows, the HLC none"
CLOUD_TITLE = "The cloud peer costs about 15% at p95 over the hub with no loss, inside the spread"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sync", type=Path, required=True)
    ap.add_argument("--clock", type=Path, required=True)
    ap.add_argument("--real-hour", type=Path)
    ap.add_argument("--real-base", type=Path)
    ap.add_argument("--cloud", type=Path)
    ap.add_argument("--star-control", type=Path, help="the in-memory hub at the cloud runs' commit")
    ap.add_argument("--live", type=Path)
    ap.add_argument("--out", type=Path, default=Path("docs"))
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    timeline(args.sync, args.out)
    sync_latency(args.sync, args.out)
    clock_skew(args.clock, args.out)
    wrote = ["partition-timeline.png", "sync-latency.png", "clock-skew.png"]
    if args.real_hour and args.real_base:
        real_latency(args.real_hour, args.real_base, args.out)
        real_clock(args.real_base, args.out)
        wrote += ["real-latency.png", "real-clock.png"]
    if args.cloud and args.star_control:
        cloud_relay(args.cloud, args.star_control, args.out)
        wrote.append("cloud-relay.png")
    print(f"wrote {', '.join(wrote)} in {args.out}\n")
    tables(args.sync, args.clock)
    real_tables(args.real_hour, args.real_base, args.cloud, args.star_control, args.live, args.sync)


if __name__ == "__main__":
    main()
