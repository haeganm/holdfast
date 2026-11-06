"""Draw the README figures from harness results.

    python harness/plot.py --sync results/<sync run dir> --clock results/<clock run dir> --out docs

Writes partition-timeline.png, sync-latency.png and clock-skew.png, and
prints the README's results tables, which tables.py builds. Every point drawn
and every number printed is a row of summary.csv or polls/*.csv in the given
directories; nothing is smoothed or filtered.
"""

from __future__ import annotations

import argparse
import statistics
import sys
from collections.abc import Sequence
from pathlib import Path

import matplotlib
import matplotlib.ticker

matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, str(Path(__file__).resolve().parent))  # python -I leaves the script's folder off the path
import tables
from tables import LABELS, SCENARIOS, SKEWS, pick, rows

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
    print("\n".join(tables.report(args.sync, args.clock, args.real_hour, args.real_base, args.cloud, args.star_control,
                                  args.live)))


if __name__ == "__main__":
    main()
