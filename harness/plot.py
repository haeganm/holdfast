"""Draw the README figures from harness results.

    python harness/plot.py --sync results/<sync run dir> --clock results/<clock run dir> --out docs

Writes partition-timeline.png, sync-latency.png and clock-skew.png. Every
point drawn is a row of summary.csv or polls/*.csv in the given directories;
nothing is smoothed or filtered.
"""

from __future__ import annotations

import argparse
import csv
import statistics
from collections.abc import Sequence
from pathlib import Path

import matplotlib

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
        ax.annotate(f"node {node}", (xs[-1], ys[-1]), xytext=(4, 0), textcoords="offset points",
                    va="center", color=INK2)
    end = float(summary["duration_ms"]) / 1000
    ax.axvline(end, color=INK2, lw=0.8, ls=":")
    ax.text(end, 0.97, " ingest stops", transform=ax.get_xaxis_transform(), va="top", color=INK2)
    ax.set_xlabel("seconds since the trace started")
    ax.set_ylabel("p95 lag behind the freshest report (s)")
    ax.set_title("Per-node lag during a 20 s partition of node c (mesh, one run)")
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
    ax.set_xticks(range(len(SCENARIOS)), [LABELS[s] for s in SCENARIOS])
    ax.set_ylabel("p95 propagation latency (s, log scale)")
    ax.set_title("Report propagation latency by network condition")
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
    ax.set_ylabel("merges that kept the older report, per run")
    ax.set_title("Merges that kept the older report, by clock and skew")
    ax.legend(title="median, range over repeats", loc="upper left", alignment="left")
    fig.tight_layout()
    fig.savefig(out / "clock-skew.png", dpi=150)
    plt.close(fig)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sync", type=Path, required=True)
    ap.add_argument("--clock", type=Path, required=True)
    ap.add_argument("--out", type=Path, default=Path("docs"))
    args = ap.parse_args()
    args.out.mkdir(parents=True, exist_ok=True)
    timeline(args.sync, args.out)
    sync_latency(args.sync, args.out)
    clock_skew(args.clock, args.out)
    print(f"wrote {args.out}/partition-timeline.png, sync-latency.png, clock-skew.png")


if __name__ == "__main__":
    main()
