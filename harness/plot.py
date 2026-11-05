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


def cell(vals: list[str], n: int) -> str:
    """Median and min-max of a summary column over repeats; blank cells are
    runs where the metric never happened, and are counted, not dropped."""
    got = [float(v) for v in vals if v != ""]
    if not got:
        return "never"
    m, a, b = spread(got)
    def fmt(x: float) -> str:
        return f"{x:g}"  # a median of an even count can end in .5; show it, don't truncate
    out = fmt(m) if a == b else f"{fmt(m)} ({fmt(a)}-{fmt(b)})"
    return out if len(got) == n else f"{out}, {n - len(got)}/{n} never"


def table(header: list[str], keys: list[tuple[str, ...]], cols: list[str],
          data: list[dict], key_cols: list[str]) -> None:
    print("| " + " | ".join(header) + " |")
    print("|" + "---|" * len(header))
    for k in keys:
        rs = [r for r in data if tuple(r[c] for c in key_cols) == k]
        if not rs:
            continue
        settled = sum(r["settled"] == "True" for r in rs)
        print("| " + " | ".join([*k, f"{settled}/{len(rs)}", *(cell([r[c] for r in rs], len(rs)) for c in cols)]) + " |")


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


def strip(ax, x: float, vals: list[float], color: str, label: str | None) -> None:
    """Every run as a dot, jittered across a narrow band, plus its median."""
    if not vals:
        return
    n = len(vals)
    xs = [x + 0.18 * ((i % 9) / 8 - 0.5) for i in range(n)]
    ax.scatter(xs, vals, s=14, color=color, alpha=0.45, linewidths=0, zorder=2)
    ax.scatter([x], [statistics.median(vals)], s=90, color=color, edgecolors=SURFACE, linewidths=2,
               zorder=3, label=label)


def real_latency(hour_dir: Path, base_dir: Path, out: Path) -> None:
    """Propagation p95 on the recorded hour: one dot per window."""
    hour, base = rows(hour_dir / "summary.csv"), rows(base_dir / "summary.csv")
    conds = [("no loss", base, "mesh", "loss0"), ("20% loss", hour, "mesh", "loss20"),
             ("50% loss", hour, "mesh", "loss50"), ("c cut 20 s", hour, "mesh", "partition"),
             ("star, no loss", base, "star", "loss0"), ("star, 50% loss", base, "star", "loss50"),
             ("star, hub cut", base, "star", "hubdown")]
    fig, ax = plt.subplots(figsize=(9, 4.4))
    for x, (_, data, topo, sc) in enumerate(conds):
        vals = [float(r["prop_p95"]) / 1000 for r in data if r["topology"] == topo and r["scenario"] == sc
                and r["clock"] == "hlc" and int(r["skew_ms"]) == 0 and r["prop_p95"] != ""]
        strip(ax, x, vals, BLUE if topo == "mesh" else ORANGE, None)
    ax.set_yscale("log")
    ax.set_xticks(range(len(conds)), [c[0] for c in conds])
    ax.set_ylabel("p95 propagation latency (s, log scale)")
    ax.set_title(REAL_LATENCY_TITLE)
    fig.tight_layout()
    fig.savefig(out / "real-latency.png", dpi=150)
    plt.close(fig)


REAL_SKEWS = [-4000, -2000, 2000, 4000]


def real_clock(base_dir: Path, out: Path) -> None:
    """Lost local writes per window on the recorded hour, by clock and skew."""
    data = rows(base_dir / "summary.csv")
    fig, ax = plt.subplots(figsize=(8, 4))
    for clock, color, dx, label in (("hlc", BLUE, -0.12, "hybrid logical clock"), ("wall", ORANGE, 0.12, "wall clock")):
        for x, sk in enumerate(REAL_SKEWS):
            vals = [int(r["ingest_lost"]) for r in data if r["clock"] == clock and int(r["skew_ms"]) == sk
                    and r["scenario"] == "loss0"]
            strip(ax, x + dx, vals, color, label if x == 0 else None)
    ax.set_xticks(range(len(REAL_SKEWS)), [f"{s:+d} ms" for s in REAL_SKEWS])
    ax.set_xlabel("node c's clock error")
    ax.set_ylabel("local writes lost, per window")
    ax.set_title(REAL_CLOCK_TITLE)
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(out / "real-clock.png", dpi=150)
    plt.close(fig)


def cloud_relay(cloud_dir: Path, sync_dir: Path, out: Path) -> None:
    """The cloud peer against the in-memory hub, same trace and scenarios."""
    cloud, sync = rows(cloud_dir / "summary.csv"), rows(sync_dir / "summary.csv")
    scens = [("loss0", "no loss"), ("loss20", "20% loss"), ("partition", "c cut 20 s")]
    fig, ax = plt.subplots(figsize=(8, 4))
    for x, (sc, _) in enumerate(scens):
        hub = [float(r["prop_p95"]) / 1000 for r in sync if r["topology"] == "star" and r["scenario"] == sc]
        rel = [float(r["prop_p95"]) / 1000 for r in cloud if r["scenario"] == sc]
        strip(ax, x - 0.15, hub, ORANGE, "in-memory hub" if x == 0 else None)
        strip(ax, x + 0.15, rel, AQUA, "cloud peer on DynamoDB" if x == 0 else None)
    ax.set_yscale("log")
    ax.set_xticks(range(len(scens)), [s[1] for s in scens])
    ax.set_ylabel("p95 propagation latency (s, log scale)")
    ax.set_title(CLOUD_TITLE)
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(out / "cloud-relay.png", dpi=150)
    plt.close(fig)


# Titles state what the data shows; set them after looking at the figures.
REAL_LATENCY_TITLE = "Propagation on the recorded hour, one dot per window"
REAL_CLOCK_TITLE = "Lost local writes on the recorded hour, by clock and skew"
CLOUD_TITLE = "The cloud relay against the in-memory hub"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--sync", type=Path, required=True)
    ap.add_argument("--clock", type=Path, required=True)
    ap.add_argument("--real-hour", type=Path)
    ap.add_argument("--real-base", type=Path)
    ap.add_argument("--cloud", type=Path)
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
    if args.cloud:
        cloud_relay(args.cloud, args.sync, args.out)
        wrote.append("cloud-relay.png")
    print(f"wrote {', '.join(wrote)} in {args.out}\n")
    tables(args.sync, args.clock)


if __name__ == "__main__":
    main()
