#!/usr/bin/env python3
"""Statistics and charts over the runs of a session.

    analyze.py <results directory>

Reads runs.csv (one line per run and measured value) and writes, next to it,

    summary.csv   per scenario, number of nodes and value: how many runs, the
                  median, the mean, the standard deviation, the least and the
                  most
    figures/      one chart per value, as PNG and as PDF: the median against
                  the number of nodes, a line per scenario, with the range of
                  the runs as bars and every run as a dot

and prints the summary. A value measured once has no deviation; the charts
then show the single run.
"""

import csv
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# The values that get a chart, with what the axis says.
CHARTS = {
    "agent_cpu_pct": "agent CPU, mean of the nodes (% of one core)",
    "agent_cpu_pct_max": "agent CPU, the busiest node (% of one core)",
    "agent_rss_mb": "agent memory, mean of the nodes (MB resident)",
    "agent_rss_mb_max": "agent memory, the largest node (MB resident)",
    "server_cpu_pct": "server CPU (% of one core)",
    "server_rss_mb": "server memory (MB resident)",
    "settle_s": "until every line is searchable (s)",
}

# The values that are not measurements but outcomes: shown as "k of n runs".
OUTCOMES = ("exactly_once", "failed")


def load(path):
    """runs.csv as {(scenario, nodes, metric): [values in the order of the rounds]}."""
    values = defaultdict(list)
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            try:
                value = float(row["value"])
            except ValueError:
                continue
            values[(row["scenario"], int(row["nodes"]), row["metric"])].append(value)
    return values


def summarize(values):
    rows = []
    for (scenario, nodes, metric), v in sorted(values.items()):
        rows.append({
            "scenario": scenario,
            "nodes": nodes,
            "metric": metric,
            "runs": len(v),
            "median": statistics.median(v),
            "mean": statistics.fmean(v),
            "stdev": statistics.stdev(v) if len(v) > 1 else 0.0,
            "min": min(v),
            "max": max(v),
        })
    return rows


def number(x):
    return f"{x:.0f}" if abs(x) >= 1000 or float(x).is_integer() else f"{x:.3g}"


def write_summary(rows, path):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["scenario", "nodes", "metric", "runs", "median", "mean", "stdev", "min", "max"])
        w.writeheader()
        for r in rows:
            w.writerow({k: (f"{v:.6g}" if isinstance(v, float) else v) for k, v in r.items()})


def print_summary(rows, values):
    scenarios = sorted({r["scenario"] for r in rows})
    for scenario in scenarios:
        sizes = sorted({r["nodes"] for r in rows if r["scenario"] == scenario})
        print(f"\n{scenario}")
        print(f"  {'':34}" + "".join(f"{f'{k} node(s)':>22}" for k in sizes))
        for metric in OUTCOMES:
            cells = []
            for k in sizes:
                v = values.get((scenario, k, metric), [])
                cells.append(f"{int(sum(v))} of {len(v)} runs" if v else "-")
            print(f"  {metric:34}" + "".join(f"{c:>22}" for c in cells))
        metrics = sorted({r["metric"] for r in rows if r["scenario"] == scenario and r["metric"] not in OUTCOMES})
        for metric in metrics:
            cells = []
            for k in sizes:
                r = next((r for r in rows if (r["scenario"], r["nodes"], r["metric"]) == (scenario, k, metric)), None)
                cells.append(f"{number(r['median'])} [{number(r['min'])}..{number(r['max'])}]" if r else "-")
            print(f"  {metric:34}" + "".join(f"{c:>22}" for c in cells))
    print("\n  median [least..most] over the runs")


def chart(values, metric, label, out):
    scenarios = sorted({s for (s, _, m) in values if m == metric})
    if not scenarios:
        return False
    fig, ax = plt.subplots(figsize=(6.4, 4.0))
    sizes = sorted({k for (_, k, m) in values if m == metric})
    top = max(max(v) for (_, _, m), v in values.items() if m == metric)
    for n, scenario in enumerate(scenarios):
        xs, medians, lows, highs = [], [], [], []
        # Scenarios side by side at each size, so that their bars do not hide each other.
        shift = (n - (len(scenarios) - 1) / 2) * 0.08
        for i, k in enumerate(sizes):
            v = values.get((scenario, k, metric))
            if not v:
                continue
            m = statistics.median(v)
            xs.append(i + shift)
            medians.append(m)
            lows.append(m - min(v))
            highs.append(max(v) - m)
            ax.scatter([i + shift] * len(v), v, s=10, color=f"C{n}", alpha=0.35, zorder=2)
        ax.errorbar(xs, medians, yerr=[lows, highs], marker="o", capsize=3, color=f"C{n}", label=scenario, zorder=3)
    ax.set_xticks(range(len(sizes)), [str(k) for k in sizes])
    ax.set_xlabel("nodes")
    ax.set_ylabel(label)
    ax.set_ylim(bottom=0, top=top * 1.15 if top > 0 else 1)
    ax.grid(axis="y", alpha=0.3)
    ax.legend(title="median, range of the runs", frameon=False)
    fig.tight_layout()
    for ext in ("png", "pdf"):
        fig.savefig(out / f"{metric}.{ext}", dpi=150)
    plt.close(fig)
    return True


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    results = Path(sys.argv[1])
    values = load(results / "runs.csv")
    if not values:
        sys.exit(f"{results / 'runs.csv'} has no runs")
    rows = summarize(values)
    write_summary(rows, results / "summary.csv")
    figures = results / "figures"
    figures.mkdir(exist_ok=True)
    drawn = [m for m, label in CHARTS.items() if chart(values, m, label, figures)]
    print_summary(rows, values)
    print(f"\n  {results / 'summary.csv'}; {len(drawn)} charts in {figures}")


if __name__ == "__main__":
    main()
