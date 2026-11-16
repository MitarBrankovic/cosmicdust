#!/usr/bin/env python3
"""Statistics and charts over the runs of a session.

    analyze.py <results directory>

Reads runs.csv (one line per run and measured value) and writes, next to it,

    summary.csv   per experiment, place on its axis, series and value: how
                  many runs, the median, the mean, the standard deviation, the
                  least and the most
    figures/      one chart per experiment and value, as PNG and as PDF: the
                  median, with the range of the runs as bars and every run as
                  a dot. Where the axis is a number (nodes, lines per second)
                  the chart is a line per series; where it is a name (a case)
                  it is a group of bars per name

and prints the summary. A value measured once has no deviation; the charts
then show the single run.
"""

import csv
import statistics
import sys
import textwrap
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# Per experiment: what its axis is, the order of its places if they are
# names, and the values that get a chart, with what the chart's axis says. A
# name that ends in * stands for every value that begins so, each with a chart
# of its own; what the * stood for goes into the label's {}.
EXPERIMENTS = {
    "nodes": {
        "axis": "nodes",
        "charts": {
            "agent_cpu_pct": "agent CPU, mean of the nodes (% of one core)",
            "agent_cpu_pct_max": "agent CPU, the busiest node (% of one core)",
            "agent_rss_mb": "agent memory, mean of the nodes (MB resident)",
            "agent_rss_mb_max": "agent memory, the largest node (MB resident)",
            "server_cpu_pct": "server CPU (% of one core)",
            "server_rss_mb": "server memory (MB resident)",
            "settle_s": "until every line is searchable (s)",
        },
    },
    "adverse": {
        "axis": "nodes",
        "charts": {
            "settle_s": "until every line is searchable or accounted for (s)",
            "agent_cpu_pct": "agent CPU, mean of the nodes (% of one core)",
            "agent_cpu_pct_max": "agent CPU, the busiest node (% of one core)",
            "agent_rss_mb_max": "agent memory, the largest node (MB resident)",
            "dropped_pct": "lines dropped for room, each drop recorded (%)",
            "link_lost_pct": "packets the narrow link lost (%)",
        },
    },
    "compare": {
        "axis": "case",
        "order": ["idle0", "idle200", "load1k", "load5k", "load10k", "edge25", "edge10"],
        "charts": {
            "collector_cpu_pct": "CPU of the collector (% of one core)",
            "collector_anon_mb": "anonymous memory of the collector (MB)",
            "collector_peak_mb": "memory of the collector at its largest, page cache included (MB)",
            "runtime_cpu_pct": "CPU of the node's runtime: dockerd, containerd, shims (% of one core)",
            "cpu_s_per_gb": "CPU seconds of the collector per GB of logs",
        },
    },
    "ceiling": {
        # What was asked of the generators; what they managed is written_per_s.
        "axis": "lines per second asked for",
        "charts": {
            "collector_cpu_pct": "CPU of the agent (% of one core)",
            "throttled_s": "time the agent was held back by its CPU limit (s)",
            "delivered_s": "until every line is at the server (s)",
            "unread_pct": "lines the agent has not read when the last one is written (%)",
            "behind_pct": "lines not yet searchable when the last one is written (%)",
            "settle_s": "until every line is searchable (s)",
            "written_per_s": "lines per second the generators managed",
            "runtime_cpu_pct": "CPU of the node's runtime: dockerd, containerd, shims (% of one core)",
        },
    },
    "compress": {
        "axis": "log",
        "charts": {
            "ratio_*": "bytes of the log files / bytes kept, a file holding {} of logs",
        },
    },
}

# The values that are not measurements but outcomes: shown as "k of n runs".
OUTCOMES = ("exactly_once", "failed")


def load(path):
    """runs.csv as {(experiment, x, series, metric): [values in the order of the rounds]}."""
    values = defaultdict(list)
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            try:
                value = float(row["value"])
            except ValueError:
                continue
            values[(row["experiment"], row["x"], row["series"], row["metric"])].append(value)
    return values


def numeric(xs):
    try:
        return [float(x) for x in xs] is not None
    except ValueError:
        return False


def places(experiment, xs):
    """The places of an experiment's axis, in the order they are shown."""
    xs = set(xs)
    if numeric(xs):
        return sorted(xs, key=float)
    order = EXPERIMENTS.get(experiment, {}).get("order", [])
    return [x for x in order if x in xs] + sorted(xs - set(order))


def summarize(values):
    rows = []
    for (experiment, x, series, metric), v in values.items():
        rows.append({
            "experiment": experiment, "x": x, "series": series, "metric": metric, "runs": len(v),
            "median": statistics.median(v), "mean": statistics.fmean(v),
            "stdev": statistics.stdev(v) if len(v) > 1 else 0.0, "min": min(v), "max": max(v),
        })
    return rows


def number(x):
    return f"{x:.0f}" if abs(x) >= 1000 or float(x).is_integer() else f"{x:.3g}"


def write_summary(rows, values, path):
    fields = ["experiment", "x", "series", "metric", "runs", "median", "mean", "stdev", "min", "max"]
    order = {e: places(e, [x for (ee, x, _, _) in values if ee == e]) for e in {r["experiment"] for r in rows}}
    rows = sorted(rows, key=lambda r: (r["experiment"], order[r["experiment"]].index(r["x"]), r["series"], r["metric"]))
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: (f"{v:.6g}" if isinstance(v, float) else v) for k, v in r.items()})


def print_summary(values):
    for experiment in sorted({e for (e, _, _, _) in values}):
        axis = EXPERIMENTS.get(experiment, {}).get("axis", "x")
        xs = places(experiment, [x for (e, x, _, _) in values if e == experiment])
        for series in sorted({s for (e, _, s, _) in values if e == experiment}):
            print(f"\n{experiment}: {series}")
            print(f"  {axis:34}" + "".join(f"{x:>22}" for x in xs))
            metrics = sorted({m for (e, _, s, m) in values if e == experiment and s == series})
            for metric in [m for m in OUTCOMES if m in metrics] + [m for m in metrics if m not in OUTCOMES]:
                cells = []
                for x in xs:
                    v = values.get((experiment, x, series, metric))
                    if not v:
                        cells.append("-")
                    elif metric in OUTCOMES:
                        cells.append(f"{int(sum(v))} of {len(v)} runs")
                    else:
                        cells.append(f"{number(statistics.median(v))} [{number(min(v))}..{number(max(v))}]")
                print(f"  {metric:34}" + "".join(f"{c:>22}" for c in cells))
    print("\n  median [least..most] over the runs")


def named(values, experiment, name):
    """The values a chart's name stands for, and for each what goes into the label."""
    if not name.endswith("*"):
        return [(name, "")]
    stem = name[:-1]
    found = {m for (e, _, _, m) in values if e == experiment and m.startswith(stem)}
    return [(m, m[len(stem):]) for m in sorted(found, key=lambda m: (len(m), m))]


def chart(values, experiment, metric, label, out):
    keys = [(x, s) for (e, x, s, m) in values if e == experiment and m == metric]
    if not keys:
        return False
    xs = places(experiment, [x for x, _ in keys])
    series = sorted({s for _, s in keys})
    lines = numeric(xs)
    top = max(max(values[(experiment, x, s, metric)]) for x, s in keys)
    fig, ax = plt.subplots(figsize=(6.4, 4.0) if lines else (max(6.4, 1.1 * len(xs) + 2), 4.0))
    width = 0.8 / len(series)
    for n, name in enumerate(series):
        at, medians, lows, highs = [], [], [], []
        for i, x in enumerate(xs):
            v = values.get((experiment, x, name, metric))
            if not v:
                continue
            m = statistics.median(v)
            # Series side by side at each place, so that they do not hide each other.
            if lines:
                span = (float(xs[-1]) - float(xs[0])) or 1.0
                pos = float(x) + (n - (len(series) - 1) / 2) * span * 0.012
            else:
                pos = i + (n - (len(series) - 1) / 2) * width
            at.append(pos)
            medians.append(m)
            lows.append(m - min(v))
            highs.append(max(v) - m)
            ax.scatter([pos] * len(v), v, s=10, color="black" if not lines else f"C{n}", alpha=0.35, zorder=4)
        if lines:
            ax.errorbar(at, medians, yerr=[lows, highs], marker="o", capsize=3, color=f"C{n}", label=name, zorder=3)
        else:
            ax.bar(at, medians, width=width * 0.9, yerr=[lows, highs], capsize=3, color=f"C{n}", label=name, zorder=3)
    if lines:
        ax.set_xticks([float(x) for x in xs], xs)
    else:
        ax.set_xticks(range(len(xs)), xs)
    ax.set_xlabel(EXPERIMENTS.get(experiment, {}).get("axis", "x"))
    # A long label on two lines: on one it would not fit the chart's height.
    ax.set_ylabel("\n".join(textwrap.wrap(label, 44)))
    ax.set_ylim(bottom=0, top=top * 1.15 if top > 0 else 1)
    ax.grid(axis="y", alpha=0.3)
    ax.legend(title="median, range of the runs", frameon=False)
    fig.tight_layout()
    for ext in ("png", "pdf"):
        fig.savefig(out / f"{experiment}-{metric}.{ext}", dpi=150)
    plt.close(fig)
    return True


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    results = Path(sys.argv[1])
    values = load(results / "runs.csv")
    if not values:
        sys.exit(f"{results / 'runs.csv'} has no runs")
    write_summary(summarize(values), values, results / "summary.csv")
    figures = results / "figures"
    figures.mkdir(exist_ok=True)
    drawn = 0
    for experiment, spec in EXPERIMENTS.items():
        for name, label in spec["charts"].items():
            for metric, rest in named(values, experiment, name):
                drawn += chart(values, experiment, metric, label.replace("{}", rest), figures)
    print_summary(values)
    print(f"\n  {results / 'summary.csv'}; {drawn} charts in {figures}")


if __name__ == "__main__":
    main()
