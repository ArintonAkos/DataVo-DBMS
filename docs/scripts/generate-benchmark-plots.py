#!/usr/bin/env python3
"""Generate the macOS benchmark figures from the raw result files of one run.

Every number is parsed from docs/public/benchmarks/<RUN>/ (the markdown tables printed by
Research.Benchmark.Host, the BenchmarkDotNet CSVs, and vector-recall-experiment.txt), so a figure
can always be traced to a file in the repository.

    python3 docs/scripts/generate-benchmark-plots.py
"""
from __future__ import annotations

import csv
import os
import re
from pathlib import Path

HERE = Path(__file__).resolve().parent
os.environ.setdefault("MPLCONFIGDIR", str((HERE / ".matplotlib-cache").resolve()))

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np

RUN = "2026-11-12"
OUT = HERE.parent / "public" / "benchmarks"
RAW = OUT / RUN
CAPTION = "Apple M4 Pro, macOS 26.3, .NET 10.0.3, single run"

mpl.rcParams.update({
    "figure.dpi": 150, "savefig.dpi": 220, "font.family": "DejaVu Sans",
    "axes.titleweight": "bold", "axes.titlesize": 13, "axes.labelsize": 10.5,
    "axes.edgecolor": "#222831", "axes.linewidth": 0.8, "text.color": "#1F2933",
    "xtick.color": "#222831", "ytick.color": "#222831", "grid.color": "#D7DEE8",
    "grid.linewidth": 0.65, "legend.frameon": False, "axes.spines.top": False,
    "axes.spines.right": False, "axes.axisbelow": True,
})

COLOR = {
    "DataVo": "#0072B2", "DataVo Relaxed": "#2FBF71", "DataVo Production": "#005A8C",
    "DataVo Flat": "#009E73", "DataVo HNSW": "#0072B2", "DataVo HNSW diversity": "#56B4E9",
    "SQLite": "#E69F00", "SQLite full": "#B07400", "DuckDB": "#D55E00",
    "DuckDB 1 thread": "#F0A27A", "LiteDB": "#CC79A7",
}


def logaxis(ax) -> None:
    """Log y axis with plain, comma-formatted tick labels at 1, 2 and 5 per decade."""
    from matplotlib.ticker import FuncFormatter, LogLocator
    ax.set_yscale("log")
    ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1.0, 2.0, 5.0), numticks=40))
    ax.yaxis.set_minor_locator(LogLocator(base=10, subs=(), numticks=40))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}" if v >= 1 else f"{v:g}"))


def num(text: str) -> float:
    return float(text.replace(",", "")) if text.strip().lower() not in ("n/a", "nan") else float("nan")


def table(name: str) -> list[dict[str, str]]:
    lines = [l for l in (RAW / name).read_text().splitlines() if l.startswith("|")]
    head = [c.strip() for c in lines[0].strip("|").split("|")]
    return [dict(zip(head, [c.strip() for c in l.strip("|").split("|")])) for l in lines[2:]]


def label(engine: str) -> str:
    e = re.sub(r"\s*\[DataVo\.Core[^\]]*\]", "", engine)
    e = re.sub(r"\s*\(\d+ threads\)", "", e)
    return {
        "DataVo (LSM Production)": "DataVo Production", "DataVo (LSM Relaxed)": "DataVo Relaxed",
        "SQLite (WAL,normal)": "SQLite", "SQLite (WAL,full)": "SQLite full",
        "DataVo-Flat": "DataVo Flat", "DataVo-HNSW-Diversity": "DataVo HNSW diversity",
    }.get(e, e)


def save(fig, stem: str) -> None:
    fig.text(0.01, -0.045, f"{CAPTION}; data: docs/public/benchmarks/{RUN}/", fontsize=7.5, color="#6B7280")
    fig.savefig(OUT / f"{stem}.png", bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print("wrote", OUT / f"{stem}.png")


def bars(ax, names, values, fmt, log=True, ylabel=""):
    cols = [COLOR.get(n, "#888") for n in names]
    b = ax.bar(names, values, color=cols, width=0.62)
    if log:
        logaxis(ax)
    ax.set_ylabel(ylabel)
    ax.grid(axis="y")
    for r, v in zip(b, values):
        ax.annotate(fmt(v), (r.get_x() + r.get_width() / 2, v), ha="center", va="bottom",
                    xytext=(0, 3), textcoords="offset points", fontsize=8.5, fontweight="bold")
    ax.margins(y=0.18)


def disk_crud():
    rows = table("disk-crud-wal.md")
    names = [label(r["Engine Name"]) for r in rows]
    vals = [num(r["Total Execution Time (ms)"]) for r in rows]
    fig, ax = plt.subplots(figsize=(8, 4.6))
    bars(ax, names, vals, lambda v: f"{v:,.0f} ms", ylabel="Total time, ms (log scale)")
    ax.set_title("Disk CRUD: 20,000 inserts then 20,000 point updates")
    save(fig, "disk-crud-wal-time")


def thread_scaling():
    series: dict[str, dict[int, float]] = {}
    for r in table("thread-scaling.md"):
        m = re.search(r"\((\d+) threads\)", r["Engine Name"])
        series.setdefault(label(r["Engine Name"]), {})[int(m.group(1))] = num(r["OPS"])
    fig, ax = plt.subplots(figsize=(8.4, 4.8))
    for name, pts in series.items():
        xs = sorted(pts)
        ax.plot(xs, [pts[x] for x in xs], marker="o", lw=2, color=COLOR.get(name), label=name)
    ax.set_xscale("log", base=2)
    ax.set_xticks([1, 2, 4, 8, 16, 32], [1, 2, 4, 8, 16, 32])
    logaxis(ax)
    ax.set_xlabel("Threads")
    ax.set_ylabel("Operations per second (log scale)")
    ax.set_title("Thread scaling: 100,000 reads + 10,000 updates")
    ax.grid(True)
    ax.legend(ncol=2, loc="center right", bbox_to_anchor=(1.0, 0.43))
    save(fig, "thread-scaling-throughput")


def ycsb():
    rows = table("ycsb-mixed.md")
    names = [label(r["Engine Name"]) for r in rows]
    vals = [num(r["Write P99 Latency (ms)"]) for r in rows]
    fig, ax = plt.subplots(figsize=(8, 4.6))
    bars(ax, names, vals, lambda v: f"{v:.3g} ms", ylabel="Write P99 latency, ms (log scale)")
    ax.set_title("YCSB mixed (80/20, 12 threads): write P99")
    save(fig, "ycsb-write-p99")


def vector_total():
    rows = table("vector-search.md")
    rows.sort(key=lambda r: num(r["Total Execution Time (ms)"]))
    names = [label(r["Engine Name"]) for r in rows]
    vals = [num(r["Total Execution Time (ms)"]) for r in rows]
    fig, ax = plt.subplots(figsize=(8.8, 4.6))
    bars(ax, names, vals, lambda v: f"{v:,.0f} ms", ylabel="Insert + build + 100 queries, ms (log scale)")
    ax.set_title("Vector search: 10,000 x 1536-dim vectors, top-10")
    save(fig, "vector-search-total-time")


def vector_recall():
    rx = re.compile(r"N=\s*(\d+) (\w+)\s+build flat\s+(\d+) ms hnsw\s+(\d+) ms \| query mean flat\s+([\d.]+) ms hnsw\s+([\d.]+) ms \| HNSW recall@10 vs exact ([\d.]+) %")
    data: dict[str, list[tuple]] = {"uniform": [], "clustered": []}
    for line in (RAW / "vector-recall-experiment.txt").read_text().splitlines():
        m = rx.search(line)
        if m:
            n, kind, bf, bh, qf, qh, rec = m.groups()
            data[kind].append((int(n), float(qf), float(qh), float(rec)))
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.6), sharey=True)
    titles = {"uniform": "Uniform random vectors (the benchmark's data)",
              "clustered": "Clustered vectors (50 centroids + noise)"}
    for ax, kind in zip(axes, ["uniform", "clustered"]):
        pts = data[kind]
        x = np.arange(len(pts))
        w = 0.36
        ax.bar(x - w / 2, [p[1] for p in pts], w, color=COLOR["DataVo Flat"], label="Flat (exact)")
        ax.bar(x + w / 2, [p[2] for p in pts], w, color=COLOR["DataVo HNSW"], label="HNSW, EfSearch 64")
        for xi, p in zip(x, pts):
            ax.annotate(f"recall {p[3]:.0f}%", (xi + w / 2, p[2]), ha="center", va="bottom",
                        xytext=(0, 3), textcoords="offset points", fontsize=8.5, fontweight="bold")
        ax.set_xticks(x, [f"N = {p[0]:,}" for p in pts])
        logaxis(ax)
        ax.set_title(titles[kind], fontsize=11)
        ax.grid(axis="y")
        ax.margins(y=0.25)
    axes[0].set_ylabel("Mean query latency, ms (log scale)")
    axes[0].legend(loc="upper left")
    fig.suptitle("HNSW versus exact Flat search, 1536 dimensions, top-10", fontweight="bold", y=1.0)
    save(fig, "vector-hnsw-vs-flat")


def bdn_rows(cls: str) -> list[dict[str, str]]:
    with open(RAW / "bdn" / f"DataVo.Benchmarks.{cls}-report.csv", newline="") as f:
        return list(csv.DictReader(f))


def to_us(text: str) -> float:
    v, unit = text.split()
    return float(v.replace(",", "")) * {"ns": 1e-3, "μs": 1.0, "us": 1.0, "ms": 1e3, "s": 1e6}[unit]


ENGINE = {"DataVo": "DataVo", "Sqlite": "SQLite", "DuckDb": "DuckDB", "DuckDb1Thread": "DuckDB 1 thread"}
ORDER = ["DataVo", "SQLite", "DuckDB 1 thread", "DuckDB"]


def bdn_aggregates():
    rows = bdn_rows("FilteredAggregateBenchmarks")
    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.8), sharey=True)
    for ax, storage in zip(axes, ["Memory", "File"]):
        queries = ["Count", "Sum", "Min", "Max"]
        x = np.arange(len(queries))
        w = 0.2
        for i, eng in enumerate(ORDER):
            vals = []
            for q in queries:
                for r in rows:
                    e, _, qq = r["Method"].partition("_")
                    if ENGINE[e] == eng and qq == q and r["Storage"] == storage:
                        vals.append(to_us(r["Mean"]))
            ax.bar(x + (i - 1.5) * w, vals, w, color=COLOR[eng], label=eng)
        ax.set_xticks(x, queries)
        logaxis(ax)
        ax.set_title(f"{storage}-backed")
        ax.grid(axis="y")
    axes[0].set_ylabel("Mean time per query, us (log scale)")
    axes[0].legend(ncol=4, loc="upper center", bbox_to_anchor=(1.1, -0.1), fontsize=9)
    fig.suptitle("Filtered aggregate over 200,000 rows (WHERE Level >= @min)", fontweight="bold", y=1.0)
    save(fig, "bdn-filtered-aggregates")


def bdn_lookup_insert():
    fig, axes = plt.subplots(1, 2, figsize=(11.5, 4.6))
    for ax, cls, title, unit_div, ylabel in [
        (axes[0], "PointLookupBenchmarks", "One row by primary key (100,000 rows)", 1.0, "Mean time per lookup, us (log scale)"),
        (axes[1], "BulkInsertBenchmarks", "Load 100,000 rows into an empty table", 1e3, "Mean load time, ms (log scale)"),
    ]:
        rows = bdn_rows(cls)
        x = np.arange(2)
        w = 0.2
        for i, eng in enumerate(ORDER):
            vals = []
            for storage in ["Memory", "File"]:
                for r in rows:
                    if ENGINE[r["Method"]] == eng and r["Storage"] == storage:
                        vals.append(to_us(r["Mean"]) / unit_div)
            ax.bar(x + (i - 1.5) * w, vals, w, color=COLOR[eng], label=eng)
        ax.set_xticks(x, ["Memory", "File"])
        logaxis(ax)
        ax.set_title(title, fontsize=11)
        ax.set_ylabel(ylabel)
        ax.grid(axis="y")
    axes[0].legend(ncol=4, loc="upper center", bbox_to_anchor=(1.1, -0.1), fontsize=9)
    save(fig, "bdn-lookup-and-insert")


if __name__ == "__main__":
    disk_crud()
    thread_scaling()
    ycsb()
    vector_total()
    vector_recall()
    bdn_aggregates()
    bdn_lookup_insert()
