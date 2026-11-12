#!/usr/bin/env python3
"""Generate the cross-platform figures from the GitHub Actions run of 12 November 2026.

Inputs, all checked in under docs/public/benchmarks/2026-11-12/:
  ci/linux-x64/benchmark.log, ci/windows-x64/benchmark.log   Research.Benchmark.Host output
  ci/<platform>/bdn/*.csv                                      BenchmarkDotNet reports
  disk-crud-wal.md, vector-search.md, bdn/*.csv                the local Apple M4 Pro run, for contrast

    python3 docs/scripts/generate-ci-benchmark-plots.py
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
from matplotlib.ticker import FuncFormatter, LogLocator

OUT = HERE.parent / "public" / "benchmarks"
RAW = OUT / "2026-11-12"

mpl.rcParams.update({
    "figure.dpi": 150, "savefig.dpi": 220, "font.family": "DejaVu Sans",
    "axes.titleweight": "bold", "axes.titlesize": 12.5, "axes.labelsize": 10.5,
    "axes.edgecolor": "#222831", "axes.linewidth": 0.8, "text.color": "#1F2933",
    "xtick.color": "#222831", "ytick.color": "#222831", "grid.color": "#D7DEE8",
    "grid.linewidth": 0.65, "legend.frameon": False, "axes.spines.top": False,
    "axes.spines.right": False, "axes.axisbelow": True,
})

COLOR = {
    "DataVo": "#0072B2", "DataVo Production": "#005A8C", "DataVo Flat": "#009E73",
    "DataVo HNSW": "#56B4E9", "SQLite": "#E69F00", "SQLite full": "#B07400",
    "DuckDB 1 thread": "#F0A27A",
}
CAPTION = ("Linux: GitHub-hosted AMD EPYC 7763, 4 vCPU. Windows: AMD EPYC 9V74, 4 vCPU. M1 VM: GitHub-hosted macOS runner. "
           "macOS: Apple M4 Pro, local. Single run each; data: docs/public/benchmarks/2026-11-12/")


def logaxis(ax) -> None:
    ax.set_yscale("log")
    ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1.0, 2.0, 5.0), numticks=40))
    ax.yaxis.set_minor_locator(LogLocator(base=10, subs=(), numticks=40))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}" if v >= 1 else f"{v:g}"))


def save(fig, stem: str) -> None:
    fig.text(0.01, -0.13, CAPTION, fontsize=7, color="#6B7280")
    fig.savefig(OUT / f"{stem}.png", bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print("wrote", OUT / f"{stem}.png")


def md_rows(text: str) -> list[dict[str, str]]:
    """Every markdown table in `text`, concatenated, as dicts keyed by header cell."""
    rows, head = [], None
    for line in text.splitlines():
        if not line.startswith("|"):
            head = None
            continue
        cells = [c.strip() for c in line.strip("|").split("|")]
        if head is None:
            head = cells
        elif not set("".join(cells)) <= set("-: "):
            rows.append(dict(zip(head, cells)))
    return rows


def scenario_rows(log: Path, scenario: str) -> list[dict[str, str]]:
    body = log.read_text().split(f"SCENARIO: {scenario}\n", 1)[1]
    body = body.split("========================================\nSCENARIO:", 1)[0]
    return md_rows(body)


def num(s: str) -> float:
    return float(s.replace(",", ""))


def pick(rows, prefix: str, col: str) -> float:
    for r in rows:
        if r["Engine Name"].startswith(prefix):
            return num(r[col])
    raise KeyError(prefix)


PLATFORMS = [
    ("Linux x64", RAW / "ci" / "linux-x64" / "benchmark.log"),
    ("Windows x64", RAW / "ci" / "windows-x64" / "benchmark.log"),
]


def grouped(ax, groups, series, fmt):
    """groups: x labels; series: {name: [values per group]}."""
    x = np.arange(len(groups))
    w = 0.8 / len(series)
    for i, (name, vals) in enumerate(series.items()):
        pos = x + (i - (len(series) - 1) / 2) * w
        bars = ax.bar(pos, vals, w, color=COLOR[name], label=name)
        for b, v in zip(bars, vals):
            ax.annotate(fmt(v), (b.get_x() + b.get_width() / 2, v), ha="center", va="bottom",
                        xytext=(0, 3), textcoords="offset points", fontsize=8, fontweight="bold")
    ax.set_xticks(x, groups)
    logaxis(ax)
    ax.grid(axis="y")
    ax.margins(y=0.2)


def durable_update():
    local = md_rows((RAW / "disk-crud-wal.md").read_text())
    groups = ["macOS M4 Pro\n(local, 20,000 updates)"]
    prod = [pick(local, "DataVo (LSM Production)", "P50 Latency (ms)")]
    sql = [pick(local, "SQLite (WAL,full)", "P50 Latency (ms)")]
    for name, log in PLATFORMS:
        rows = scenario_rows(log, "disk-crud-wal")
        groups.append(f"{name}\n(CI, 50,000 updates)")
        prod.append(pick(rows, "DataVo (LSM Production)", "P50 Latency (ms)"))
        sql.append(pick(rows, "SQLite (WAL,full)", "P50 Latency (ms)"))
    fig, ax = plt.subplots(figsize=(9, 4.8))
    grouped(ax, groups, {"DataVo Production": prod, "SQLite full": sql}, lambda v: f"{v:.3g} ms")
    ax.set_ylabel("Median latency of one durable update, ms (log scale)")
    ax.set_title("Cost of one durable update by platform")
    ax.legend(ncol=2, loc="upper center", bbox_to_anchor=(0.5, -0.17))
    save(fig, "ci-durable-update-latency")


def vector_search():
    local = md_rows((RAW / "vector-search.md").read_text())
    groups = ["macOS M4 Pro\n(local)"]
    cols = {"DataVo Flat": "DataVo-Flat", "DataVo HNSW": "DataVo [", "SQLite": "SQLite"}
    series = {k: [pick(local, v, "Total Execution Time (ms)")] for k, v in cols.items()}
    for name, log in PLATFORMS:
        rows = scenario_rows(log, "vector-search")
        groups.append(f"{name}\n(CI)")
        for k, v in cols.items():
            series[k].append(pick(rows, v, "Total Execution Time (ms)"))
    fig, ax = plt.subplots(figsize=(9, 4.8))
    grouped(ax, groups, series, lambda v: f"{v:,.0f} ms")
    ax.set_ylabel("Total time, ms (log scale)")
    ax.set_title("Vector search: 10,000 x 1536-dim inserts, 100 top-10 queries")
    ax.legend(ncol=3, loc="upper center", bbox_to_anchor=(0.5, -0.17))
    save(fig, "ci-vector-search-time")


BDN_PLATFORMS = [
    ("M4 Pro\n(local)", RAW / "bdn"),
    ("M1 VM\n(CI)", RAW / "ci" / "macos-arm64-vm" / "bdn"),
    ("Linux x64\n(CI)", RAW / "ci" / "linux-x64" / "bdn"),
    ("Windows x64\n(CI)", RAW / "ci" / "windows-x64" / "bdn"),
]
ENG = {"DataVo": "DataVo", "Sqlite": "SQLite", "DuckDb1Thread": "DuckDB 1 thread"}


def to_us(text: str) -> float:
    v, unit = text.split()
    return float(v.replace(",", "")) * {"ns": 1e-3, "μs": 1.0, "us": 1.0, "ms": 1e3, "s": 1e6}[unit]


def bdn(folder: Path, cls: str):
    with open(folder / f"DataVo.Benchmarks.{cls}-report.csv", newline="") as f:
        return list(csv.DictReader(f))


def bdn_platforms():
    fig, axes = plt.subplots(1, 2, figsize=(12, 4.8))
    groups = [p for p, _ in BDN_PLATFORMS]
    for ax, cls, storage, query, title, ylabel in [
        (axes[0], "FilteredAggregateBenchmarks", "Memory", "Count",
         "Filtered COUNT, 200,000 rows, in memory", "Mean time per query, us (log scale)"),
        (axes[1], "PointLookupBenchmarks", "File", None,
         "Primary-key lookup, 100,000 rows, file-backed", "Mean time per lookup, us (log scale)"),
    ]:
        series = {e: [] for e in ENG.values()}
        for _, folder in BDN_PLATFORMS:
            for r in bdn(folder, cls):
                if r["Storage"] != storage:
                    continue
                method = r["Method"]
                if query:
                    eng, _, q = method.partition("_")
                    if q != query:
                        continue
                else:
                    eng = method
                if eng in ENG:
                    series[ENG[eng]].append(to_us(r["Mean"]))
        grouped(ax, groups, series, lambda v: f"{v:,.0f}" if v >= 10 else f"{v:.1f}")
        ax.set_title(title, fontsize=11)
        ax.set_ylabel(ylabel)
    axes[0].legend(ncol=3, loc="upper center", bbox_to_anchor=(1.1, -0.17), fontsize=9)
    save(fig, "ci-bdn-by-platform")


if __name__ == "__main__":
    durable_update()
    vector_search()
    bdn_platforms()
