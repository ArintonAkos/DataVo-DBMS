#!/usr/bin/env python3
"""Generate the Linux CI benchmark plots in the same matplotlib style as the
macOS headline figures (output/pdf/datavo_benchmark_figures.py), so the README
plots are visually consistent.

Data is the Ubuntu 24.04 x64 GitHub Actions "sqlite-vec rerun" Linux snapshot
(see docs/manual/performance/benchmarks.md, "Linux CI Snapshot ... sqlite-vec
rerun"). Outputs PNG into docs/public/benchmarks/.

Run (needs matplotlib):
    python3 docs/scripts/generate-linux-benchmark-plots.py
"""
from __future__ import annotations

import os
from pathlib import Path

MPL_CACHE = Path(__file__).resolve().parent / ".matplotlib-cache"
MPL_CACHE.mkdir(parents=True, exist_ok=True)
os.environ.setdefault("MPLCONFIGDIR", str(MPL_CACHE.resolve()))

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np

OUT_DIR = Path(__file__).resolve().parents[1] / "public" / "benchmarks"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --- Style: identical to output/pdf/datavo_benchmark_figures.py ---
mpl.rcParams.update(
    {
        "figure.dpi": 180,
        "savefig.dpi": 360,
        "font.family": "DejaVu Sans",
        "pdf.fonttype": 42,
        "ps.fonttype": 42,
        "svg.fonttype": "none",
        "axes.titleweight": "bold",
        "axes.labelweight": "bold",
        "axes.edgecolor": "#222831",
        "axes.linewidth": 0.8,
        "xtick.color": "#222831",
        "ytick.color": "#222831",
        "text.color": "#1F2933",
        "grid.color": "#D7DEE8",
        "grid.linewidth": 0.65,
        "legend.frameon": False,
    }
)

COLORS = {
    "datavo": "#0072B2",
    "datavo_lsm": "#009E73",
    "datavo_lsm_relaxed": "#2FBF71",
    "sqlite": "#E69F00",
    "litedb": "#CC79A7",
}

# --- Linux CI (Ubuntu 24.04 x64, GitHub Actions) sqlite-vec rerun data ---
# vector search: (label, total_time_ms, query_p99_ms, color)
VECTOR = [
    ("DataVo-Flat", 602.778, 10.595424, COLORS["datavo_lsm"]),
    ("SQLite (sqlite-vec)", 2186.128, 19.589254, COLORS["sqlite"]),
    ("DataVo (HNSW)", 79536.809, 2.557093, COLORS["datavo"]),
    ("LiteDB", 164640.254, 1663.416067, COLORS["litedb"]),
]

# thread scaling: threads -> ops/s per engine
THREADS = [1, 2, 4, 8, 16, 32]
THREAD_SERIES = [
    ("DataVo (LSM Relaxed)", [669441.4, 692373.008, 557513.046, 554449.457, 553293.479, 551425.988], COLORS["datavo_lsm_relaxed"]),
    ("SQLite (WAL,normal)", [125139.63, 188187.215, 206687.661, 242693.117, 252509.775, 243662.72], COLORS["sqlite"]),
    ("LiteDB", [24505.799, 24510.724, 22167.619, 21192.578, 22149.964, 22280.677], COLORS["litedb"]),
]


def _clean(ax: plt.Axes) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.set_axisbelow(True)  # gridlines behind bars/lines


def _save(fig: plt.Figure, name: str) -> None:
    path = OUT_DIR / name
    fig.savefig(path, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    print(f"wrote {path.relative_to(OUT_DIR.parents[2])}")


def vector_bar(value_index: int, title: str, ylabel: str, fmt: str, name: str) -> None:
    labels = [row[0] for row in VECTOR]
    values = [row[value_index] for row in VECTOR]
    colors = [row[3] for row in VECTOR]
    x = np.arange(len(labels))
    fig, ax = plt.subplots(figsize=(8.6, 5.4))
    bars = ax.bar(x, values, color=colors, edgecolor="#111827", linewidth=0.6)
    ax.set_yscale("log")
    ax.set_xticks(x, labels, rotation=12, ha="right")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(True, axis="y", which="both")
    ax.grid(False, axis="x")
    _clean(ax)
    for bar, value in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2, value * 1.16, fmt.format(value),
                ha="center", va="bottom", fontsize=8.4, fontweight="bold")
    ax.set_ylim(min(values) * 0.55, max(values) * 2.2)
    fig.tight_layout()
    _save(fig, name)


def thread_scaling() -> None:
    fig, ax = plt.subplots(figsize=(8.8, 5.4))
    for engine, ops, color in THREAD_SERIES:
        ax.plot(THREADS, ops, marker="o", linewidth=2.5, markersize=6, color=color, label=engine)
        for tx, value in zip(THREADS, ops):
            ax.text(tx, value * 1.05, f"{value / 1000:,.0f}K", ha="center", va="bottom",
                    fontsize=7.2, color=color,
                    fontweight="bold" if tx in (1, 32) else "normal")
    ax.set_yscale("log")
    ax.set_xscale("log", base=2)
    ax.set_xticks(THREADS)
    ax.get_xaxis().set_major_formatter(mpl.ticker.ScalarFormatter())
    ax.yaxis.set_major_formatter(mpl.ticker.FuncFormatter(lambda v, _: f"{v / 1000:.0f}K"))
    ax.set_xlabel("Worker threads")
    ax.set_ylabel("Operations per second (log scale)")
    ax.set_title("Linux CI: Thread Scaling Throughput")
    ax.grid(True, axis="both", which="major")
    _clean(ax)
    ax.legend(loc="center right")
    fig.tight_layout()
    _save(fig, "linux-thread-scaling-throughput.png")


if __name__ == "__main__":
    vector_bar(1, "Linux CI: Vector Search Total Time", "Total time (ms, log scale)",
               "{:,.1f} ms", "linux-vector-search-time.png")
    vector_bar(2, "Linux CI: Vector Query P99 Latency", "Query P99 latency (ms, log scale)",
               "{:,.3f} ms", "linux-vector-search-p99.png")
    thread_scaling()
    print("done")
