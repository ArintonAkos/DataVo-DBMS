#!/usr/bin/env python3
"""Charts for DataVo blog series, Part 2.

Every number below is copied directly from a verified BenchmarkDotNet report
(sources noted in footer). Run:  python3 docs/public/blog/part-2/make_charts.py
"""
import os
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

OUT = os.path.dirname(os.path.abspath(__file__))
DPI = 220

# Match Part 1 editorial palette
BLUE, ORANGE, GREY, GREEN = "#2a5d8f", "#c9782b", "#8d9199", "#4f8a6a"
INK, MUTED, GRID = "#222222", "#666666", "#e3e3e3"

plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 9.5,
    "axes.edgecolor": "#999999",
    "axes.labelcolor": INK,
    "axes.titlesize": 10.5,
    "axes.titleweight": "bold",
    "axes.titlelocation": "left",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "xtick.color": INK,
    "ytick.color": INK,
    "figure.facecolor": "white",
    "axes.facecolor": "white",
    "savefig.facecolor": "white",
})


def fmt_ns(v):
    if v >= 1000:
        return f"{v / 1000:,.1f} µs"
    return f"{v:,.1f} ns" if v < 100 else f"{v:,.0f} ns"


def make_chart():
    # Panel A: Primary-key point lookup mean latency (ns) across 3 platforms
    # Sources:
    #   - Apple M4 Pro: docs/public/benchmarks/2026-11-21-m4pro/DataVo.Benchmarks.PointLookupBenchmarks-report-github.md (02fa240f, 20 Nov 2026)
    #   - Linux x64 & Windows x64 CI: CI run 44748508039 (20 Nov 2026, master @ 02fa240f)
    platforms = [
        "Apple M4 Pro\n(Memory)",
        "Apple M4 Pro\n(File / LSM)",
        "Linux x64 CI\n(Memory)",
        "Linux x64 CI\n(File / LSM)",
        "Windows x64 CI\n(Memory)",
        "Windows x64 CI\n(File / LSM)",
    ]
    datavo_ns = [55.01, 216.22, 130.8, 288.3, 239.6, 666.9]
    sqlite_ns = [715.65, 1654.00, 1398.2, 3642.7, 2024.0, 6741.1]
    duckdb_ns = [63559.27, 62701.88, 203347.4, 204139.7, 376049.5, 380684.1]

    # Panel B: Managed heap allocation per operation (bytes)
    # Sources: Apple M4 Pro BDN reports (2026-11-21-m4pro) — identical across platforms for .NET 10 64-bit
    alloc_labels = [
        "Point lookup\nDataVo (Mem)",
        "Point lookup\nDataVo (LSM)",
        "Point lookup\nSQLite",
        "Point lookup\nDuckDB (1t)",
        "Filtered COUNT\nDataVo (Mem/LSM)",
        "Filtered COUNT\nSQLite",
        "Filtered COUNT\nDuckDB (1t)",
    ]
    alloc_bytes = [56, 144, 608, 2144, 0, 504, 1144]
    alloc_colors = [BLUE, GREEN, ORANGE, GREY, BLUE, ORANGE, GREY]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13.2, 5.4), gridspec_kw={"width_ratios": [1.35, 1.0]})

    # ---- Panel A: Cross-platform point lookup latency (log scale) ----
    width = 0.26
    xs = list(range(len(platforms)))
    series = [
        ("DataVo (compiled LINQ)", datavo_ns, BLUE),
        ("SQLite (ADO.NET prepared)", sqlite_ns, ORANGE),
        ("DuckDB (1 thread, ADO.NET)", duckdb_ns, GREY),
    ]
    for i, (label, vals, color) in enumerate(series):
        offset = (i - 1) * width
        bars = ax1.bar([x + offset for x in xs], vals, width * 0.9, color=color, label=label, zorder=3)
        for bar, v in zip(bars, vals):
            ax1.text(
                bar.get_x() + bar.get_width() / 2,
                v * 1.18,
                fmt_ns(v),
                ha="center",
                va="bottom",
                fontsize=7.2,
                rotation=90,
                color=INK,
            )

    ax1.set_yscale("log")
    ax1.set_ylim(20, 4_500_000)
    ax1.set_xticks(xs)
    ax1.set_xticklabels(platforms, fontsize=8.0)
    ax1.set_ylabel("Mean lookup latency (ns, log scale)")
    ax1.set_title("A. Primary-key point lookup (100k rows, shuffled keys)")
    ax1.grid(axis="y", color=GRID, zorder=0)
    ax1.legend(loc="upper left", frameon=False, fontsize=8.2)

    # ---- Panel B: Managed heap allocations (bytes / op) ----
    ys = list(range(len(alloc_labels)))[::-1]
    bars2 = ax2.barh(ys, alloc_bytes, height=0.62, color=alloc_colors, zorder=3)
    for bar, b, lbl in zip(bars2, alloc_bytes, alloc_labels):
        note = ""
        if b == 56:
            note = "  (= Player object)"
        elif b == 0:
            note = "  (zero heap alloc)"
        ax2.text(
            b + 35,
            bar.get_y() + bar.get_height() / 2,
            f"{b:,} B{note}",
            va="center",
            ha="left",
            fontsize=8.2,
            fontweight="bold" if b in (0, 56) else "normal",
            color=INK,
        )

    # Subtle separator between point lookup and filtered count groups
    ax2.axhline(2.5, color="#cccccc", linestyle="--", linewidth=0.9, zorder=2)
    ax2.set_yticks(ys)
    ax2.set_yticklabels(alloc_labels, fontsize=8.2)
    ax2.set_xlim(0, 2750)
    ax2.set_xlabel("Managed heap allocated per operation (bytes)")
    ax2.set_title("B. Managed heap allocation per call (BDN MemoryDiagnoser)")
    ax2.grid(axis="x", color=GRID, zorder=0)

    fig.suptitle(
        "Point-lookup latency across three platforms and managed heap allocations per call",
        fontsize=12.5,
        fontweight="bold",
        x=0.01,
        ha="left",
        y=0.99,
    )
    fig.tight_layout(rect=(0, 0.055, 1, 0.94))
    fig.text(
        0.01,
        0.01,
        "Sources: BenchmarkDotNet 0.15.8 (.NET 10) — Apple M4 Pro (02fa240f, 20 Nov 2026) and GitHub Actions Linux/Windows x64 CI run 44748508039 (20 Nov 2026). "
        "Lower is better on both panels; native C/C++ heap allocations in SQLite/DuckDB are excluded from .NET MemoryDiagnoser.",
        fontsize=7.4,
        color=MUTED,
        ha="left",
        va="bottom",
    )

    out_path = os.path.join(OUT, "01-lookup-latency-and-alloc.png")
    fig.savefig(out_path, dpi=DPI, bbox_inches="tight")
    plt.close(fig)
    print("wrote", out_path)


if __name__ == "__main__":
    make_chart()
