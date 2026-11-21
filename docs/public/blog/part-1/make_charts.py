#!/usr/bin/env python3
"""Charts for DataVo blog series, Part 1.

Every number below is copied from a measurement file (source noted per chart).
Nothing is derived except the bar geometry. Run:  python3 make_charts.py
"""
import os
import matplotlib
import matplotlib.ticker

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "charts")
os.makedirs(OUT, exist_ok=True)
DPI = 220

# ---- editorial style -------------------------------------------------------
BLUE, ORANGE, GREY, PURPLE, GREEN = "#2a5d8f", "#c9782b", "#8d9199", "#7b5a96", "#4f8a6a"
INK, MUTED, GRID = "#222222", "#666666", "#e3e3e3"
plt.rcParams.update({
    "font.family": "DejaVu Sans",
    "font.size": 10,
    "axes.edgecolor": "#999999",
    "axes.labelcolor": INK,
    "axes.titlesize": 11,
    "axes.titleweight": "bold",
    "axes.titlelocation": "left",
    "axes.spines.top": False,
    "axes.spines.right": False,
    "xtick.color": MUTED,
    "ytick.color": INK,
    "figure.facecolor": "white",
    "axes.facecolor": "white",
    "savefig.facecolor": "white",
})


def footer(fig, text):
    fig.text(0.01, 0.005, text, fontsize=7.5, color=MUTED, ha="left", va="bottom", wrap=True)


def save(fig, name):
    path = os.path.join(OUT, name)
    fig.savefig(path, dpi=DPI, bbox_inches="tight")
    plt.close(fig)
    print("wrote", path)


def fmt_time(v, unit):
    """v in `unit` ('ns' or 'µs'); exact value label."""
    s = f"{v:,.2f}" if v < 100 else f"{v:,.1f}"
    return f"{s} {unit}"


# =============================================================================
# Chart 1: point lookup + filtered COUNT(*), Linux x64 & Windows x64 (BDN)
# Source: DataVo.Benchmarks.{PointLookup,FilteredAggregate}Benchmarks-report-github.md
#         (CI run 44748508039, 2026-11-20; 4 logical / 2 physical cores)
# =============================================================================
def chart1():
    lookup_ns = {  # (Memory, File)
        "Linux x64": {"DataVo": (130.8, 288.3), "SQLite": (1398.2, 3642.7), "DuckDB (1 thread)": (203347.4, 204139.7)},
        "Windows x64": {"DataVo": (239.6, 666.9), "SQLite": (2024.0, 6741.1), "DuckDB (1 thread)": (376049.5, 380684.1)},
    }
    count_us = {
        "Linux x64": {"DataVo": (9.287, 8.813), "SQLite": (4553.243, 5603.374), "DuckDB (1 thread)": (369.267, 538.399)},
        "Windows x64": {"DataVo": (16.20, 15.94), "SQLite": (8607.58, 13446.12), "DuckDB (1 thread)": (683.64, 1102.20)},
    }
    colors = {"DataVo": BLUE, "SQLite": ORANGE, "DuckDB (1 thread)": GREY}
    fig, axes = plt.subplots(2, 2, figsize=(12.5, 8.2))
    rows = [("Primary-key point lookup, 100,000 rows", lookup_ns, "ns", 1.0),
            ("Filtered COUNT(*), 200,000 rows, [Columnar] index", count_us, "µs", 1.0)]
    for r, (title, data, unit, _) in enumerate(rows):
        for c, plat in enumerate(["Linux x64", "Windows x64"]):
            ax = axes[r][c]
            width = 0.26
            for i, eng in enumerate(["DataVo", "SQLite", "DuckDB (1 thread)"]):
                for j, v in enumerate(data[plat][eng]):
                    x = j + (i - 1) * width
                    ax.bar(x, v, width * 0.92, color=colors[eng], zorder=3)
                    ax.text(x, v * 1.18, fmt_time(v, unit), ha="center", va="bottom", fontsize=7.4,
                            rotation=90, color=INK)
            ax.set_yscale("log")
            ax.set_xticks([0, 1])
            ax.set_xticklabels(["Memory", "File (DataVo LSM / SQLite WAL NORMAL)"], fontsize=8.5)
            top = max(max(v) for v in data[plat].values())
            ax.set_ylim(top=top * 25)
            ax.set_ylabel(f"mean time per operation ({unit}, log scale)")
            ax.grid(axis="y", color=GRID, zorder=0)
            ax.set_title(f"{title}\n{plat}", fontsize=10)
    handles = [Patch(color=colors[k], label=k) for k in colors]
    fig.legend(handles=handles, loc="upper center", ncol=3, frameon=False, bbox_to_anchor=(0.5, 1.0))
    fig.suptitle("Point lookups and columnar aggregates on two x64 CI runners", fontsize=13,
                 fontweight="bold", x=0.01, ha="left", y=1.035)
    fig.tight_layout(rect=(0, 0.03, 1, 0.97))
    footer(fig, "BenchmarkDotNet 0.15.8, .NET 10.0.15, GitHub-hosted VMs (4 logical / 2 physical cores), CI run 44748508039, "
                "2026-11-20. Means, log y-axis. DuckDB is a column store held to one thread here.")
    save(fig, "01-cross-platform-lookup-and-count.png")


# =============================================================================
# Chart 2: 100k-row warm initial sync, three panels
# Source: sync-ci-electric/compare.md (run 44756364158, Ubuntu 26.04 x64, 2 vCPU, PG 17.12)
# =============================================================================
def chart2():
    stacks = [  # label, family, warm ms, wire B/row, client alloc MB
        ("DataVo-Sync → DataVo inmemory", "datavo", 605, 45.5, 36.4),
        ("DataVo-Sync → DataVo lsm", "datavo", 726, 45.5, 49.1),
        ("DataVo-Sync → DataVo lsm-strict", "datavo", 706, 45.5, 49.1),
        ("JSON/WS → SQLite WAL NORMAL", "json", 832, 214.8, 129.9),
        ("JSON/WS + deflate → SQLite NORMAL", "json", 852, 58.5, 130.2),
        ("JSON/WS → SQLite WAL FULL", "json", 702, 214.8, 129.9),
        ("JSON/WS + deflate → SQLite FULL", "json", 909, 58.5, 130.0),
        ("ElectricSQL 1.8.1 → SQLite NORMAL", "electric", 1167, 362.7, 251.4),
    ]
    col = {"datavo": BLUE, "json": ORANGE, "electric": PURPLE}
    panels = [("Warm initial sync time", "ms", 2, "{:,.0f} ms"),
              ("Wire bytes per row", "B/row", 3, "{:,.1f} B"),
              ("Client GC allocations", "MB", 4, "{:,.1f} MB")]
    fig, axes = plt.subplots(1, 3, figsize=(15, 5.2), sharey=True)
    ys = list(range(len(stacks)))[::-1]
    for ax, (title, unit, idx, f) in zip(axes, panels):
        vals = [s[idx] for s in stacks]
        ax.barh(ys, vals, color=[col[s[1]] for s in stacks], height=0.68, zorder=3)
        for y, v in zip(ys, vals):
            ax.text(v + max(vals) * 0.015, y, f.format(v), va="center", fontsize=8.5, color=INK)
        ax.set_xlim(0, max(vals) * 1.28)
        ax.set_title(title)
        ax.set_xlabel(unit)
        ax.grid(axis="x", color=GRID, zorder=0)
    axes[0].set_yticks(ys)
    axes[0].set_yticklabels([s[0] for s in stacks], fontsize=8.8)
    fig.legend(handles=[Patch(color=BLUE, label="DataVo-Sync"), Patch(color=ORANGE, label="custom SQLite + JSON/WebSocket (downlink only)"),
                        Patch(color=PURPLE, label="ElectricSQL 1.8.1 (Shape API, read path)")],
               loc="upper center", ncol=3, frameon=False, bbox_to_anchor=(0.5, 1.02))
    fig.suptitle("Initial sync of 100,000 rows from PostgreSQL: time is close, bytes and allocations are not",
                 fontsize=13, fontweight="bold", x=0.01, ha="left", y=1.08)
    fig.tight_layout(rect=(0, 0.04, 1, 1))
    footer(fig, "Run 44756364158, Ubuntu 26.04 x64, 2 vCPU, PostgreSQL 17.12, loopback. Warm = median of runs 2-4 in one client process. "
                "Wire = TCP bytes both directions. Single run; the JSON rows moved by up to 1.7x between earlier runs, so the time ordering "
                "among the first seven bars is not established.")
    save(fig, "02-initial-sync-100k.png")


# =============================================================================
# Chart 3: concurrent WebSocket client scaling
# Source: sync-ci-electric/stress-10-100-1000.md (PgHubBridge off; 10 cycles x 20 rows per client)
# =============================================================================
def chart3():
    clients = [10, 100, 1000]
    rows_s = [14273, 9784, 12754]
    p50 = [10.63, 158.87, 1220.10]
    p95 = [26.70, 300.40, 3418.56]
    p99 = [30.42, 393.95, 4636.53]
    # NOTE: the source table's header labels are shifted; columns re-aligned against
    # E2E-RESULTS.md ("10 clients, 10.6 ms p50"; "1,000 clients ... 15.68 s").
    fig, (a, b) = plt.subplots(1, 2, figsize=(12.5, 5.0))
    xs = range(3)
    a.bar(xs, rows_s, color=BLUE, width=0.55, zorder=3)
    for x, v in zip(xs, rows_s):
        a.text(x, v + 250, f"{v:,} rows/s", ha="center", fontsize=9)
    a.set_xticks(list(xs))
    a.set_xticklabels([f"{c:,} clients" for c in clients])
    a.set_ylim(0, 17000)
    a.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    a.set_ylabel("aggregate throughput (rows/s)")
    a.set_title("Throughput")
    a.grid(axis="y", color=GRID, zorder=0)

    for series, lab, colr, mk in [(p50, "p50", BLUE, "o"), (p95, "p95", GREEN, "s"), (p99, "p99", ORANGE, "D")]:
        b.plot(list(xs), series, color=colr, marker=mk, lw=2, label=lab, zorder=3)
        for x, v in zip(xs, series):
            off, ha = {"p50": ((0, -15), "center"), "p95": ((9, -3), "left"), "p99": ((0, 8), "center")}[lab]
            b.annotate(f"{v:,.2f}", (x, v), textcoords="offset points", xytext=off, ha=ha, fontsize=8, color=colr)
    b.set_yscale("log")
    b.set_ylim(4, 15000)
    b.set_xlim(-0.25, 2.6)
    b.set_xticks(list(xs))
    b.set_xticklabels([f"{c:,} clients" for c in clients])
    b.set_ylabel("latency (ms, log scale)")
    b.set_title("Latency percentiles")
    b.grid(axis="y", color=GRID, zorder=0)
    b.legend(frameon=False, loc="upper left")
    fig.suptitle("DataVo-Sync hub: throughput holds at 10–14k rows/s, tail latency does not (10 → 100 → 1,000 clients)", fontsize=13,
                 fontweight="bold", x=0.01, ha="left", y=1.02)
    fig.tight_layout(rect=(0, 0.06, 1, 1))
    footer(fig, "Run 44756364158, Ubuntu 26.04 x64, 2 vCPU. Each client: 10 cycles x 20 rows over WebSocket (2,000 / 20,000 / 200,000 rows total; "
                "wall 0.14 / 2.04 / 15.68 s). PostgreSQL bridge OFF: this measures the hub and transport, not the PostgreSQL path. "
                "Clients and server share the VM. No SQLite or ElectricSQL series exists for this test.")
    save(fig, "03-websocket-client-scaling.png")


# =============================================================================
# Chart 4: concurrent ops, 8 readers + 2 writers, 5 s
# Source: docs/public/benchmarks/2026-11-21-m4pro/concurrent-ops.md (Apple M4 Pro)
#         & linux-x64-net10-benchmark.log / windows-x64-net10-benchmark.log (CI run 44748508039)
# =============================================================================
def chart4():
    data = {
        "Apple M4 Pro\n(21 Nov)": [
            ("DataVo (InMemory)", BLUE, dict(ops=1087329, writes=224522, wp99=0.619834)),
            ("SQLite (WAL, normal)", GREEN, dict(ops=389341, writes=91458, wp99=1.299083)),
            ("SQLite (shared-cache)", ORANGE, dict(ops=381982, writes=2, wp99=5011.840208)),
        ],
        "Linux x64 CI\n(20 Nov)": [
            ("DataVo (InMemory)", BLUE, dict(ops=1530919, writes=302878, wp99=0.124786)),
            ("SQLite (shared-cache)", ORANGE, dict(ops=244903, writes=2, wp99=10061.055113)),
        ],
        "Windows x64 CI\n(20 Nov)": [
            ("DataVo (InMemory)", BLUE, dict(ops=1535444, writes=98696, wp99=1.0843)),
            ("SQLite (shared-cache)", ORANGE, dict(ops=432870, writes=2, wp99=5151.9354)),
        ],
    }
    fig, (a, b) = plt.subplots(1, 2, figsize=(14.0, 5.6))
    w = 0.295
    for i, plat in enumerate(data):
        entries = data[plat]
        n = len(entries)
        for j, (eng, colr, d) in enumerate(entries):
            x = i + (j - (n - 1) / 2.0) * w
            a.bar(x, d["ops"], w * 0.84, color=colr, zorder=3)
            a.text(x, d["ops"] + 28000, f"{d['ops']:,}", ha="center", fontsize=7.6)
            b.bar(x, d["writes"], w * 0.84, color=colr, zorder=3)
            p99_lbl = f"{d['wp99']:,.0f} ms" if d["wp99"] >= 100 else f"{d['wp99']:,.2f} ms"
            b.text(x, d["writes"] * 1.32, f"{d['writes']:,}\n{p99_lbl}", ha="center", fontsize=7.5)
    for ax in (a, b):
        ax.set_xticks([0, 1, 2])
        ax.set_xticklabels(list(data))
        ax.grid(axis="y", color=GRID, zorder=0)
    a.set_ylim(0, 1850000)
    a.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v:,.0f}"))
    a.set_ylabel("reported operations / s")
    a.set_title("Throughput as reported")
    b.set_yscale("log")
    b.set_ylim(1, 4e6)
    b.set_ylabel("writes completed (log scale)")
    b.set_title("Writes completed (with write p99 latency)")
    fig.legend(
        handles=[
            Patch(color=BLUE, label="DataVo (InMemory)"),
            Patch(color=GREEN, label="SQLite (WAL, normal — M4 Pro)"),
            Patch(color=ORANGE, label="SQLite (shared-cache)"),
        ],
        loc="upper center", ncol=3, frameon=False, bbox_to_anchor=(0.5, 1.0),
    )
    fig.suptitle("Concurrent ops, 8 readers + 2 writers: shared-cache starves writers, WAL does not",
                 fontsize=13, fontweight="bold", x=0.01, ha="left", y=1.05)
    fig.tight_layout(rect=(0, 0.08, 1, 0.97))
    footer(fig, "100,000 preloaded records, 5 s window, busy_timeout=3,000 ms. "
                "M4 Pro (21 Nov, back-to-back run): DataVo completes 224,522 writes (0.62 ms p99) vs SQLite WAL 91,458 writes (1.30 ms p99), while SQLite shared-cache stalls at 2 writes (5,012 ms p99). "
                "CI run 44748508039 (20 Nov, 4 vCPU) only measured SQLite shared-cache.")
    save(fig, "04-concurrent-ops.png")


# =============================================================================
# Chart 5: macOS 28-query SQL matrix + 5 vector shapes, relative speed vs SQLite
# Source: docs/public/benchmarks/2026-11-17-engine-perf-followups/sql-matrix.md (last column,
#         SQLite median / DataVo LSM median); vector rows from datavo-vs-sqlite.md (384-d, 50,000 docs)
# =============================================================================
def chart5():
    # (label, value shown as bar, optional marker value)
    groups = [
        ("Joins", [("J1 3-table inner join, no filter", 10.28, None), ("J2 3-table join, filtered (~0.2%)", 3.85, None),
                   ("J3 3-table join, filter, ORDER BY…LIMIT 50", 5.22, None), ("J4 LEFT JOIN, COUNT/SUM", 3.48, None),
                   ("J5 LEFT JOIN anti-join (IS NULL)", 0.94, None), ("J6 Orders × Customers GROUP BY", 5.87, None),
                   ("J7 small × large via index", 2.28, None)]),
        ("Filters", [("W1 equality, secondary index", 2.42, None), ("W2 equality, no index", 0.94, None),
                                ("W3 (A OR B) AND range", 0.95, None), ("W4 W3 + NULL-aware", 0.85, None),
                                ("W5 prefix LIKE", 0.89, None), ("W6 substring LIKE", 0.74, None)]),
        ("Primary-key ranges", [("R1 100 rows BETWEEN", 1.35, None), ("R2 100 rows >= AND <", 1.41, None),
                                ("R3 10,000 rows BETWEEN", 1.75, None), ("R4 10,000 rows >= AND <", 1.76, None)]),
        ("GROUP BY", [("G1 4 groups", 4.70, None), ("G2 1,000 groups", 3.30, None),
                      ("G3 ~125,000 groups, indexed key, HAVING+top-100", 0.69, None)]),
        ("Columnar index (○ = same query without it)", [
            ("C1 COUNT, range", 13.73, 0.82), ("C2 SUM(INT), range", 10.65, 0.55), ("C3 SUM(DOUBLE), range", 0.61, 0.61),
            ("C4 COUNT/SUM/MIN/MAX", 4.82, 0.66), ("C5 COUNT, string filter", 1.07, 1.07),
            ("C6 COUNT, (a OR b) AND range", 7.46, 0.55), ("C7 GROUP BY Bucket", 3.17, 3.17),
            ("C8 ORDER BY…LIMIT 10, unindexed", 0.79, 0.78)]),
        ("Vector, 384-d, 50k docs (◆ = HNSW)", [
            ("V0 top-10, no filter", 8.3, 70), ("V1 filter ~5%", 3.7, 3.9), ("V2 filter ~50%", 7.0, 22),
            ("V3 V1 + JOIN", 3.8, 4.0), ("V4 JOIN, ~10% survive", 3.0, 3.6)]),
    ]
    n = sum(len(g[1]) for g in groups) + len(groups) - 1
    fig, ax = plt.subplots(figsize=(12, 0.285 * n + 2.2))
    y = n
    yticks, ylabels, band_info = [], [], []
    for gi, (gname, items) in enumerate(groups):
        band_info.append((gname, y))
        y -= 0  # header row occupies y level
        ax.text(0.0, y + 0.0, "", transform=ax.get_yaxis_transform())
        y -= 1
        for label, val, mk in items:
            if val >= 1.2:
                colr = BLUE
            elif val >= 1 / 1.2:
                colr = GREY
            else:
                colr = ORANGE
            ax.barh(y, val - 1, left=1, height=0.7, color=colr, zorder=3)
            is_vec = gname.startswith("Vector")
            txt = f"{val:.1f}×" if is_vec else f"{val:.2f}×"
            if mk is not None and abs(mk - val) > 1e-9:
                txt += f"  (◆ HNSW {mk:g}×)" if is_vec else f"  (○ {mk:.2f}×)"
            if val >= 1:
                xr = max(val, mk) if (mk is not None and is_vec) else val
                ax.text(xr * 1.07, y, txt, va="center", ha="left", fontsize=8, color=INK)
            else:
                xl = min(val, mk) if mk is not None else val
                ax.text(xl / 1.07, y, txt, va="center", ha="right", fontsize=8, color=INK)
            if mk is not None and abs(mk - val) > 1e-9:
                ax.plot(mk, y, marker="D" if is_vec else "o", mfc=INK if is_vec else "white",
                        mec=INK, ms=5.2, zorder=5, ls="none")
            yticks.append(y)
            ylabels.append(label)
            y -= 1
        y -= 0
        if gi < len(groups) - 1:
            y -= 0
    # group headers
    # recompute header positions: header sits one row above first item of each group
    ax.set_yticks(yticks)
    ax.set_yticklabels(ylabels, fontsize=8.5)
    for gname, ytop in band_info:
        ax.text(-0.02, ytop - 0.15, gname.upper(), transform=ax.get_yaxis_transform(), ha="right", va="center",
                fontsize=7.6, color=MUTED, fontweight="bold")
    ax.axvline(1.0, color=INK, lw=1.2, zorder=4)
    ax.set_xscale("log")
    ax.set_xlim(0.28, 130)
    ticks = [0.5, 1, 2, 5, 10, 20, 50, 100]
    ax.set_xticks(ticks)
    ax.set_xticklabels([f"{t:g}×" for t in ticks])
    ax.set_ylim(min(yticks) - 0.8, n + 0.2)
    ax.grid(axis="x", color=GRID, zorder=0)
    ax.set_xlabel("SQLite time ÷ DataVo time  (right of 1× = DataVo faster; left = SQLite faster)")
    ax.text(1.03, n + 0.1, "parity", fontsize=8, color=INK, va="bottom")
    ax.legend(handles=[Patch(color=BLUE, label="DataVo faster (> 1.2×)"), Patch(color=GREY, label="within ±20% (not established)"),
                       Patch(color=ORANGE, label="SQLite faster (< 1/1.2)"),
                       Line2D([], [], marker="o", mfc="white", mec=INK, ls="none", label="○ without columnar index"),
                       Line2D([], [], marker="D", color=INK, ls="none", label="◆ HNSW (bars are exact Flat)")],
              loc="upper right", frameon=True, framealpha=1, edgecolor=GRID, fontsize=8)
    fig.suptitle("28 SQL shapes and 5 vector shapes against SQLite: where DataVo wins and where it loses", fontsize=13,
                 fontweight="bold", x=0.01, ha="left", y=0.995)
    fig.tight_layout(rect=(0.03, 0.035, 1, 0.97))
    footer(fig, "Apple M4 Pro, .NET 10.0.3, 1,000,000 orders, DataVo LSM Relaxed vs SQLite WAL NORMAL (matched durability), prepared statements, "
                "median of one call, every answer checked against an oracle. One run (2026-11-17); runs vary by up to ~10%. "
                "Bars for C1–C8 are DataVo with the columnar index; ○ is the same query without it. Vector bars: sqlite-vec (exact) ÷ DataVo Flat (exact).")
    save(fig, "05-sql-matrix-vs-sqlite.png")


# =============================================================================
# Chart 6: Incremental View Maintenance (complex-vip: 50,000 updates + 3-table join reads)
# Source: 2026-11-12/complex-vip.md (M4 Pro), linux-x64-net10-benchmark.log, windows-x64-net10-benchmark.log
# =============================================================================
def chart6():
    # Total time in seconds across 50,000 insert+read iterations
    data = {
        "Apple M4 Pro\n(12 Nov)": {"DataVo (IVM)": 0.1917, "DuckDB (recompute)": 34.339, "SQLite (recompute)": 126.064},
        "Linux x64 CI\n(20 Nov)": {"DataVo (IVM)": 0.5999, "DuckDB (recompute)": 112.920, "SQLite (recompute)": 237.074},
        "Windows x64 CI\n(20 Nov)": {"DataVo (IVM)": 0.5455, "DuckDB (recompute)": 135.540, "SQLite (recompute)": 247.797},
    }
    colors = {"DataVo (IVM)": BLUE, "DuckDB (recompute)": GREY, "SQLite (recompute)": ORANGE}
    fig, ax = plt.subplots(figsize=(11.5, 4.8))
    w = 0.25
    plats = list(data.keys())
    for i, plat in enumerate(plats):
        for j, eng in enumerate(["DataVo (IVM)", "DuckDB (recompute)", "SQLite (recompute)"]):
            v = data[plat][eng]
            x = i + (j - 1) * w
            ax.bar(x, v, w * 0.9, color=colors[eng], zorder=3)
            lbl = f"{v * 1000:.0f} ms" if v < 1.0 else f"{v:,.1f} s"
            ax.text(x, v * 1.18, lbl, ha="center", va="bottom", fontsize=8.8, color=INK, fontweight="bold" if j == 0 else "normal")
    ax.set_yscale("log")
    ax.set_ylim(0.08, 900)
    ax.set_xticks(range(len(plats)))
    ax.set_xticklabels(plats, fontsize=9.5)
    ax.set_ylabel("total time for 50,000 updates + reads (s, log scale)")
    ax.grid(axis="y", color=GRID, zorder=0)
    ax.legend(handles=[Patch(color=colors[k], label=k) for k in colors], loc="upper left", frameon=False, ncol=3, fontsize=9)
    fig.suptitle("Incremental view vs recomputing a 3-table JOIN + GROUP BY (50,000 updates)", fontsize=13,
                 fontweight="bold", x=0.01, ha="left", y=1.01)
    fig.tight_layout(rect=(0, 0.06, 1, 0.97))
    footer(fig, "complex-vip scenario: 10,000 orders, 1,000 accounts, 50 markets preloaded, then 50,000 insert+read iterations. "
                "DataVo maintains the join incrementally via Z-set deltas; DuckDB and SQLite re-run the prepared query each time. Log y-axis.")
    save(fig, "06-incremental-views-complex-vip.png")


if __name__ == "__main__":
    chart1()
    chart2()
    chart3()
    chart4()
    chart5()
    chart6()
