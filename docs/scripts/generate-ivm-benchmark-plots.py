#!/usr/bin/env python3
"""Generate IVM/reactive benchmark SVG and PNG plots without third-party Python deps."""
from __future__ import annotations

import math
import subprocess
from pathlib import Path
from xml.sax.saxutils import escape

OUT_DIR = Path(__file__).resolve().parents[1] / "public" / "benchmarks"
OUT_DIR.mkdir(parents=True, exist_ok=True)

WIDTH = 1200
HEIGHT = 760
PLOT_X = 150
PLOT_Y = 145
PLOT_W = 880
PLOT_H = 430
BASELINE_Y = PLOT_Y + PLOT_H

COLORS = {
    "datavo": "#0072B2",
    "sqlite": "#E69F00",
    "duckdb": "#DC2626",
    "axis": "#222831",
    "text": "#1F2933",
    "muted": "#4B5563",
    "grid": "#D7DEE8",
    "edge": "#111827",
}


def y_for(value: float, min_value: float, max_value: float) -> float:
    log_min = math.log10(min_value)
    log_max = math.log10(max_value)
    position = (math.log10(value) - log_min) / (log_max - log_min)
    return BASELINE_Y - position * PLOT_H


def text(x: float, y: float, value: str, *, size: int = 20, weight: str = "400",
         anchor: str = "middle", fill: str = COLORS["text"]) -> str:
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" text-anchor="{anchor}" '
        f'font-family="DejaVu Sans, Arial, sans-serif" font-size="{size}" '
        f'font-weight="{weight}" fill="{fill}">{escape(value)}</text>'
    )


def line(x1: float, y1: float, x2: float, y2: float, *, color: str, width: float = 1) -> str:
    return (
        f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" '
        f'stroke="{color}" stroke-width="{width}"/>'
    )


def rect(x: float, y: float, w: float, h: float, *, fill: str, stroke: str = COLORS["edge"]) -> str:
    return (
        f'<rect x="{x:.1f}" y="{y:.1f}" width="{w:.1f}" height="{h:.1f}" '
        f'fill="{fill}" stroke="{stroke}" stroke-width="1.1"/>'
    )


def format_tick(value: float) -> str:
    if value >= 1000:
        return f"{value / 1000:g}k"
    return f"{value:g}"


def plot_total_time(
    *,
    title: str,
    subtitle: str,
    rows: list[tuple[str, float, str]],
    name: str,
) -> None:
    values = [row[1] for row in rows]
    min_value = 10 ** math.floor(math.log10(min(values)) - 0.2)
    max_value = 10 ** math.ceil(math.log10(max(values)) + 0.2)
    baseline = values[0]

    parts: list[str] = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}">',
        '<rect width="100%" height="100%" fill="white"/>',
        text(WIDTH / 2, 62, title, size=29, weight="700"),
        text(WIDTH / 2, 96, subtitle, size=16, fill=COLORS["muted"]),
    ]

    tick = 1.0
    while tick < min_value:
        tick *= 10
    while tick <= max_value:
        y = y_for(tick, min_value, max_value)
        parts.append(line(PLOT_X, y, PLOT_X + PLOT_W, y, color=COLORS["grid"], width=1.1))
        parts.append(text(PLOT_X - 18, y + 5, format_tick(tick), size=14, anchor="end", fill=COLORS["muted"]))
        tick *= 10

    parts.append(line(PLOT_X, PLOT_Y, PLOT_X, BASELINE_Y, color=COLORS["axis"], width=1.4))
    parts.append(line(PLOT_X, BASELINE_Y, PLOT_X + PLOT_W, BASELINE_Y, color=COLORS["axis"], width=1.4))
    parts.append(text(49, PLOT_Y + PLOT_H / 2, "Total time (ms, log scale)", size=16, weight="700", anchor="middle"))
    parts[-1] = parts[-1].replace("<text ", '<text transform="rotate(-90 49 {:.1f})" '.format(PLOT_Y + PLOT_H / 2), 1)

    bar_w = 150
    gap = (PLOT_W - len(rows) * bar_w) / (len(rows) + 1)
    for i, (label, value, color) in enumerate(rows):
        x = PLOT_X + gap + i * (bar_w + gap)
        y = y_for(value, min_value, max_value)
        h = BASELINE_Y - y
        parts.append(rect(x, y, bar_w, h, fill=color))
        parts.append(text(x + bar_w / 2, BASELINE_Y + 42, label, size=18, weight="700"))
        parts.append(text(x + bar_w / 2, y - 18, f"{value:,.1f} ms", size=15, weight="700"))
        ratio = "incremental" if i == 0 else f"{value / baseline:,.1f}x slower"
        label_y = y + max(38, min(64, h * 0.32))
        parts.append(text(x + bar_w / 2, label_y + 5, ratio, size=15, weight="700", fill="#FFFFFF"))

    parts.append(text(WIDTH / 2, HEIGHT - 55, "Lower is better. DataVo row uses maintained reactive state; SQLite/DuckDB rows execute the benchmark baseline path.", size=14, fill=COLORS["muted"]))
    parts.append("</svg>")

    svg_path = OUT_DIR / f"{name}.svg"
    png_path = OUT_DIR / f"{name}.png"
    svg_path.write_text("\n".join(parts), encoding="utf-8")
    subprocess.run(["rsvg-convert", str(svg_path), "-o", str(png_path)], check=True)
    print(f"wrote {svg_path}")
    print(f"wrote {png_path}")


if __name__ == "__main__":
    plot_total_time(
        title="IVM Simple Exposure: Total Time",
        subtitle="50,000 insert+read iterations after 10,000 baseline rows (macOS arm64)",
        rows=[
            ("DataVo", 585.771, COLORS["datavo"]),
            ("SQLite", 2820.346, COLORS["sqlite"]),
            ("DuckDB", 95492.897, COLORS["duckdb"]),
        ],
        name="ivm-simple-exposure-total-time",
    )

    plot_total_time(
        title="IVM Complex VIP: Total Time",
        subtitle="50,000 JOIN + GROUP BY insert+read iterations (macOS arm64)",
        rows=[
            ("DataVo", 485.525, COLORS["datavo"]),
            ("DuckDB", 42996.154, COLORS["duckdb"]),
            ("SQLite", 401777.617, COLORS["sqlite"]),
        ],
        name="ivm-complex-vip-total-time",
    )
