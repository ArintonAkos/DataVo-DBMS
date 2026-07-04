const fs = require("node:fs");
const path = require("node:path");

const outDir = path.join(__dirname, "..", "public", "benchmarks");
fs.mkdirSync(outDir, { recursive: true });

const colors = {
  dataVo: "#2563eb",
  dataVoFlat: "#059669",
  dataVoRelaxed: "#0891b2",
  sqlite: "#d97706",
  liteDb: "#7c3aed",
  grid: "#d8dee9",
  axis: "#334155",
  text: "#0f172a",
  muted: "#64748b",
  background: "#ffffff",
};

const vectorRows = [
  { name: "DataVo", timeMs: 79536.809, p99Ms: 2.557093, color: colors.dataVo },
  { name: "DataVo-Flat", timeMs: 602.778, p99Ms: 10.595424, color: colors.dataVoFlat },
  { name: "LiteDB", timeMs: 164640.254, p99Ms: 1663.416067, color: colors.liteDb },
  { name: "SQLite/sqlite-vec", timeMs: 2186.128, p99Ms: 19.589254, color: colors.sqlite },
];

const threadRows = [
  { threads: 1, dataVo: 669441.4, sqlite: 125139.63, liteDb: 24505.799 },
  { threads: 2, dataVo: 692373.008, sqlite: 188187.215, liteDb: 24510.724 },
  { threads: 4, dataVo: 557513.046, sqlite: 206687.661, liteDb: 22167.619 },
  { threads: 8, dataVo: 554449.457, sqlite: 242693.117, liteDb: 21192.578 },
  { threads: 16, dataVo: 553293.479, sqlite: 252509.775, liteDb: 22149.964 },
  { threads: 32, dataVo: 551425.988, sqlite: 243662.72, liteDb: 22280.677 },
];

function escapeXml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function formatCompact(value) {
  if (value >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(value >= 10_000_000 ? 0 : 1)}M`;
  }

  if (value >= 1_000) {
    return `${(value / 1_000).toFixed(value >= 100_000 ? 0 : 1)}k`;
  }

  if (value >= 100) {
    return value.toFixed(0);
  }

  if (value >= 10) {
    return value.toFixed(1);
  }

  return value.toFixed(2);
}

function writeSvg(name, svg) {
  fs.writeFileSync(path.join(outDir, name), `${svg.trim()}\n`, "utf8");
}

function barChart({ title, subtitle, unit, rows, valueKey, output, log = false }) {
  const width = 920;
  const height = 520;
  const margin = { top: 82, right: 48, bottom: 112, left: 112 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const values = rows.map((row) => row[valueKey]);
  const minPositive = Math.min(...values.filter((value) => value > 0));
  const maxValue = Math.max(...values);
  const yMin = log ? Math.pow(10, Math.floor(Math.log10(minPositive)) - 1) : 0;
  const yMax = log ? Math.pow(10, Math.ceil(Math.log10(maxValue))) : Math.ceil(maxValue * 1.12);
  const ticks = log
    ? Array.from({ length: Math.log10(yMax) - Math.log10(yMin) + 1 }, (_, index) => yMin * 10 ** index)
    : [0, 0.25, 0.5, 0.75, 1].map((ratio) => yMax * ratio);

  const y = (value) => {
    const ratio = log
      ? (Math.log10(value) - Math.log10(yMin)) / (Math.log10(yMax) - Math.log10(yMin))
      : (value - yMin) / (yMax - yMin);
    return margin.top + plotHeight - ratio * plotHeight;
  };

  const barSlot = plotWidth / rows.length;
  const barWidth = Math.min(104, barSlot * 0.58);

  const grid = ticks.map((tick) => {
    const yy = y(tick);
    return `
      <line x1="${margin.left}" y1="${yy}" x2="${width - margin.right}" y2="${yy}" stroke="${colors.grid}" stroke-width="1"/>
      <text x="${margin.left - 14}" y="${yy + 5}" text-anchor="end" font-size="13" fill="${colors.muted}">${formatCompact(tick)}</text>`;
  }).join("");

  const bars = rows.map((row, index) => {
    const x = margin.left + index * barSlot + (barSlot - barWidth) / 2;
    const yy = y(row[valueKey]);
    const heightValue = margin.top + plotHeight - yy;
    return `
      <rect x="${x}" y="${yy}" width="${barWidth}" height="${heightValue}" rx="4" fill="${row.color}"/>
      <text x="${x + barWidth / 2}" y="${yy - 10}" text-anchor="middle" font-size="13" font-weight="700" fill="${colors.text}">${formatCompact(row[valueKey])}</text>
      <text x="${x + barWidth / 2}" y="${height - 70}" text-anchor="middle" font-size="14" fill="${colors.text}">${escapeXml(row.name)}</text>`;
  }).join("");

  const subtitleText = subtitle
    ? `<text x="${margin.left}" y="55" font-size="15" fill="${colors.muted}">${escapeXml(subtitle)}</text>`
    : "";

  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeXml(title)}">
      <rect width="100%" height="100%" fill="${colors.background}"/>
      <text x="${margin.left}" y="32" font-size="25" font-weight="800" fill="${colors.text}">${escapeXml(title)}</text>
      ${subtitleText}
      ${grid}
      <line x1="${margin.left}" y1="${margin.top + plotHeight}" x2="${width - margin.right}" y2="${margin.top + plotHeight}" stroke="${colors.axis}" stroke-width="1.2"/>
      <line x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${margin.top + plotHeight}" stroke="${colors.axis}" stroke-width="1.2"/>
      <text x="${margin.left}" y="${height - 24}" font-size="14" fill="${colors.muted}">Linux CI sqlite-vec rerun, July 4 2026</text>
      <text x="28" y="${margin.top - 18}" font-size="14" font-weight="700" fill="${colors.axis}">${escapeXml(unit)}${log ? " (log scale)" : ""}</text>
      ${bars}
    </svg>`;
  writeSvg(output, svg);
}

function lineChart() {
  const width = 920;
  const height = 520;
  const margin = { top: 82, right: 48, bottom: 90, left: 112 };
  const plotWidth = width - margin.left - margin.right;
  const plotHeight = height - margin.top - margin.bottom;
  const maxValue = 750000;
  const ticks = [0, 150000, 300000, 450000, 600000, 750000];
  const x = (index) => margin.left + (index / (threadRows.length - 1)) * plotWidth;
  const y = (value) => margin.top + plotHeight - (value / maxValue) * plotHeight;
  const series = [
    { key: "dataVo", name: "DataVo LSM Relaxed", color: colors.dataVoRelaxed },
    { key: "sqlite", name: "SQLite WAL normal", color: colors.sqlite },
    { key: "liteDb", name: "LiteDB", color: colors.liteDb },
  ];

  const grid = ticks.map((tick) => {
    const yy = y(tick);
    return `
      <line x1="${margin.left}" y1="${yy}" x2="${width - margin.right}" y2="${yy}" stroke="${colors.grid}" stroke-width="1"/>
      <text x="${margin.left - 14}" y="${yy + 5}" text-anchor="end" font-size="13" fill="${colors.muted}">${formatCompact(tick)}</text>`;
  }).join("");

  const lines = series.map((entry) => {
    const points = threadRows.map((row, index) => `${x(index)},${y(row[entry.key])}`).join(" ");
    const dots = threadRows.map((row, index) => `
      <circle cx="${x(index)}" cy="${y(row[entry.key])}" r="5" fill="${entry.color}"/>
    `).join("");
    return `
      <polyline points="${points}" fill="none" stroke="${entry.color}" stroke-width="3" stroke-linecap="round" stroke-linejoin="round"/>
      ${dots}`;
  }).join("");

  const xLabels = threadRows.map((row, index) => `
    <text x="${x(index)}" y="${margin.top + plotHeight + 34}" text-anchor="middle" font-size="14" fill="${colors.text}">${row.threads}</text>
  `).join("");

  const legend = series.map((entry, index) => {
    const lx = margin.left + index * 212;
    const ly = height - 32;
    return `
      <rect x="${lx}" y="${ly - 12}" width="14" height="14" rx="2" fill="${entry.color}"/>
      <text x="${lx + 22}" y="${ly}" font-size="14" fill="${colors.text}">${escapeXml(entry.name)}</text>`;
  }).join("");

  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" role="img" aria-label="Linux thread scaling throughput">
      <rect width="100%" height="100%" fill="${colors.background}"/>
      <text x="${margin.left}" y="32" font-size="25" font-weight="800" fill="${colors.text}">Linux thread scaling throughput</text>
      <text x="${margin.left}" y="55" font-size="15" fill="${colors.muted}">110,000 mixed read/update operations, 1 to 32 threads.</text>
      ${grid}
      <line x1="${margin.left}" y1="${margin.top + plotHeight}" x2="${width - margin.right}" y2="${margin.top + plotHeight}" stroke="${colors.axis}" stroke-width="1.2"/>
      <line x1="${margin.left}" y1="${margin.top}" x2="${margin.left}" y2="${margin.top + plotHeight}" stroke="${colors.axis}" stroke-width="1.2"/>
      <text x="28" y="${margin.top - 18}" font-size="14" font-weight="700" fill="${colors.axis}">ops/s</text>
      <text x="${margin.left + plotWidth / 2}" y="${height - 58}" text-anchor="middle" font-size="14" font-weight="700" fill="${colors.axis}">Thread count</text>
      ${xLabels}
      ${lines}
      ${legend}
    </svg>`;
  writeSvg("linux-thread-scaling-throughput.svg", svg);
}

barChart({
  title: "Linux vector search total time",
  subtitle: "10,000 x 1536-dim vectors, then 100 top-10 queries.",
  unit: "ms",
  rows: vectorRows,
  valueKey: "timeMs",
  output: "linux-vector-search-time.svg",
  log: true,
});

barChart({
  title: "Linux vector search query P99",
  subtitle: "Query-phase P99 latency for 100 top-10 searches.",
  unit: "ms",
  rows: vectorRows,
  valueKey: "p99Ms",
  output: "linux-vector-search-p99.svg",
  log: true,
});

lineChart();
