#!/usr/bin/env node
/**
 * Generates paired light/dark monochrome Rough.js SVG diagrams for
 * Building DataVo — Part 2 (Packed byte buffers, Native AOT, and Roslyn codegen).
 *
 * Matches the exact Part 1 visual palette, stroke weights, hachure fills,
 * 780px viewBox width, and system sans/monospace typography.
 *
 * Run from repo root:  node docs/public/blog/part-2/make_diagrams.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);
const rough = require(path.resolve(__dirname, "../../../node_modules/roughjs"));

const SANS = `-apple-system, BlinkMacSystemFont, "Helvetica Neue", Arial, sans-serif`;
const MONO = `ui-monospace, SFMono-Regular, Menlo, Consolas, monospace`;

const PALETTES = {
  light: {
    ink: "#1c1c1c",
    muted: "#666666",
    faint: "#a0a0a0",
    hatch: "#d4d4d4",
  },
  dark: {
    ink: "#e5e5e5",
    muted: "#9a9a9a",
    faint: "#5a5a5a",
    hatch: "#3c3c3c",
  },
};

function esc(s) {
  return String(s)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

function createCanvas(width, height, ariaLabel, theme) {
  const pal = PALETTES[theme];
  const gen = rough.generator();
  const elements = [];
  let nextSeed = 101;
  const seed = () => nextSeed++;

  function pushPaths(drawable, stroke, strokeWidth, dash = null, fillStroke = null) {
    const paths = gen.toPaths(drawable);
    for (const p of paths) {
      const isFill = p.stroke === "none" || (fillStroke && p.stroke === fillStroke);
      const sColor = isFill && fillStroke ? fillStroke : stroke;
      const sWidth = isFill && fillStroke ? 0.8 : strokeWidth;
      const dashAttr = dash && !isFill ? ` stroke-dasharray="${dash}"` : "";
      elements.push(
        `  <path d="${p.d}" fill="none" stroke="${sColor}" stroke-width="${sWidth}" stroke-linecap="round" stroke-linejoin="round"${dashAttr} />`
      );
    }
  }

  function box(x, y, w, h, opts = {}) {
    const {
      dashed = false,
      hatch = false,
      muted = false,
      roughness = 0.85,
    } = opts;
    const stroke = muted ? pal.muted : pal.ink;
    const ro = {
      seed: seed(),
      roughness,
      bowing: 0.7,
      stroke,
      strokeWidth: 1.3,
    };
    if (hatch) {
      ro.fill = pal.hatch;
      ro.fillStyle = "hachure";
      ro.fillWeight = 0.8;
      ro.hachureGap = 7;
      ro.hachureAngle = -42;
    }
    const drawable = gen.rectangle(x, y, w, h, ro);
    pushPaths(drawable, stroke, 1.3, dashed ? "5 4" : null, hatch ? pal.hatch : null);
  }

  function diamond(cx, cy, rx, ry, opts = {}) {
    const stroke = opts.muted ? pal.muted : pal.ink;
    const pts = [
      [cx, cy - ry],
      [cx + rx, cy],
      [cx, cy + ry],
      [cx - rx, cy],
    ];
    const drawable = gen.polygon(pts, {
      seed: seed(),
      roughness: 0.85,
      bowing: 0.6,
      stroke,
      strokeWidth: 1.3,
    });
    pushPaths(drawable, stroke, 1.3);
  }

  function line(x1, y1, x2, y2, opts = {}) {
    const { dashed = false, faint = false, muted = false, strokeWidth = 1.25 } = opts;
    const stroke = faint ? pal.faint : muted ? pal.muted : pal.ink;
    const drawable = gen.line(x1, y1, x2, y2, {
      seed: seed(),
      roughness: 0.7,
      bowing: 0.5,
      stroke,
      strokeWidth,
    });
    pushPaths(drawable, stroke, strokeWidth, dashed ? "5 4" : null);
  }

  function polyline(points, opts = {}) {
    const { dashed = false, muted = false, strokeWidth = 1.25 } = opts;
    const stroke = muted ? pal.muted : pal.ink;
    for (let i = 0; i < points.length - 1; i++) {
      const [x1, y1] = points[i];
      const [x2, y2] = points[i + 1];
      line(x1, y1, x2, y2, { dashed, muted, strokeWidth });
    }
  }

  function arrowHead(x1, y1, x2, y2, opts = {}) {
    const stroke = opts.muted ? pal.muted : pal.ink;
    const angle = Math.atan2(y2 - y1, x2 - x1);
    const len = 8.5;
    const spread = 0.42;
    const ax1 = x2 - len * Math.cos(angle - spread);
    const ay1 = y2 - len * Math.sin(angle - spread);
    const ax2 = x2 - len * Math.cos(angle + spread);
    const ay2 = y2 - len * Math.sin(angle + spread);
    const d1 = gen.line(ax1, ay1, x2, y2, { seed: seed(), roughness: 0.4, stroke, strokeWidth: 1.35 });
    const d2 = gen.line(ax2, ay2, x2, y2, { seed: seed(), roughness: 0.4, stroke, strokeWidth: 1.35 });
    pushPaths(d1, stroke, 1.35);
    pushPaths(d2, stroke, 1.35);
  }

  function arrow(x1, y1, x2, y2, opts = {}) {
    line(x1, y1, x2, y2, opts);
    arrowHead(x1, y1, x2, y2, opts);
  }

  function polyArrow(points, opts = {}) {
    polyline(points, opts);
    const [x1, y1] = points[points.length - 2];
    const [x2, y2] = points[points.length - 1];
    arrowHead(x1, y1, x2, y2, opts);
  }

  function text(x, y, content, opts = {}) {
    const {
      mono = false,
      size = 12.5,
      weight = 400,
      anchor = "middle",
      muted = false,
      faint = false,
    } = opts;
    const fill = faint ? pal.faint : muted ? pal.muted : pal.ink;
    const font = mono ? MONO : SANS;
    elements.push(
      `  <text x="${x}" y="${y}" fill="${fill}" font-family='${font}' font-size="${size}" font-weight="${weight}" text-anchor="${anchor}">${esc(content)}</text>`
    );
  }

  function render() {
    return [
      `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${width} ${height}" width="${width}" height="${height}" role="img" aria-label="${esc(ariaLabel)}">`,
      ...elements,
      `</svg>`,
      "",
    ].join("\n");
  }

  return { box, diamond, line, polyline, arrow, polyArrow, text, render };
}

// =============================================================================
// Figure 1: Managed-Native Interop Boundary vs. Single Compilation Unit
// =============================================================================
function fig01(theme) {
  const c = createCanvas(
    780,
    330,
    "Figure 1 — SQLite managed-to-native P/Invoke boundary versus DataVo single managed compilation unit",
    theme
  );

  // Left: SQLite via ADO.NET / Dapper
  c.text(20, 26, "SQLite via ADO.NET", { size: 14, weight: 600, anchor: "start" });

  c.box(20, 46, 330, 40);
  c.text(185, 71, ".NET App + SqliteCommand", { size: 13, weight: 600 });

  c.arrow(185, 86, 185, 118, { muted: true });
  c.text(196, 106, "608 B managed alloc", { mono: true, size: 11, anchor: "start", muted: true });

  // Interop wall
  c.line(20, 124, 350, 124, { dashed: true, faint: true });
  c.text(185, 139, "P/Invoke ABI boundary (no inlining)", { size: 11.2, muted: true });

  c.box(20, 150, 330, 40, { dashed: true, muted: true });
  c.text(185, 175, "sqlite3_bind_* → sqlite3_step()", { mono: true, size: 12, muted: true });

  c.arrow(185, 190, 185, 208, { muted: true });

  c.box(20, 208, 330, 40, { dashed: true, muted: true });
  c.text(185, 233, "sqlite3_column_* + UTF-8 decode", { mono: true, size: 12, muted: true });

  c.box(20, 264, 330, 34, { dashed: true, muted: true });
  c.text(185, 286, "715.7 ns (mem) · 1,654 ns (file)", { mono: true, size: 12, muted: true });

  c.text(20, 318, "Native binaries per OS; FFI tax on every row.", { size: 11.5, anchor: "start", muted: true });

  // Vertical divider
  c.line(376, 16, 376, 316, { dashed: true, faint: true });

  // Right: DataVo single JIT / AOT compilation unit
  c.text(400, 26, "DataVo (one managed compilation unit)", { size: 14, weight: 600, anchor: "start" });

  c.box(400, 46, 360, 202);
  c.box(418, 60, 324, 38);
  c.text(580, 84, "db.Table<Player>().FirstOrDefault(...)", { mono: true, size: 11.8, weight: 600 });

  c.arrow(580, 98, 580, 124);
  c.text(592, 115, "[InterceptsLocation]", { mono: true, size: 11, anchor: "start", muted: true });

  c.box(418, 124, 324, 40, { hatch: true });
  c.text(580, 149, "PackedKeyReader<Player>", { size: 13, weight: 600 });

  c.arrow(580, 164, 580, 190);
  c.text(592, 181, "ReadOnlySpan<byte>", { mono: true, size: 11, anchor: "start", muted: true });

  c.box(418, 190, 324, 42, { hatch: true });
  c.text(580, 216, "FlatRowStore (64 B aligned)", { size: 13, weight: 600 });

  c.box(400, 264, 360, 34, { hatch: true });
  c.text(580, 286, "55.0 ns (mem) · 56 B (= Player)", { mono: true, size: 12.2, weight: 600 });

  c.text(400, 318, "Zero native binaries · zero marshalling.", { size: 11.5, anchor: "start", muted: true });

  return c.render();
}

// =============================================================================
// Figure 2: FlatRowStore 32-Byte Record Layout for Player
// =============================================================================
function fig02(theme) {
  const c = createCanvas(
    780,
    276,
    "Figure 2 — FlatRowStore 32-byte widest-first packed record layout for Player and parallel ReferenceDirectory",
    theme
  );

  c.text(20, 26, "FlatRowStore record for Player (32 B fixed + 1 ref slot)", {
    size: 14,
    weight: 600,
    anchor: "start",
  });
  c.text(20, 47, "64-byte aligned chunk (4,096 slots on Pinned Object Heap) · widest-first alignment", {
    size: 11.5,
    anchor: "start",
    muted: true,
  });

  // Main 32-byte record bar: x=20..760 (w=740), y=66..130 (h=64)
  const y0 = 66;
  const h0 = 64;
  const cells = [
    { x: 20, w: 164, title: "Gold (long)", off: "0..7", hatch: true },
    { x: 184, w: 164, title: "Ratio (double)", off: "8..15", hatch: true },
    { x: 348, w: 94, title: "Id (int)", off: "16..19", hatch: true },
    { x: 442, w: 94, title: "Level", off: "20..23", hatch: true },
    { x: 536, w: 50, title: "Act", off: "24", hatch: false },
    { x: 586, w: 52, title: "Stat", off: "25", hatch: false },
    { x: 638, w: 52, title: "Null", off: "26", hatch: false },
    { x: 690, w: 70, title: "Pad", off: "27..31", hatch: false, dashed: true, muted: true },
  ];

  c.box(20, y0, 516, h0, { hatch: true });
  c.box(536, y0, 154, h0);
  c.box(690, y0, 70, h0, { dashed: true, muted: true });

  for (const sep of [184, 348, 442, 586, 638]) {
    c.line(sep, y0, sep, y0 + h0);
  }

  for (const cell of cells) {
    const cx = cell.x + cell.w / 2;
    c.text(cx, y0 + 28, cell.title, {
      size: 12.2,
      weight: cell.muted ? 400 : 600,
      muted: !!cell.muted,
    });
    c.text(cx, y0 + 49, cell.off, {
      mono: true,
      size: 11,
      muted: true,
    });
  }

  // Three crisp bottom callout boxes (2 lines each for breathing room)
  c.box(20, 168, 232, 66);
  c.text(136, 194, "ReferenceDirectory", { size: 12.5, weight: 600 });
  c.text(136, 216, "slot 0 → string Name", { mono: true, size: 11.5, muted: true });

  c.box(274, 168, 232, 66, { hatch: true });
  c.text(390, 194, "13× smaller footprint", { size: 12.5, weight: 600 });
  c.text(390, 216, "640.3 B → 49.2 B / row", { mono: true, size: 11.5, weight: 600 });

  c.box(528, 168, 232, 66, { dashed: true });
  c.text(644, 194, "Status @ 25 · Null @ 26", { size: 12.5, weight: 600 });
  c.text(644, 216, "0=Empty · 1=Packed · 2=Spill", { mono: true, size: 11, muted: true });

  c.arrow(612, 130, 612, 166);
  c.arrow(664, 130, 664, 166, { muted: true });

  c.text(20, 260, "Fixed-width columns sit on natural alignment boundaries; string and vector references sit in a parallel chunk array.", {
    size: 11.5,
    anchor: "start",
    muted: true,
  });

  return c.render();
}

// =============================================================================
// Figure 3: Dual Lookup Pipeline (55 ns In-Memory vs. 216 ns LSM Fallback)
// =============================================================================
function fig03(theme) {
  const c = createCanvas(
    780,
    240,
    "Figure 3 — 55 ns lock-free packed lookup versus 216 ns LSM clustered point path",
    theme
  );

  // Entry box
  c.box(20, 82, 134, 58);
  c.text(87, 106, "FirstOrDefault", { size: 13, weight: 600 });
  c.text(87, 126, "p.Id == id", { mono: true, size: 11.5, muted: true });

  c.arrow(154, 111, 184, 111);

  // Diamond: Packed?
  c.diamond(232, 111, 46, 36);
  c.text(232, 115, "Packed?", { size: 12.2, weight: 600 });

  // Upper branch (yes -> InMemory 55 ns)
  c.polyArrow(
    [
      [232, 75],
      [232, 48],
      [296, 48],
    ],
    {}
  );
  c.text(264, 38, "yes", { mono: true, size: 11.5 });

  c.box(300, 20, 146, 56);
  c.text(373, 44, "PackedKeyReader", { size: 12.5, weight: 600 });
  c.text(373, 64, "PK map → slot", { mono: true, size: 11.2, muted: true });

  c.arrow(446, 48, 468, 48);

  c.box(472, 20, 138, 56, { hatch: true });
  c.text(541, 44, "ReadPackedRow", { size: 12.5, weight: 600 });
  c.text(541, 64, "inlined offsets", { mono: true, size: 11, muted: true });

  c.arrow(610, 48, 632, 48);

  c.box(636, 20, 124, 56, { hatch: true });
  c.text(698, 44, "55.0 ns", { mono: true, size: 13, weight: 600 });
  c.text(698, 64, "56 B · no lock", { mono: true, size: 11, muted: true });

  // Lower branch (no -> LSM File / Tx 216 ns)
  c.polyArrow(
    [
      [232, 147],
      [232, 174],
      [296, 174],
    ],
    { dashed: true, muted: true }
  );
  c.text(264, 165, "no / LSM", { mono: true, size: 11, muted: true });

  c.box(300, 146, 146, 56);
  c.text(373, 170, "PreparedStmt", { size: 12.5, weight: 600 });
  c.text(373, 190, "ExecuteBorrowed", { mono: true, size: 11, muted: true });

  c.arrow(446, 174, 468, 174, { muted: true });

  c.box(472, 146, 138, 56);
  c.text(541, 170, "ClusteredPoint", { size: 12.5 });
  c.text(541, 190, "Latest / mmap", { mono: true, size: 11, muted: true });

  c.arrow(610, 174, 632, 174, { muted: true });

  c.box(636, 146, 124, 56, { dashed: true });
  c.text(698, 170, "216.2 ns", { mono: true, size: 13, weight: 600 });
  c.text(698, 190, "144 B · slot box", { mono: true, size: 11, muted: true });

  c.text(
    20,
    228,
    "In-memory reads bypass SQL and locks; LSM, transactions, and misses fall back cleanly to the prepared statement.",
    { size: 11.5, anchor: "start", muted: true }
  );

  return c.render();
}

// =============================================================================
// Figure 4: Prepared Statement AST Isolation (SqlParameterBinder)
// =============================================================================
function fig04(theme) {
  const c = createCanvas(
    780,
    274,
    "Figure 4 — Prepared statement AST isolation via SqlParameterBinder serving point queries and reactive views concurrently",
    theme
  );

  // Left: Shared immutable template
  c.box(20, 74, 198, 96, { hatch: true });
  c.text(119, 100, "PreparedStatement", { size: 12.8, weight: 600 });
  c.text(119, 124, "WHERE Team=@t LIMIT @k", { mono: true, size: 11 });
  c.text(119, 148, "parsed once · read-only", { size: 11, muted: true });

  c.arrow(218, 122, 246, 122);

  // Middle: SqlParameterBinder
  c.box(250, 88, 146, 68);
  c.text(323, 117, "SqlParameterBinder", { size: 12.2, weight: 600 });
  c.text(323, 138, "clone + bind AST", { mono: true, size: 11, muted: true });

  // Fan-out to 3 isolated consumers
  c.polyArrow([
    [396, 106],
    [418, 106],
    [418, 46],
    [440, 46],
  ]);
  c.arrow(396, 122, 440, 122);
  c.polyArrow([
    [396, 138],
    [418, 138],
    [418, 198],
    [440, 198],
  ]);

  // Consumer 1
  c.box(444, 18, 316, 56);
  c.text(602, 42, "Thread A · ExecuteBorrowed", { size: 12.5, weight: 600 });
  c.text(602, 61, "Team='Red', k=1 · thread-local slot", { mono: true, size: 11, muted: true });

  // Consumer 2
  c.box(444, 94, 316, 56, { hatch: true });
  c.text(602, 118, "SubscribeZeroAlloc", { size: 12.5, weight: 600 });
  c.text(602, 137, "AST #1 (Red, k=3) → QueryChangeRef", { mono: true, size: 11, muted: true });

  // Consumer 3
  c.box(444, 170, 316, 56);
  c.text(602, 194, "Subscribe", { size: 12.5, weight: 600 });
  c.text(602, 213, "AST #2 (Blue, k=5) → QueryChange", { mono: true, size: 11, muted: true });

  c.text(
    20,
    256,
    "The template AST is never mutated in place: concurrent queries and multi-tenant reactive views share one parsed handle.",
    { size: 11.5, anchor: "start", muted: true }
  );

  return c.render();
}

// =============================================================================
// Figure 5: Build-Time Roslyn Codegen vs. Native AOT Runtime
// =============================================================================
function fig05(theme) {
  const c = createCanvas(
    780,
    330,
    "Figure 5 — Build-time Roslyn source generators versus trimmed Native AOT runtime execution",
    theme
  );

  // Left: Build time
  c.text(20, 34, "Build time (Roslyn generators)", { size: 14, weight: 600, anchor: "start" });

  c.box(20, 52, 336, 48);
  c.text(188, 73, "[DataVoTable] + static lambdas", { mono: true, size: 11.8, weight: 600 });
  c.text(188, 91, "Where · OrderBy · Take · Select", { size: 11, muted: true });

  c.arrow(100, 100, 100, 124);
  c.arrow(276, 100, 276, 124);

  c.box(20, 126, 160, 68);
  c.text(100, 148, "TableGenerator", { size: 12.2, weight: 600 });
  c.text(100, 166, "ReadPackedRow", { mono: true, size: 11, muted: true });
  c.text(100, 182, "ReadReactiveRow", { mono: true, size: 11, muted: true });

  c.box(196, 126, 160, 68);
  c.text(276, 148, "LinqInterceptorGen", { size: 12.2, weight: 600 });
  c.text(276, 166, "[InterceptsLocation]", { mono: true, size: 10.8, muted: true });
  c.text(276, 182, "Filter / Projector", { mono: true, size: 11, muted: true });

  c.arrow(100, 194, 100, 218);
  c.arrow(276, 194, 276, 218);

  c.box(20, 220, 336, 56, { hatch: true });
  c.text(188, 243, "Generated C# (zero reflection)", { size: 12.5, weight: 600 });
  c.text(188, 263, "DataVoLinqPlan · Filter · Projector", { mono: true, size: 11, muted: true });

  c.text(20, 308, "Uncompilable shapes warn and stream safely.", { size: 11.5, anchor: "start", muted: true });

  // Vertical compile boundary
  c.line(382, 22, 382, 314, { dashed: true, faint: true });
  c.text(382, 14, "PublishAot=true", { mono: true, size: 10.5, faint: true });

  // Right: Native AOT runtime
  c.text(406, 34, "Native AOT binary (0 IL warnings)", { size: 14, weight: 600, anchor: "start" });

  c.box(406, 56, 354, 60, { hatch: true });
  c.text(583, 81, "Point Lookup", { size: 13, weight: 600 });
  c.text(583, 102, "55.0 ns · 56 B alloc", { mono: true, size: 11.5 });

  c.box(406, 134, 354, 60, { hatch: true });
  c.text(583, 159, "Compiled & [Columnar] Scans", { size: 13, weight: 600 });
  c.text(583, 180, "27.1 – 89.6 µs (200k rows) · 0 B", { mono: true, size: 11.5 });

  c.box(406, 212, 354, 60, { hatch: true });
  c.text(583, 237, "Typed Reactive SubscribeInto", { size: 13, weight: 600 });
  c.text(583, 258, "Memory<T> · 0 B / drain", { mono: true, size: 11.5 });

  c.text(406, 308, "Verified end-to-end by DataVo.AotSmoke.", { size: 11.5, anchor: "start", muted: true });

  return c.render();
}

const FIGURES = [
  ["fig-01-interop-boundary", fig01],
  ["fig-02-flatrowstore-layout", fig02],
  ["fig-03-lookup-pipeline-and-liveness", fig03],
  ["fig-04-prepared-ast-isolation", fig04],
  ["fig-05-roslyn-build-vs-runtime", fig05],
];

for (const [name, fn] of FIGURES) {
  for (const theme of ["light", "dark"]) {
    const outPath = path.join(__dirname, `${name}-${theme}.svg`);
    fs.writeFileSync(outPath, fn(theme), "utf8");
    console.log("wrote", path.relative(process.cwd(), outPath));
  }
}
