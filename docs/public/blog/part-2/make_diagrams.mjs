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
    318,
    "Figure 1 — SQLite managed-to-native P/Invoke boundary versus DataVo single managed compilation unit",
    theme
  );

  // Left: SQLite via ADO.NET / Dapper
  c.text(20, 26, "SQLite via ADO.NET / Dapper", { size: 14, weight: 600, anchor: "start" });

  c.box(20, 44, 332, 38);
  c.text(186, 68, ".NET App + SqliteCommand / Reader", { size: 12.5, weight: 600 });

  c.arrow(186, 82, 186, 114, { muted: true });
  c.text(196, 102, "marshal params (608 B)", { mono: true, size: 10.5, anchor: "start", muted: true });

  // Interop wall
  c.line(20, 120, 352, 120, { dashed: true, faint: true });
  c.text(186, 134, "P/Invoke ABI boundary (JIT cannot inline)", { size: 11, muted: true });

  c.box(20, 144, 332, 38, { dashed: true, muted: true });
  c.text(186, 168, "sqlite3_bind_* → sqlite3_step() (C VM)", { mono: true, size: 11.5, muted: true });

  c.arrow(186, 182, 186, 200, { muted: true });

  c.box(20, 200, 332, 38, { dashed: true, muted: true });
  c.text(186, 224, "sqlite3_column_* + UTF-8 → UTF-16 decode", { mono: true, size: 11.5, muted: true });

  c.box(20, 254, 332, 32, { dashed: true, muted: true });
  c.text(186, 275, "715.7 ns (mem) · 1,654 ns (file) · 608 B", { mono: true, size: 11.5, muted: true });

  c.text(20, 306, "Native .dll/.so/.dylib per OS; FFI tax on every row.", { size: 11.5, anchor: "start", muted: true });

  // Vertical divider
  c.line(376, 16, 376, 306, { dashed: true, faint: true });

  // Right: DataVo single JIT / AOT compilation unit
  c.text(400, 26, "DataVo (one JIT / AOT compilation unit)", { size: 14, weight: 600, anchor: "start" });

  c.box(400, 44, 360, 194);
  c.box(416, 58, 328, 36);
  c.text(580, 81, "db.Table<Player>().FirstOrDefault(...)", { mono: true, size: 11.5, weight: 600 });

  c.arrow(580, 94, 580, 118);
  c.text(590, 110, "[InterceptsLocation]", { mono: true, size: 10.5, anchor: "start", muted: true });

  c.box(416, 118, 328, 38, { hatch: true });
  c.text(580, 142, "PackedKeyReader<Player> (inlined)", { size: 12.5, weight: 600 });

  c.arrow(580, 156, 580, 180);
  c.text(590, 172, "ReadOnlySpan<byte> + ref", { mono: true, size: 10.5, anchor: "start", muted: true });

  c.box(416, 180, 328, 42, { hatch: true });
  c.text(580, 206, "FlatRowStore (64-byte aligned POH)", { size: 12.5, weight: 600 });

  c.box(400, 254, 360, 32, { hatch: true });
  c.text(580, 275, "55.0 ns (mem) · 56 B (= Player object)", { mono: true, size: 12, weight: 600 });

  c.text(400, 306, "Zero native binaries · zero marshalling · 0 B engine overhead.", { size: 11.5, anchor: "start", muted: true });

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

  c.text(20, 26, "FlatRowStore record for Player (32 B fixed record + 1 ref slot)", {
    size: 14,
    weight: 600,
    anchor: "start",
  });
  c.text(20, 46, "64-byte aligned FlatChunk (4,096 slots on Pinned Object Heap) · widest-first alignment", {
    size: 11.5,
    anchor: "start",
    muted: true,
  });

  // Main 32-byte record bar: x=20..760 (w=740), y=62..124 (h=62)
  const y0 = 62;
  const h0 = 62;
  const cells = [
    { x: 20, w: 164, title: "Gold (long)", off: "0..7 (8 B)", hatch: true },
    { x: 184, w: 164, title: "Ratio (double)", off: "8..15 (8 B)", hatch: true },
    { x: 348, w: 94, title: "Id (int)", off: "16..19", hatch: true },
    { x: 442, w: 94, title: "Level", off: "20..23", hatch: true },
    { x: 536, w: 50, title: "Act", off: "24", hatch: false },
    { x: 586, w: 52, title: "Stat", off: "25", hatch: false },
    { x: 638, w: 52, title: "Null", off: "26", hatch: false },
    { x: 690, w: 70, title: "Pad 5B", off: "27..31", hatch: false, dashed: true, muted: true },
  ];

  // Outer hachure on fixed-width values 0..23
  c.box(20, y0, 516, h0, { hatch: true });
  // Status/Act/Null cells
  c.box(536, y0, 154, h0);
  // Pad cell
  c.box(690, y0, 70, h0, { dashed: true, muted: true });

  // Vertical separators inside record
  for (const sep of [184, 348, 442, 586, 638]) {
    c.line(sep, y0, sep, y0 + h0);
  }

  for (const cell of cells) {
    const cx = cell.x + cell.w / 2;
    c.text(cx, y0 + 27, cell.title, {
      size: 12,
      weight: cell.muted ? 400 : 600,
      muted: !!cell.muted,
    });
    c.text(cx, y0 + 48, cell.off, {
      mono: true,
      size: 10.5,
      muted: true,
    });
  }

  // Three bottom callout boxes (no crossing arrows)
  // Left: ReferenceDirectory
  c.box(20, 162, 240, 72);
  c.text(140, 184, "ReferenceDirectory (parallel)", { size: 12, weight: 600 });
  c.text(140, 204, "slot 0 → string Name", { mono: true, size: 11.5 });
  c.text(140, 223, "Volatile.Read on load · same row idx", { size: 10.5, muted: true });

  // Middle: Memory comparison
  c.box(280, 162, 220, 72, { hatch: true });
  c.text(390, 184, "Retained memory (13× drop)", { size: 12, weight: 600 });
  c.text(390, 205, "640.3 B → 49.2 B / row", { mono: true, size: 11.5, weight: 600 });
  c.text(390, 223, "(shared strings; 145.3 B unique)", { size: 10.5, muted: true });

  // Right: Status + NullBits + KeptApart escape hatch
  c.box(520, 162, 240, 72, { dashed: true });
  c.text(640, 184, "Status @ 25 · NullBits @ 26", { size: 12, weight: 600 });
  c.text(640, 204, "0=Empty 1=Packed 2=KeptApart", { mono: true, size: 10.8 });
  c.text(640, 223, "Side spill map only if Status = 2", { size: 10.5, muted: true });

  // Vertical arrows from Status & NullBits down to right callout
  c.arrow(612, 124, 612, 160);
  c.arrow(664, 124, 664, 160, { muted: true });

  c.text(20, 260, "Every fixed-width column sits on a multiple of its own width; string/vector references sit in a parallel chunk array.", {
    size: 11.5,
    anchor: "start",
    muted: true,
  });

  return c.render();
}

// =============================================================================
// Figure 3: Dual Lookup Pipeline (55 ns vs 216 ns) + ARM64 Lock-Free Ordering
// =============================================================================
function fig03(theme) {
  const c = createCanvas(
    780,
    364,
    "Figure 3 — 55 ns lock-free packed lookup versus 216 ns LSM fallback and ARM64 volatile memory ordering",
    theme
  );

  // Entry box
  c.box(20, 76, 134, 54);
  c.text(87, 99, "FirstOrDefault", { size: 12.5, weight: 600 });
  c.text(87, 117, "p.Id == id", { mono: true, size: 11.5, muted: true });

  c.arrow(154, 103, 184, 103);

  // Diamond: Packed?
  c.diamond(232, 103, 46, 34);
  c.text(232, 107, "Packed?", { size: 12, weight: 600 });

  // Upper branch (yes -> InMemory 55 ns)
  c.polyArrow(
    [
      [232, 69],
      [232, 46],
      [296, 46],
    ],
    {}
  );
  c.text(264, 38, "yes", { mono: true, size: 11.5 });

  c.box(300, 20, 146, 52);
  c.text(373, 42, "PackedKeyReader", { size: 12, weight: 600 });
  c.text(373, 60, "PK map → slot", { mono: true, size: 11, muted: true });

  c.arrow(446, 46, 468, 46);

  c.box(472, 20, 138, 52, { hatch: true });
  c.text(541, 42, "ReadPackedRow", { size: 12, weight: 600 });
  c.text(541, 60, "DataVoPackedRow", { mono: true, size: 10.5, muted: true });

  c.arrow(610, 46, 632, 46);

  c.box(636, 20, 124, 52, { hatch: true });
  c.text(698, 42, "55.0 ns", { mono: true, size: 12.5, weight: 600 });
  c.text(698, 60, "56 B · no lock", { mono: true, size: 10.8, muted: true });

  // Lower branch (no -> LSM File / Tx 216 ns)
  c.polyArrow(
    [
      [232, 137],
      [232, 160],
      [296, 160],
    ],
    { dashed: true, muted: true }
  );
  c.text(264, 152, "no / LSM", { mono: true, size: 10.8, muted: true });

  c.box(300, 134, 146, 52);
  c.text(373, 156, "PreparedStmt", { size: 12, weight: 600 });
  c.text(373, 174, "ExecuteBorrowed", { mono: true, size: 10.8, muted: true });

  c.arrow(446, 160, 468, 160, { muted: true });

  c.box(472, 134, 138, 52);
  c.text(541, 156, "ClusteredPoint", { size: 12 });
  c.text(541, 174, "Latest / mmap", { mono: true, size: 10.8, muted: true });

  c.arrow(610, 160, 632, 160, { muted: true });

  c.box(636, 134, 124, 52, { dashed: true });
  c.text(698, 156, "216.2 ns", { mono: true, size: 12.5, weight: 600 });
  c.text(698, 174, "144 B · slot box", { mono: true, size: 10.5, muted: true });

  // Divider before ARM64 memory-ordering panel
  c.line(20, 208, 760, 208, { dashed: true, faint: true });

  c.text(20, 232, "Lock-free optimistic read vs. concurrent delete (weakly ordered ARM64 / M4 Pro)", {
    size: 13,
    weight: 600,
    anchor: "start",
  });

  // Writer lane (left)
  c.text(20, 254, "Writer (FlatRowStore.Remove)", { size: 11.5, weight: 600, anchor: "start", muted: true });
  c.box(20, 264, 158, 48);
  c.text(99, 285, "1. Volatile.Write", { mono: true, size: 11, weight: 600 });
  c.text(99, 302, "Status = Empty", { mono: true, size: 11, muted: true });

  c.arrow(178, 288, 200, 288);

  c.box(204, 264, 158, 48);
  c.text(283, 285, "2. Volatile.Write", { mono: true, size: 11, weight: 600 });
  c.text(283, 302, "refs[slot] = null", { mono: true, size: 11, muted: true });

  // Vertical mini divider
  c.line(384, 248, 384, 318, { dashed: true, faint: true });

  // Reader lane (right)
  c.text(406, 254, "Reader (PackedKeyReader.TryReadFrom)", { size: 11.5, weight: 600, anchor: "start", muted: true });
  c.box(406, 264, 162, 48);
  c.text(487, 285, "1. Volatile.Read", { mono: true, size: 11, weight: 600 });
  c.text(487, 302, "copy fields + refs", { mono: true, size: 11, muted: true });

  c.arrow(568, 288, 590, 288);

  c.box(594, 264, 166, 48, { hatch: true });
  c.text(677, 285, "2. Volatile.Read", { mono: true, size: 11, weight: 600 });
  c.text(677, 302, "IsTypedLive() ?", { mono: true, size: 11 });

  c.text(
    20,
    346,
    "Opposite volatile order guarantees: any reader that observes a cleared ref also observes Status=Empty and retries via statement.",
    { size: 11.2, anchor: "start", muted: true }
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
  c.text(119, 98, "DataVoPreparedStatement", { size: 12.2, weight: 600 });
  c.text(119, 118, "SELECT Id, Name, Score", { mono: true, size: 10.5 });
  c.text(119, 134, "WHERE Team=@t LIMIT @k", { mono: true, size: 10.5 });
  c.text(119, 155, "parsed once · immutable AST", { size: 10.5, muted: true });

  c.arrow(218, 122, 246, 122);

  // Middle: SqlParameterBinder
  c.box(250, 88, 146, 68);
  c.text(323, 116, "SqlParameterBinder", { size: 12, weight: 600 });
  c.text(323, 136, "clone + bind AST", { mono: true, size: 11, muted: true });

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

  // Consumer 1: Point / prepared execution
  c.box(444, 18, 316, 56);
  c.text(602, 40, "Thread A · ExecuteBorrowed((t,\"Red\"), (k,1))", { mono: true, size: 10.8, weight: 600 });
  c.text(602, 59, "Fast-path plan → thread-local RowResultSlot", { size: 11, muted: true });

  // Consumer 2: Borrowed reactive view
  c.box(444, 94, 316, 56, { hatch: true });
  c.text(602, 116, "SubscribeZeroAlloc(stmt, (t,\"Red\"), (k,3))", { mono: true, size: 10.8, weight: 600 });
  c.text(602, 135, "Isolated AST #1 → TopKReactiveQuery (QueryChangeRef)", { size: 10.8, muted: true });

  // Consumer 3: Owned reactive view
  c.box(444, 170, 316, 56);
  c.text(602, 192, "Subscribe(stmt, (t,\"Blue\"), (k,5))", { mono: true, size: 10.8, weight: 600 });
  c.text(602, 211, "Isolated AST #2 → TopKReactiveQuery (QueryChange)", { size: 10.8, muted: true });

  c.text(
    20,
    256,
    "Template AST is never mutated in place: concurrent queries and multi-tenant reactive views share one parsed handle.",
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
  c.text(20, 34, "Build time (Roslyn IIncrementalGenerator)", { size: 13.8, weight: 600, anchor: "start" });

  c.box(20, 52, 336, 46);
  c.text(188, 71, "[DataVoTable] Player + static LINQ lambdas", { mono: true, size: 11.2, weight: 600 });
  c.text(188, 88, "Where · OrderBy · Take · Select · FirstOrDefault", { size: 10.8, muted: true });

  c.arrow(100, 98, 100, 122);
  c.arrow(276, 98, 276, 122);

  c.box(20, 124, 160, 68);
  c.text(100, 145, "DataVoTableGenerator", { size: 11.5, weight: 600 });
  c.text(100, 163, "ReadPackedRow", { mono: true, size: 10.8, muted: true });
  c.text(100, 179, "ReadReactiveRow", { mono: true, size: 10.8, muted: true });

  c.box(196, 124, 160, 68);
  c.text(276, 145, "LinqInterceptorGen", { size: 11.5, weight: 600 });
  c.text(276, 163, "[InterceptsLocation]", { mono: true, size: 10.5, muted: true });
  c.text(276, 179, "IRowRefFilter / Proj", { mono: true, size: 10.5, muted: true });

  c.arrow(100, 192, 100, 216);
  c.arrow(276, 192, 276, 216);

  c.box(20, 218, 336, 58, { hatch: true });
  c.text(188, 240, "Generated C# (0 reflection · 0 Expression.Compile)", { size: 11.8, weight: 600 });
  c.text(188, 260, "DataVoLinqPlan · IColumnarFilter · RowRefProjector", { mono: true, size: 10.5 });

  c.text(20, 308, "Uncompilable shapes warn DATAVOL001–004 and stream safely.", { size: 11.2, anchor: "start", muted: true });

  // Vertical compile boundary
  c.line(382, 22, 382, 314, { dashed: true, faint: true });
  c.text(382, 14, "PublishAot=true", { mono: true, size: 10, faint: true });

  // Right: Native AOT runtime
  c.text(406, 34, "Native AOT binary (IL2026 / IL3050 = 0)", { size: 13.8, weight: 600, anchor: "start" });

  c.box(406, 56, 354, 60, { hatch: true });
  c.text(583, 80, "Point Lookup (PackedKeyReader)", { size: 12.5, weight: 600 });
  c.text(583, 101, "55.0 ns · 56 B alloc (Apple M4 Pro)", { mono: true, size: 11.2 });

  c.box(406, 134, 354, 60, { hatch: true });
  c.text(583, 158, "Compiled Scan & [Columnar] Kernels", { size: 12.5, weight: 600 });
  c.text(583, 179, "27.1 – 89.6 µs (200k rows) · 0 B alloc", { mono: true, size: 11.2 });

  c.box(406, 212, 354, 60, { hatch: true });
  c.text(583, 236, "Typed Reactive SubscribeInto(Memory<T>)", { size: 12.5, weight: 600 });
  c.text(583, 257, "BufferedWindowReactiveQuery · 0 B / drain", { mono: true, size: 11.2 });

  c.text(406, 308, "Verified end-to-end by DataVo.AotSmoke native binary.", { size: 11.2, anchor: "start", muted: true });

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
