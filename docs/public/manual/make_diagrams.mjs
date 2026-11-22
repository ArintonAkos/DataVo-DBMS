#!/usr/bin/env node
/**
 * Generates paired light/dark monochrome Rough.js SVG diagrams for the
 * DataVo Manual (8 diagrams, 16 SVGs total).
 *
 * Matches the visual palette, stroke weights, hachure fills, 780px viewBox
 * width, and system sans/monospace typography of the blog diagrams.
 *
 * Run from repo root:  node docs/public/manual/make_diagrams.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);
const rough = require(path.resolve(__dirname, "../../node_modules/roughjs"));

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
  let nextSeed = 201;
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
// Figure 1: DataVo Embedded Architecture
// =============================================================================
function fig01Architecture(theme) {
  const c = createCanvas(
    780,
    340,
    "Figure 1 — DataVo embedded architecture from C# interfaces through the query planner to InMemory FlatRowStore, LSM storage, and indexes",
    theme
  );

  c.text(20, 24, "DataVo embedded architecture (in-process .NET)", {
    size: 14,
    weight: 600,
    anchor: "start",
  });

  // Layer 1: Client Interfaces (3 boxes across top)
  c.box(20, 42, 234, 52);
  c.text(137, 64, "Compiled LINQ & Generators", { size: 12.2, weight: 600 });
  c.text(137, 82, "[DataVoTable] · [DataVoQuery]", { mono: true, size: 10.8, muted: true });

  c.box(273, 42, 234, 52);
  c.text(390, 64, "DataVoContext API", { size: 12.2, weight: 600 });
  c.text(390, 82, "Execute · StreamRef · Subscribe", { mono: true, size: 10.8, muted: true });

  c.box(526, 42, 234, 52, { dashed: true });
  c.text(643, 64, "ADO.NET · Dapper · EF Core", { size: 12.2, weight: 600 });
  c.text(643, 82, "DataVoConnection · DbCommand", { mono: true, size: 10.8, muted: true });

  // Arrows down to Execution & Planning layer
  c.arrow(137, 94, 137, 124);
  c.arrow(390, 94, 390, 124);
  c.arrow(643, 94, 643, 124, { muted: true });

  // Layer 2: Query Planner + MVCC + Reactive IVM
  c.box(20, 126, 480, 62, { hatch: true });
  c.text(260, 151, "Query Planner & Typed Fast Paths", { size: 13, weight: 600 });
  c.text(260, 172, "TypedScan · TypedJoin · HybridVector · Compiled Kernels", { mono: true, size: 11, muted: true });

  c.box(520, 126, 240, 62);
  c.text(640, 151, "MVCC & Z-Set IVM Engine", { size: 12.5, weight: 600 });
  c.text(640, 172, "Snapshot Isolation · Delta Queue", { mono: true, size: 10.8, muted: true });

  // Arrows down to Storage & Indexes
  c.arrow(140, 188, 140, 222);
  c.arrow(380, 188, 380, 222);
  c.arrow(640, 188, 640, 222);

  // Layer 3: Storage Engines & Indexes
  c.box(20, 224, 234, 72, { hatch: true });
  c.text(137, 248, "InMemory (FlatRowStore)", { size: 12.5, weight: 600 });
  c.text(137, 267, "64 B aligned packed chunks", { mono: true, size: 10.8, muted: true });
  c.text(137, 284, "lock-free PK map · 0 B scans", { size: 10.8, muted: true });

  c.box(273, 224, 234, 72, { hatch: true });
  c.text(390, 248, "LSM Persistent Engine", { size: 12.5, weight: 600 });
  c.text(390, 267, "WAL → MemTable → SSTables", { mono: true, size: 10.8, muted: true });
  c.text(390, 284, "mmap reads · zone maps", { size: 10.8, muted: true });

  c.box(526, 224, 234, 72);
  c.text(643, 248, "Secondary Indexes", { size: 12.5, weight: 600 });
  c.text(643, 267, "B-Tree · HNSW / Flat Vector", { mono: true, size: 10.8, muted: true });
  c.text(643, 284, "SIMD Columnar · Spatial", { size: 10.8, muted: true });

  c.text(
    20,
    324,
    "Pure managed C# (.NET 10 / Native AOT): no P/Invoke boundary, no native binaries, and no external server process.",
    { size: 11.5, anchor: "start", muted: true }
  );

  return c.render();
}

// =============================================================================
// Figure 2: LSM Write Path
// =============================================================================
function fig02LsmWritePath(theme) {
  const c = createCanvas(
    780,
    286,
    "Figure 2 — LSM commit path from write request through WAL and Arena MemTable to immutable SSTables and columnar zone maps",
    theme
  );

  c.text(20, 24, "LSM write & flush path (StorageMode.Lsm)", {
    size: 14,
    weight: 600,
    anchor: "start",
  });

  // Step 1: Write / Batch
  c.box(20, 56, 128, 62);
  c.text(84, 82, "Write / Batch", { size: 12.5, weight: 600 });
  c.text(84, 102, "INSERT / UPDATE", { mono: true, size: 10.6, muted: true });

  c.arrow(148, 87, 178, 87);

  // Step 2: WAL
  c.box(182, 56, 152, 62, { hatch: true });
  c.text(258, 82, "Write-Ahead Log", { size: 12.5, weight: 600 });
  c.text(258, 102, "append + fsync*", { mono: true, size: 10.8, muted: true });

  c.arrow(334, 87, 364, 87);

  // Step 3: Arena MemTable
  c.box(368, 56, 162, 62, { hatch: true });
  c.text(449, 82, "Arena MemTable", { size: 12.5, weight: 600 });
  c.text(449, 102, "sorted in-memory", { mono: true, size: 10.8, muted: true });

  c.arrow(530, 87, 568, 87);
  c.text(549, 77, "flush", { mono: true, size: 10.5, muted: true });

  // Step 4: Immutable SSTables
  c.box(572, 42, 188, 90, { hatch: true });
  c.text(666, 68, "Immutable SSTables", { size: 12.5, weight: 600 });
  c.text(666, 89, "L0 → L1 compaction", { mono: true, size: 10.8, muted: true });
  c.text(666, 110, "mmap + Bloom filters", { size: 11, muted: true });

  // Vertical branch from SSTables down to Columnar Zone Maps
  c.arrow(666, 132, 666, 166);
  c.text(676, 154, "format v8", { mono: true, size: 10.5, anchor: "start", muted: true });

  c.box(460, 168, 300, 66);
  c.text(610, 193, "Columnar Segments & Zone Maps", { size: 12.5, weight: 600 });
  c.text(610, 214, "4,096-row chunks · min/max/NULL/NaN skip", { mono: true, size: 10.8, muted: true });

  // Durability callout under WAL
  c.arrow(258, 118, 258, 166, { dashed: true, muted: true });
  c.box(20, 168, 410, 66, { dashed: true });
  c.text(225, 193, "*Durability Mode (LsmStrictFsync)", { size: 12.2, weight: 600 });
  c.text(225, 214, "true: group-commit fsync  |  false: OS page-cache write", { mono: true, size: 10.8, muted: true });

  c.text(
    20,
    268,
    "Writes become visible only after WAL commit. Compaction merges SSTables and prunes tombstones in the background.",
    { size: 11.5, anchor: "start", muted: true }
  );

  return c.render();
}

// =============================================================================
// Figure 3: MVCC Snapshot Isolation
// =============================================================================
function fig03MvccSnapshot(theme) {
  const c = createCanvas(
    780,
    300,
    "Figure 3 — MVCC snapshot isolation showing CommitClock, xmin/xmax row version visibility, and first-committer-wins conflict detection",
    theme
  );

  c.text(20, 24, "MVCC snapshot isolation (CommitClock & row versions)", {
    size: 14,
    weight: 600,
    anchor: "start",
  });

  // Top timeline bar: CommitClock
  c.text(20, 56, "CommitClock", { mono: true, size: 11.5, weight: 600, anchor: "start" });
  c.arrow(114, 52, 756, 52);
  for (const [x, lbl] of [
    [170, "t=10"],
    [320, "t=11"],
    [470, "t=12"],
    [620, "t=13"],
  ]) {
    c.line(x, 45, x, 59);
    c.text(x, 38, lbl, { mono: true, size: 10.8, muted: true });
  }

  // Row Version Chain box
  c.box(20, 78, 340, 118);
  c.text(190, 100, "Row #42 version visibility", { size: 12.5, weight: 600 });

  c.box(36, 114, 146, 66, { hatch: true });
  c.text(109, 137, "v1: Score = 100", { mono: true, size: 11.2, weight: 600 });
  c.text(109, 157, "xmin=10 · xmax=12", { mono: true, size: 10.5, muted: true });
  c.text(109, 172, "visible to t=11", { size: 10.5, muted: true });

  c.arrow(182, 147, 200, 147);

  c.box(202, 114, 142, 66, { dashed: true });
  c.text(273, 137, "v2: Score = 250", { mono: true, size: 11.2, weight: 600 });
  c.text(273, 157, "xmin=12 · xmax=∞", { mono: true, size: 10.5, muted: true });
  c.text(273, 172, "visible to t≥12", { size: 10.5, muted: true });

  // Right side: Two concurrent transactions
  c.box(394, 78, 366, 54, { hatch: true });
  c.text(577, 101, "Tx A (snapshot t=11): UPDATE Row #42", { size: 12, weight: 600 });
  c.text(577, 120, "commits first at t=12 → sets v1.xmax = 12", { mono: true, size: 10.8, muted: true });

  c.box(394, 144, 366, 54, { dashed: true });
  c.text(577, 167, "Tx B (snapshot t=11): UPDATE Row #42", { size: 12, weight: 600 });
  c.text(577, 186, "sees v1.xmax=12 > 11 → write-write conflict!", { mono: true, size: 10.8, muted: true });

  // Bottom rule bar
  c.box(20, 214, 740, 48);
  c.text(390, 235, "Visibility rule: row is visible to snapshot S iff (xmin ≤ S) AND (xmax == ∞ OR xmax > S)", {
    mono: true,
    size: 11.4,
    weight: 600,
  });
  c.text(390, 253, "Readers never block writers; concurrent updates to the same key follow first-committer-wins.", {
    size: 11,
    muted: true,
  });

  c.text(
    20,
    286,
    "Savepoints (SAVEPOINT / ROLLBACK TO / RELEASE) undo nested writes inside a transaction without releasing its snapshot.",
    { size: 11.5, anchor: "start", muted: true }
  );

  return c.render();
}

// =============================================================================
// Figure 4: Z-Set IVM Pull-Drain Pipeline
// =============================================================================
function fig04ZsetIvmPull(theme) {
  const c = createCanvas(
    780,
    274,
    "Figure 4 — Z-set incremental view maintenance pull-drain pipeline from Commit through Delta Queue and DispatchPendingNotifications to SubscribeInto",
    theme
  );

  c.text(20, 24, "Z-set IVM pull-drain pipeline (zero background callback storms)", {
    size: 14,
    weight: 600,
    anchor: "start",
  });

  // 4 stages left to right
  c.box(20, 58, 134, 68);
  c.text(87, 84, "1. Commit", { size: 12.5, weight: 600 });
  c.text(87, 104, "+1 / -1 Z-set", { mono: true, size: 11, muted: true });

  c.arrow(154, 92, 184, 92);

  c.box(188, 58, 152, 68, { hatch: true });
  c.text(264, 84, "2. Delta Queue", { size: 12.5, weight: 600 });
  c.text(264, 104, "lock-free buffer", { mono: true, size: 11, muted: true });

  c.arrow(340, 92, 370, 92);

  c.box(374, 58, 198, 68, { hatch: true });
  c.text(473, 82, "3. Pull Drain", { size: 12.5, weight: 600 });
  c.text(473, 101, "DispatchPending", { mono: true, size: 10.6, muted: true });
  c.text(473, 116, "Notifications()", { mono: true, size: 10.6, muted: true });

  c.arrow(572, 92, 602, 92);

  c.box(606, 58, 154, 68);
  c.text(683, 82, "4. Subscribers", { size: 12.5, weight: 600 });
  c.text(683, 101, "SubscribeInto", { mono: true, size: 10.8, muted: true });
  c.text(683, 116, "Memory<HudSlot>", { mono: true, size: 10.6, muted: true });

  // Lower detail boxes
  c.box(20, 158, 356, 68, { dashed: true });
  c.text(198, 183, "Incremental Maintenance (O(Δ) work)", { size: 12.2, weight: 600 });
  c.text(198, 204, "Filter · Join · GroupBy · Sum/Count · Top-K", { mono: true, size: 10.8, muted: true });

  c.box(404, 158, 356, 68, { hatch: true });
  c.text(582, 183, "Caller-controlled thread & 0 B drain", { size: 12.2, weight: 600 });
  c.text(582, 204, "UI / game tick drains once per frame into Memory<T>", { size: 11, muted: true });

  c.arrow(473, 126, 473, 156, { muted: true });
  c.arrow(683, 126, 683, 156, { muted: true });

  c.text(
    20,
    256,
    "Writes enqueue signed row deltas without blocking; your main loop calls DispatchPendingNotifications() when ready to update UI state.",
    { size: 11.5, anchor: "start", muted: true }
  );

  return c.render();
}

// =============================================================================
// Figure 5: Query Planner Dispatch Cascade
// =============================================================================
function fig05PlannerFastPaths(theme) {
  const c = createCanvas(
    780,
    336,
    "Figure 5 — Query planner dispatch cascade trying Typed Single-Table, Typed Join, and Hybrid Vector paths before the General Evaluator fallback",
    theme
  );

  c.text(20, 24, "Query planner dispatch cascade (first matching path wins)", {
    size: 14,
    weight: 600,
    anchor: "start",
  });

  // Left: SELECT input
  c.box(20, 134, 118, 60);
  c.text(79, 159, "SELECT AST", { size: 12.5, weight: 600 });
  c.text(79, 179, "bound params", { mono: true, size: 10.8, muted: true });

  // Vertical spine at x=174 connecting to 4 tiers
  c.line(138, 164, 174, 164);
  c.line(174, 72, 174, 266);

  // Tier 1: Typed Single-Table
  c.arrow(174, 72, 206, 72);
  c.box(210, 44, 550, 56, { hatch: true });
  c.text(226, 66, "1. Typed Single-Table Path", { size: 12.5, weight: 600, anchor: "start" });
  c.text(226, 86, "PK/Index lookup · PK range seek · SIMD Columnar agg · UTF-8 typed scan", {
    mono: true,
    size: 10.8,
    anchor: "start",
    muted: true,
  });

  // Tier 2: Typed Join
  c.arrow(174, 136, 206, 136);
  c.box(210, 108, 550, 56, { hatch: true });
  c.text(226, 130, "2. Typed Join Path (INNER / LEFT on integer keys)", {
    size: 12.5,
    weight: 600,
    anchor: "start",
  });
  c.text(226, 150, "Hash join · DenseInt64Set bit-set probe · Index-driven probe · Top-K", {
    mono: true,
    size: 10.8,
    anchor: "start",
    muted: true,
  });

  // Tier 3: Hybrid Vector
  c.arrow(174, 200, 206, 200);
  c.box(210, 172, 550, 56, { hatch: true });
  c.text(226, 194, "3. Hybrid Vector Path (<=> / <-> ORDER BY ... LIMIT k)", {
    size: 12.5,
    weight: 600,
    anchor: "start",
  });
  c.text(226, 214, "HNSW graph / cost-routed Flat SIMD scan · stored-row candidate filter", {
    mono: true,
    size: 10.8,
    anchor: "start",
    muted: true,
  });

  // Tier 4: General Evaluator Fallback
  c.arrow(174, 266, 206, 266, { dashed: true, muted: true });
  c.box(210, 238, 550, 56, { dashed: true, muted: true });
  c.text(226, 260, "4. General Evaluator (fallback — always correct)", {
    size: 12.5,
    weight: 600,
    anchor: "start",
    muted: true,
  });
  c.text(226, 280, "DISTINCT · CTEs · window functions · RIGHT/FULL/CROSS joins · tx joins", {
    mono: true,
    size: 10.8,
    anchor: "start",
    muted: true,
  });

  c.text(
    20,
    318,
    "A fast path either returns the exact same rows as the general evaluator or declines cleanly; declining is never an error.",
    { size: 11.5, anchor: "start", muted: true }
  );

  return c.render();
}

// =============================================================================
// Figure 6: Zero-Allocation RowRef Read Pass
// =============================================================================
function fig06ZeroAllocRowRef(theme) {
  const c = createCanvas(
    780,
    278,
    "Figure 6 — FlatRowStore packed chunk copied in 1,024-row pages into a stack-only RowRef ref struct for zero-allocation column reads",
    theme
  );

  c.text(20, 24, "Zero-allocation RowRef pass (FlatRowStore → 1,024-row pages)", {
    size: 14,
    weight: 600,
    anchor: "start",
  });

  // Left: FlatRowStore chunk
  c.box(20, 56, 184, 146, { hatch: true });
  c.text(112, 82, "FlatRowStore Chunk", { size: 12.5, weight: 600 });
  c.text(112, 102, "64 B aligned bytes", { mono: true, size: 10.8, muted: true });
  c.line(34, 118, 190, 118, { muted: true });
  c.text(112, 140, "Row 0 | Row 1 | ...", { mono: true, size: 11 });
  c.text(112, 162, "packed fixed cells", { size: 11, muted: true });
  c.text(112, 184, "+ RefDirectory slots", { mono: true, size: 10.6, muted: true });

  // Arrow 1: 1,024-row page borrow
  c.arrow(204, 128, 256, 128);
  c.text(230, 104, "1,024 rows", { mono: true, size: 10.5, muted: true });
  c.text(230, 118, "short lock", { size: 10.5, muted: true });

  // Middle: RowRefStream reusable page buffer
  c.box(260, 74, 174, 110);
  c.text(347, 100, "RowRefStream", { size: 12.5, weight: 600 });
  c.text(347, 120, "reusable page buffer", { mono: true, size: 10.6, muted: true });
  c.text(347, 144, "lock released before", { size: 11, muted: true });
  c.text(347, 162, "foreach body runs", { size: 11, muted: true });

  // Arrow 2: ReadOnlySpan<byte>
  c.arrow(434, 128, 480, 128);
  c.text(457, 116, "span", { mono: true, size: 10.5, muted: true });

  // Right: RowRef ref struct + typed accessors
  c.box(484, 56, 276, 146, { hatch: true });
  c.text(622, 82, "RowRef (readonly ref struct)", { size: 12.5, weight: 600 });
  c.text(622, 102, "stack-only · 0 B managed alloc", { mono: true, size: 10.8, muted: true });
  c.line(498, 116, 746, 116, { muted: true });
  c.text(504, 138, "row.GetInt32(ord)      → int", { mono: true, size: 10.8, anchor: "start" });
  c.text(504, 160, "row.GetFloat(ord)      → float", { mono: true, size: 10.8, anchor: "start" });
  c.text(504, 182, "row.GetStringUtf8(ord) → ReadOnlySpan<byte>", { mono: true, size: 10.6, anchor: "start" });

  c.text(
    20,
    236,
    "Because RowRef is a ref struct, C# forbids escaping it past the foreach iteration — making buffer reuse memory-safe.",
    { size: 11.5, anchor: "start", muted: true }
  );
  c.text(
    20,
    256,
    "Look up column ordinals once before the loop (players.GetOrdinal(\"Score\")); after warm-up, a full scan allocates 0 B.",
    { size: 11.5, anchor: "start", muted: true }
  );

  return c.render();
}

// =============================================================================
// Figure 7: Streaming Modes Comparison
// =============================================================================
function fig07StreamingModes(theme) {
  const c = createCanvas(
    780,
    316,
    "Figure 7 — Comparison of db.Execute full-result buffering, StreamQuery 1,024-row yield return pages with early yield break, and StreamRef 0 B ref struct spans",
    theme
  );

  c.text(20, 24, "Query result modes: Execute vs. StreamQuery vs. StreamRef", {
    size: 14,
    weight: 600,
    anchor: "start",
  });

  // Lane 1: db.Execute
  c.box(20, 46, 170, 68, { dashed: true, muted: true });
  c.text(105, 74, "db.Execute(sql)", { mono: true, size: 11.8, weight: 600, muted: true });
  c.text(105, 96, "full-result buffer", { size: 11, muted: true });

  c.arrow(190, 80, 224, 80, { muted: true });

  c.box(228, 46, 336, 68, { dashed: true, muted: true });
  c.text(396, 74, "Materializes ALL matching rows in memory", { size: 12, muted: true });
  c.text(396, 96, "List<Dictionary<string, object?>>", { mono: true, size: 10.8, muted: true });

  c.arrow(564, 80, 594, 80, { muted: true });

  c.box(598, 46, 162, 68, { dashed: true, muted: true });
  c.text(679, 74, "O(N) RAM", { mono: true, size: 12.2, weight: 600, muted: true });
  c.text(679, 96, "best for small sets", { size: 11, muted: true });

  // Lane 2: db.StreamQuery
  c.box(20, 128, 170, 68);
  c.text(105, 156, "db.StreamQuery()", { mono: true, size: 11.8, weight: 600 });
  c.text(105, 178, "IEnumerable<DataVoRow>", { mono: true, size: 10.3, muted: true });

  c.arrow(190, 162, 224, 162);

  c.box(228, 128, 336, 68);
  c.text(396, 154, "1,024-row yield return pages", { size: 12.2, weight: 600 });
  c.text(396, 176, "early break stops scanning remaining pages", { size: 11, muted: true });

  c.arrow(564, 162, 594, 162);

  c.box(598, 128, 162, 68);
  c.text(679, 154, "O(1) page RAM", { mono: true, size: 12, weight: 600 });
  c.text(679, 176, "full SQL + early exit", { size: 11, muted: true });

  // Lane 3: db.StreamRef / table.StreamRef
  c.box(20, 210, 170, 68, { hatch: true });
  c.text(105, 238, "table.StreamRef()", { mono: true, size: 11.8, weight: 600 });
  c.text(105, 260, "RowRefStream", { mono: true, size: 10.8, muted: true });

  c.arrow(190, 244, 224, 244);

  c.box(228, 210, 336, 68, { hatch: true });
  c.text(396, 236, "1,024-row reused buffer → RowRef", { size: 12.2, weight: 600 });
  c.text(396, 258, "stack-only ref struct spans · no boxing", { mono: true, size: 10.8, muted: true });

  c.arrow(564, 244, 594, 244);

  c.box(598, 210, 162, 68, { hatch: true });
  c.text(679, 236, "0 B / pass", { mono: true, size: 12.5, weight: 600 });
  c.text(679, 258, "max scan throughput", { size: 11, muted: true });

  c.text(
    20,
    300,
    "Choose Execute for small lookups, StreamQuery for large SQL exports or Take(n), and StreamRef for zero-GC table scans.",
    { size: 11.5, anchor: "start", muted: true }
  );

  return c.render();
}

// ---------------------------------------------------------------------------
// Fig 08 — Client Interfaces to DataVo.Core Engine & Storage (mobile-legible)
// ---------------------------------------------------------------------------
function fig08ClientInterfaces(theme) {
  const c = createCanvas(
    780,
    380,
    "Five .NET 10 client interfaces routing into the DataVo.Core engine and storage layers",
    theme
  );

  // Top: Your .NET 10 App
  c.box(170, 14, 440, 44, { hatch: true });
  c.text(390, 41, "Your .NET 10 Application", { size: 15, weight: 600 });

  // Fan-out arrows from app to interface layer
  c.arrow(220, 58, 95, 92);
  c.arrow(320, 58, 270, 92);
  c.arrow(460, 58, 455, 92);
  c.arrow(560, 58, 680, 92, { dashed: true, muted: true });

  // Interface 1: Compiled LINQ (hatched to highlight zero-alloc fast path)
  c.box(18, 96, 156, 76, { hatch: true });
  c.text(96, 126, "Compiled LINQ", { size: 14, weight: 600 });
  c.text(96, 146, "DataVo.Generators", { mono: true, size: 12.8, muted: true });
  c.text(96, 164, "55 ns · 0 B scans", { mono: true, size: 12.5, weight: 600 });

  // Interface 2: DataVoContext
  c.box(192, 96, 156, 76);
  c.text(270, 126, "DataVoContext", { size: 14, weight: 600 });
  c.text(270, 146, "DataVo.Core", { mono: true, size: 12.8, muted: true });
  c.text(270, 164, "SQL · Vector · IVM", { size: 12.8, muted: true });

  // Interface 3 & 4: Dapper -> ADO.NET
  c.box(366, 96, 212, 76);
  c.text(472, 121, "Dapper  →  ADO.NET", { size: 14, weight: 600 });
  c.text(472, 144, "DataVo.Data", { mono: true, size: 12.8, muted: true });
  c.text(472, 164, "DbConnection / Reader", { size: 12.8, muted: true });

  // Interface 5: EF Core Bridge (dashed preview)
  c.box(596, 96, 166, 76, { dashed: true, muted: true });
  c.text(679, 126, "EF Core Bridge", { size: 14, weight: 600, muted: true });
  c.text(679, 146, "Preview · No AOT", { size: 12.8, muted: true });
  c.text(679, 164, "DbContext queries", { size: 12.8, muted: true });

  // Arrows into DataVo.Core Engine
  c.arrow(96, 172, 140, 212);
  c.arrow(270, 172, 290, 212);
  c.arrow(472, 172, 472, 212);
  c.arrow(679, 172, 640, 212, { dashed: true, muted: true });

  // Core Engine box
  c.box(46, 216, 688, 56);
  c.text(390, 241, "DataVo.Core Engine", { size: 15, weight: 600 });
  c.text(390, 261, "SQL Parser  ·  Typed Fast Paths  ·  MVCC  ·  HNSW & Columnar Indexes", {
    size: 13.2,
    muted: true,
  });

  // Arrows down to Storage Modes
  c.arrow(170, 272, 150, 304);
  c.arrow(390, 272, 390, 304);
  c.arrow(610, 272, 630, 304, { dashed: true, muted: true });

  // Storage row (3 clean cards)
  c.box(46, 308, 210, 56);
  c.text(151, 333, "InMemory", { size: 14, weight: 600 });
  c.text(151, 353, "FlatRowStore buffers", { mono: true, size: 12.8, muted: true });

  c.box(285, 308, 210, 56, { hatch: true });
  c.text(390, 333, "LSM (Recommended)", { size: 14, weight: 600 });
  c.text(390, 353, "WAL · SSTables · mmap", { mono: true, size: 12.8, muted: true });

  c.box(524, 308, 210, 56, { dashed: true, muted: true });
  c.text(629, 333, "Disk (Legacy)", { size: 14, weight: 600, muted: true });
  c.text(629, 353, "v0.1 / v0.2 compat", { mono: true, size: 12.8, muted: true });

  return c.render();
}

const FIGURES = [
  ["fig-01-architecture", fig01Architecture],
  ["fig-02-lsm-write-path", fig02LsmWritePath],
  ["fig-03-mvcc-snapshot", fig03MvccSnapshot],
  ["fig-04-zset-ivm-pull", fig04ZsetIvmPull],
  ["fig-05-planner-fast-paths", fig05PlannerFastPaths],
  ["fig-06-zero-alloc-rowref", fig06ZeroAllocRowRef],
  ["fig-07-streaming-modes", fig07StreamingModes],
  ["fig-08-client-interfaces", fig08ClientInterfaces],
];

for (const [name, fn] of FIGURES) {
  for (const theme of ["light", "dark"]) {
    const outPath = path.join(__dirname, `${name}-${theme}.svg`);
    fs.writeFileSync(outPath, fn(theme), "utf8");
    console.log("wrote", path.relative(process.cwd(), outPath));
  }
}
