# DataVo Docs Audit Report
**Date:** 2026-07-04

## Executive Summary

The generated DataVo manual is close to launch quality. It does not read like generic AI filler: the tone is restrained, the claims are bounded, and the pages repeatedly make the right alpha disclosures around PostgreSQL compatibility, storage-format stability, durability, EF support, and isolation. The strongest material is where the docs sound like a systems engineer trying to keep users out of trouble: WAL durability, LSM strict versus relaxed mode, transaction limits, unsupported PostgreSQL features, and the embedded deployment model.

The structure mostly follows the intended PostgreSQL-manual style: concept first, runnable or near-runnable examples next, summary matrix last. The recurring weakness is templating. Almost every page ends with a similar support table, and several sections reuse the same Ada/Grace/User examples and "supported subset" language. The docs are not launch-blocked editorially, but they need a final human pass for duplicated phrasing, incomplete EF/vector snippets, and a few reference pages that are still more tutorial than reference.

- Overall AI-Tell Score: 3/10
- Overall Structure Score: 8/10
- Overall Copy-Paste Factor: Mostly strong. SQL and core `DataVoContext` examples are useful; EF/vector and reference diagnostics need tightening.

## Section Breakdown

### Preface

- AI-Tell Score: 3/10
- Structure Score: 8/10
- Copy-Paste Factor: Low risk. The code is short, credible, and appropriate for positioning pages.
- Remarks: "What Is DataVo?" is the best page in the section. It gives the reader a SQLite-style mental model, then immediately proves the API with an in-memory program, an LSM configuration, and a vector SQL example. "v0.1 Alpha Scope" and "Roadmap" are honest about unfinished areas, which is exactly right for an alpha database engine. The weakness is repetition: all three pages restate the same embedded/API/storage/SQL boundary, then end in similar support-summary tables.
- Action Items: Give each preface page a sharper job. "What Is DataVo?" should sell the mental model; "Alpha Scope" should be the contract; "Roadmap" should describe sequencing and priorities. Rewrite broad phrases such as "performance direction" and "package polish" into concrete launch work. Consider reducing the support table density in the preface so it does not feel mechanically generated.

### Tutorial

- AI-Tell Score: 2.5/10
- Structure Score: 8.5/10
- Copy-Paste Factor: Mostly low risk, with one visible exception.
- Remarks: The tutorial section has the best instructional flow in the manual. "Quickstart" starts from project creation, builds the database one step at a time, shows a full file, and includes expected output. "Using DataVoContext" explains sessions and transaction ownership clearly. "Vector Search Example" is also practical because it uses three-dimensional vectors for readability while naming `VECTOR(1536)` as the production shape.
- Action Items: Fix the EF vector snippet in "Entity Framework Example"; it introduces `ItemEmbedding` and `ctx` without defining either. Add expected output to the L2/vector-filter examples or explain why exact floating-point output may vary. Reduce repeated `Users`/`Ada` examples once the user has moved beyond the quickstart; later tutorial pages would feel more authored with examples tailored to their topic.

### SQL Language

- AI-Tell Score: 3/10
- Structure Score: 8.5/10
- Copy-Paste Factor: Low risk. Most SQL snippets are compact, realistic, and usable after the setup shown on the page.
- Remarks: This section successfully reads like a manual. "Supported SQL" gives the broad dialect boundary without opening with a wall of tables. "Queries" progresses logically through projection, filtering, ordering, aggregation, joins, subqueries, `UNION ALL`, and vector ranking. "SQL Compatibility Matrix" correctly shows examples before the compatibility table, so the matrix feels like a summary instead of the page's main content.
- Action Items: Add one or two actual unsupported-syntax examples with the error behavior users should expect. Replace repeated abstract nouns like "surface" and "shape" when a more specific word works. For example, "runtime SQL surface" can usually become "runtime SQL support", "query support", or the exact feature being discussed.

### Storage Engine

- AI-Tell Score: 2/10
- Structure Score: 8/10
- Copy-Paste Factor: Low risk. The configuration snippets are practical and tied to real durability choices.
- Remarks: This is the most credible section. "WAL And Durability" is clear about why benchmark numbers must include durability mode. "Transactions and MVCC" is candid about session-bound transactions, lock-free point reads, row/table coordination, and the lack of full serializable isolation. "LSM Mode" earns trust by naming concrete internals: WAL segments, MemTables, SSTables, manifest edits, background flush, and the 32 MB threshold.
- Action Items: Add a short glossary-style paragraph before or inside "LSM Mode" defining WAL, MemTable, SSTable, and manifest for readers who are not storage-engine specialists. Expand "Storage Modes" with decision criteria beyond one sentence per mode: test/demo, simple persistence, write-heavy durable workload, rebuildable cache, and benchmark-only. Replace "benchmark ceilings" with a plainer phrase such as "upper-bound benchmark runs."

### Client Interfaces

- AI-Tell Score: 4/10
- Structure Score: 7/10
- Copy-Paste Factor: Moderate risk. This is the section most likely to break reader trust if copied directly.
- Remarks: "Roslyn Source Generators" is concise and useful: it defines a projection, declares a `[DataVoQuery]`, calls the generated method, and explains the manifest path. "Native AOT" is appropriately cautious and does not overclaim. The EF material is weaker because it appears in both Tutorial and Client Interfaces with similar language, and the vector EF example is not complete enough to copy.
- Action Items: Create one canonical compile-ready EF sample that includes packages/usings, entity, context, options, write path, read path, and any vector entity if vector EF is shown. Then make the tutorial and reference pages point to different slices of that single example. Fix the dangling line in "Entity Framework Support": "Use query capability checks before running a LINQ query that may be outside the supported alpha subset." It needs the promised capability-check snippet or should be removed.

### Reference

- AI-Tell Score: 3.5/10
- Structure Score: 7.5/10
- Copy-Paste Factor: Low to moderate risk. Examples are useful, but some reference material lacks exact outputs/defaults.
- Remarks: "Limits" and "Unsupported Features" are launch-critical and mostly pass: they state product boundaries without burying users in caveats. "Error Handling" gives the right defensive pattern around `QueryResult.IsError`, `Messages`, `Fields`, and `Data`. "AI Access And MCP" is clear about public AI exports and MCP boundaries. "Configuration Reference" is the least reference-like page; it walks through examples but does not give a consolidated option/default table for the knobs it names.
- Action Items: Add a configuration option table with names, types, defaults, modes affected, and risk notes. Add realistic `QueryResult.Messages` examples to "Error Handling", including one parser or missing-table failure. In "AI Access And MCP", replace "access-denied/not-found response" with the exact response shape if that is stable; otherwise say the page is unavailable through the public MCP server.

## The "Robotic Hitlist"

These exact quotes from the documentation should be rewritten or completed before launch:

- "The supported path starts with direct embedding."
- "The SQL surface is intentionally documented as a subset."
- "What makes this an alpha is the boundary around the edges."
- "The current foundation is the embedded API."
- "The SQL roadmap is about clarity before breadth."
- "The tooling roadmap is separate."
- "Use generated queries for stable point reads, inserts, and narrow fixed-shape updates."
- "The general path is what you use first."
- "Planner and vector knobs are available for advanced experiments."
- "Relaxed LSM mode is a performance setting, not the same durability contract."
- "It is appropriate for caches, rebuildable data, and benchmark ceilings, but recent acknowledged writes can be lost on power or kernel failure."
- "This page exists so early users do not have to infer product boundaries from source files, tests, or roadmap notes."
- "Use query capability checks before running a LINQ query that may be outside the supported alpha subset."
- "This API validates the LINQ expression and either executes the supported subset or reports a clear unsupported-pattern error."
- "That makes it suitable for pasting into an assistant, an issue, or a retrieval test."

## Launch Readiness Notes

The docs are not suffering from the classic AI-manual problem: they avoid the obvious banned words, they are not breathless, and they do not make fake enterprise claims. The main editorial problem is a visible generation pattern: short concept paragraph, code block, another short paragraph, code block, support table. That pattern is not bad on one page, but across the whole manual it becomes noticeable.

Highest-priority fixes before launch:

1. Make all EF/vector examples compile or clearly mark them as fragments.
2. Add actual outputs and actual error messages where the docs teach behavior.
3. Reduce duplicate support tables in pages that already explain the boundary clearly.
4. Replace repeated house-style abstractions: "surface", "shape", "path", "subset", "boundary", and "advanced experiments" where a concrete noun would be clearer.
5. Add consolidated reference tables for configuration and diagnostics so the Reference section earns its name.
