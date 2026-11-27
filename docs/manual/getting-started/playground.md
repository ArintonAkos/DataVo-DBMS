---
title: SQL Playground
description: Run DataVo SQL in your browser through the experimental DataVo.Browser WebAssembly runtime.
---

# SQL Playground

This page runs the real DataVo engine in your browser. `DataVo.Browser` compiles `DataVo.Core` to WebAssembly, and the editor below sends each script to it. Nothing leaves your machine.

<SqlEditor />

Press **⌘/Ctrl + Enter** or **Execute SQL** to run every statement in the editor. **Reset Playground** clears the stored databases.

## What to Expect

- **Experimental runtime.** `DataVo.Browser` exists for this playground and the browser test suite. It is not published on npm and carries no compatibility promise. See [Status & Roadmap](/manual/getting-started/roadmap).
- **Same SQL dialect.** The engine is the same one the NuGet packages ship, so the [SQL Reference](/manual/sql/compatibility) applies, including `VECTOR(n)` columns and `HNSW` indexes.
- **Browser storage.** Databases are kept in the page's `localStorage` and survive a reload, so the quota is a few megabytes. Use **Reset Playground** to start over.
- **First run is slower.** The first execution downloads the trimmed 7.7 MB WebAssembly bundle and starts the .NET runtime; later statements run against the warm engine.
