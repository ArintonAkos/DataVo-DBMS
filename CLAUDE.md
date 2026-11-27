# CLAUDE.md

Project guidance for Claude Code in `DataVo-DBMS`.

## Storage Backends

- **Active backends:** `InMemory` (`DataVo.Core/StorageEngine/Memory/InMemoryStorageEngine.cs`) and `Lsm` (`DataVo.Core/StorageEngine/Lsm/`). Build all new features, performance work, columnar extensions, and fast paths for these backends.
- **`StorageMode.Disk` / `DiskStorageEngine` were removed in v0.5.0.** `InMemory` and `Lsm` are the only storage modes; do not reintroduce a paged engine or write compatibility code for its data files.

## Engineering & Verification Guardrails

- **Native AOT locked:** `DataVo.Core`, `DataVo.Data`, and `DataVo.VectorData` treat IL trim/AOT diagnostics (`IL2026`/`IL3050`) as build errors; `DataVo.Generators` targets `netstandard2.0`. Always verify with `bash scripts/check-aot-baseline.sh`.
- **Code style:** Follow `.agents/rules/code-style-guide.md` (concise XML doc comments on public/internal members, small focused methods, shared helpers instead of duplicate logic).
- **Test discipline:** Never weaken or skip test assertions to make a suite pass.
- **Quota/build hygiene:** During multi-step work, iterate with targeted `dotnet test DataVo.Tests --filter ...` (and `DataVo.Generators.Tests` when touching generators); run full `dotnet test`, `scripts/check-aot-baseline.sh`, and BenchmarkDotNet at milestone verification gates.
