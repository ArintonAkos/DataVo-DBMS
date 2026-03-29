# Documentation Rollout Plan

Generated: 2026-03-29

This plan tracks XML/API documentation completion across the solution to support a no-new-warning quality gate.

Current snapshot:

- CS1591/related documentation diagnostics still open in workspace: 173
- Files fully cleaned in this pass: 9

## Completed In This Pass

- DataVo.Core/Runtime/Security/DatabaseRole.cs
- DataVo.Core/Runtime/Security/SessionPrincipal.cs
- DataVo.Core/StorageEngine/Config/DataVoConfig.cs (DataVoAuthUser section)
- DataVo.Core/Parser/AST/SqlNode.cs (SHOW\* auth-introspection statements)
- DataVo.Core/Runtime/DataVoEngine.cs (security management and introspection methods)
- DataVo.Core/Parser/Types/Row.cs
- DataVo.Core/Parser/Types/ListedTable.cs
- DataVo.Core/Parser/Types/JoinedRowId.cs
- DataVo.Core/Services/TableService.cs

## Remaining High-Volume Areas

1. Parser AST and parser public surface
2. Parser types and statement helpers
3. Storage engines (disk/memory)
4. Transaction/locking public API surface
5. DataVo.Data and DataVo.EntityFrameworkCore public extension surfaces

## Gate Strategy

1. Keep CS1591 visible (do not suppress globally).
2. Land docs in batches by subsystem.
3. Enforce no-new-documentation-warning policy in CI using a baseline comparison.
4. After backlog burn-down, enforce zero CS1591 for release branches.

## Batch Order

1. Parser and AST public nodes
2. Storage engines and transaction surfaces
3. DataVo.Data and EFCore integration layers
4. Remaining utility/public collections and extension methods

## Progress Notes

- 2026-03-29: Completed docs pass for security/auth public surfaces and parser helper collection types.
- 2026-03-29: Added this rollout tracker to keep CI doc-quality work visible and incremental.
