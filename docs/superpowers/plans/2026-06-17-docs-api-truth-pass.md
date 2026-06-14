# Docs API Truth Pass Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make public docs and package metadata match the actual current API and maturity status.

**Architecture:** This is a documentation and metadata correction slice. It does not change runtime behavior. It adds a focused docs consistency check so broken public examples are easier to catch later.

**Tech Stack:** Markdown, MSBuild props, shell/rg verification, existing .NET tests.

---

## File Structure

- Modify: `README.md`
  - Replace invalid `DatabasePath` with `DiskStoragePath`.
  - Replace nonexistent `DataVoContext.ExecuteWithParams` examples with compilable `DataVo.Data` command examples or literal vector SQL examples.
  - Align status language with preview/production-hardening reality.
- Modify: `docs/features/vector-queries-guide.md`
  - Apply the same API corrections for vector examples.
- Modify: `docs/DataVo.Core/Parser/DQL/index.md`
  - Replace generated-looking prose with concrete DQL responsibilities.
- Modify: `docs/features/roadmap-and-integrations.md`
  - Ensure production-readiness language is not contradicted by README.
- Modify: `Directory.Build.props`
  - Replace stale package repository metadata with the correct DataVo repository URL if known from repo remotes; otherwise use a neutral project URL placeholder only if already accepted elsewhere.
- Create: `scripts/check-doc-api-samples.sh`
  - Fail on known invalid docs symbols: `ExecuteWithParams`, `DatabasePath`.

## Task 1: Add Docs Drift Guard

**Files:**
- Create: `scripts/check-doc-api-samples.sh`

- [ ] **Step 1: Write the failing guard script**

Create:

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if rg -n "ExecuteWithParams|DatabasePath" "$ROOT/README.md" "$ROOT/docs/features" >/tmp/datavo-doc-api-drift.txt; then
  cat /tmp/datavo-doc-api-drift.txt
  echo "Docs contain API symbols that do not exist in the current C# surface." >&2
  exit 1
fi
```

- [ ] **Step 2: Run guard to verify it fails**

Run: `bash scripts/check-doc-api-samples.sh`

Expected: FAIL and report `ExecuteWithParams` / `DatabasePath` occurrences.

- [ ] **Step 3: Keep script executable**

Run: `chmod +x scripts/check-doc-api-samples.sh`

Expected: exit 0.

## Task 2: Correct Public API Examples

**Files:**
- Modify: `README.md`
- Modify: `docs/features/vector-queries-guide.md`

- [ ] **Step 1: Replace `DatabasePath`**

Change all `DataVoConfig` samples from:

```csharp
DatabasePath = "./embeddings.db"
```

to:

```csharp
DiskStoragePath = "./embeddings.db"
```

- [ ] **Step 2: Replace `ExecuteWithParams` direct-context samples**

Use actual `DataVo.Data` APIs:

```csharp
using DataVo.Data;

using var connection = new DataVoConnection("StorageMode=Disk;DataSource=./embeddings.db");
connection.Open();

using var command = connection.CreateCommand();
command.CommandText = "INSERT INTO Items VALUES (@id, @name, @vec)";
command.Parameters.AddWithValue("@id", 1);
command.Parameters.AddWithValue("@name", "Widget");
command.Parameters.AddWithValue("@vec", "[0.1, 0.2, 0.3]");
command.ExecuteNonQuery();
```

For vector arrays, be explicit that current ADO.NET parameter formatting serializes non-scalar values as SQL string literals, so examples should pass vector literals until typed vector binding lands.

- [ ] **Step 3: Run guard to verify docs symbols are gone**

Run: `bash scripts/check-doc-api-samples.sh`

Expected: PASS.

## Task 3: Align Product Status and Metadata

**Files:**
- Modify: `README.md`
- Modify: `docs/features/roadmap-and-integrations.md`
- Modify: `Directory.Build.props`

- [ ] **Step 1: Update status language**

Use this exact README status text:

```markdown
DataVo is preview software aimed at local-first and embeddable database scenarios.

- Local package distribution is available now.
- Browser/WebAssembly runtime support is available now.
- Public NuGet and npm publication are in deployment preparation.
- Production-hardening work is active; validate representative workloads before production adoption.
```

- [ ] **Step 2: Update package URLs**

Run: `git remote -v`.

If the remote points to this DataVo repo, use that URL in `PackageProjectUrl` and `RepositoryUrl`.

If no trustworthy DataVo remote exists, remove the misleading old repository values and use:

```xml
<PackageProjectUrl>https://github.com/ArintonAkos/DataVo-DBMS</PackageProjectUrl>
<RepositoryUrl>https://github.com/ArintonAkos/DataVo-DBMS.git</RepositoryUrl>
```

- [ ] **Step 3: Verify metadata no longer references old repository values**

Run a repository-wide search for stale old repository metadata across `Directory.Build.props`, `README.md`, and `docs`.

Expected: no matches.

## Task 4: Replace Generated-Looking DQL Docs

**Files:**
- Modify: `docs/DataVo.Core/Parser/DQL/index.md`

- [ ] **Step 1: Replace component breakdown**

Use concise text:

```markdown
| Component (File) | Architectural Role |
| :-- | :-- |
| `Select.cs` and partials | Coordinates SELECT execution: source resolution, planner selection, legacy/Volcano execution, grouping, HAVING, window values, projection, ordering, and DISTINCT. |
| `Select.Planner.cs` | Chooses between legacy and Volcano execution paths using feature support and cost heuristics. |
| `Select.FastPathDecisions.cs` | Detects vector nearest-neighbor and predicate fast paths that can use HNSW indexes. |
```

- [ ] **Step 2: Run docs guard and tests**

Run: `bash scripts/check-doc-api-samples.sh`

Expected: PASS.

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore`

Expected: 0 failed.
