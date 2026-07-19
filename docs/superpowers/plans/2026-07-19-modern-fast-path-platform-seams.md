# Modern Fast Paths and Platform Seams Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore the modern `.NET 10` behavior lost in the compatibility commit, make the experimental `netstandard2.1` behavior explicit behind whole-file seams, and leave the common engine code readable without claiming portable durable-storage support.

**Architecture:** Common algorithms call small DataVo-owned seams whose modern and portable implementations are selected by complete target files. Modern files use the same BCL/intrinsic APIs as baseline commit `2fb977d`; portable files preserve in-memory correctness and fail explicitly when they reach unvalidated durable I/O. DataVo-owned queue and task helpers remain in `DataVo.Core.Compat`, never under `System.*`. HNSW structure and vector distance/norm kernels are deliberately deferred to Stage 2C.

**Tech Stack:** C#/.NET 10, .NET Standard 2.1, xUnit, NuGet/MSBuild package validation, Bash, `GC.GetAllocatedBytesForCurrentThread`, opt-in stopwatch probes.

## Global Constraints

- Compare modern behavior with `2fb977d`, the direct pre-compatibility baseline. The broad regression entered in `5065987`.
- Keep `net10.0` as the normal target. `netstandard2.1` remains opt-in through `DataVoEnablePortableTarget=true` and must not enter normal packages or publish workflows.
- Keep portable `StorageMode.Disk` and `StorageMode.Lsm` rejected before backend construction. Portable storage helpers must throw `PlatformNotSupportedException` if reached; do not retain weaker simulated durability.
- Put each target selection at one complete-file boundary. Common algorithm and storage orchestration files must not contain interleaved target checks.
- Do not declare DataVo compatibility collections or task extensions in BCL namespaces. Internal compiler attributes under `System.Runtime.CompilerServices` are the only permitted portable compiler shims.
- Preserve public APIs, persisted password hashes, WAL bytes, CRC32C output, scalar-key strings, query results, and modern durable-publication behavior.
- Do not restructure HNSW, change HNSW defaults/diagnostics, or modify `SimdDistanceKernels` and vector norm routing in this plan. Those remain Stage 2C.
- Do not add Unity projects, UPM packaging, Burst/jobs bridges, or publish the portable asset.
- Timing evidence is informational and must run in isolated Release processes. Do not put wall-clock thresholds in xUnit.
- Preserve unrelated user-owned untracked files. Use `apply_patch` for edits and stage exact paths rather than `git add .`.
- Baseline for this plan is commit `f43632c`.

---

### Task 1: Lock the modern regression contracts with focused tests

**Files:**

- Modify: `DataVo.Tests/Transactions/WalAppenderTests.cs`
- Modify: `DataVo.Tests/Storage/IndexKeyEncoderTypedTests.cs`
- Create: `DataVo.Tests/Performance/ScalarKeyFormattingAllocationTests.cs`
- Modify: `DataVo.Tests/Reactive/CellValueGuidTests.cs`
- Create: `DataVo.Tests/Compat/ThrowHelperTests.cs`
- Modify: `DataVo.Tests/Lsm/ArenaTests.cs`

- [ ] Add CRC32C parity cases for lengths `0, 1, 2, 3, 4, 7, 8, 9, 31, 32, 33`, plus a seeded large payload. Compute expected values with a test-local Castagnoli reference loop so the production implementation is not its own oracle.
- [ ] Add scalar-key culture/parity tests for `int`, `long`, `double`, `float`, and `decimal`. Pin the intentional providers: index-key floating values use current culture; compiled comparison keys are invariant. Restore the culture in `finally`.
- [ ] Add warmed allocation tests that keep outputs live and distinguish the output string allocation from numeric boxing. Cover direct typed overloads; do not use the object overload to judge unavoidable caller boxing.
- [ ] Add deterministic and seeded GUID round trips, then measure 10,000 warmed `CellValue.AsGuid()` calls and require exactly zero bytes allocated.
- [ ] Assert guard caller names, null-versus-empty/whitespace exception types, negative actual values, and disposed `ObjectName == typeof(Arena).FullName`.
- [ ] Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj -c Release \
  --filter "FullyQualifiedName~WalAppenderTests|FullyQualifiedName~IndexKeyEncoderTypedTests|FullyQualifiedName~ScalarKeyFormattingAllocationTests|FullyQualifiedName~CellValueGuidTests|FullyQualifiedName~ThrowHelperTests|FullyQualifiedName~ArenaTests"
```

Expected RED at `f43632c`: scalar boxing/allocation, GUID allocation, missing guard caller names, incorrect null/whitespace distinction, and short disposed object names. CRC correctness remains green and characterizes the implementation before its performance restoration.

- [ ] Commit only the regression tests:

```bash
git add \
  DataVo.Tests/Transactions/WalAppenderTests.cs \
  DataVo.Tests/Storage/IndexKeyEncoderTypedTests.cs \
  DataVo.Tests/Performance/ScalarKeyFormattingAllocationTests.cs \
  DataVo.Tests/Reactive/CellValueGuidTests.cs \
  DataVo.Tests/Compat/ThrowHelperTests.cs \
  DataVo.Tests/Lsm/ArenaTests.cs
git commit -m "test: lock modern compatibility regressions"
```

---

### Task 2: Restore CRC32C, scalar formatting, and decimal extraction

**Files:**

- Modify: `DataVo.Core/Transactions/WalAppender.cs`
- Create: `DataVo.Core/Transactions/WalCrc32C.Modern.cs`
- Create: `DataVo.Core/Transactions/WalCrc32C.Portable.cs`
- Create: `DataVo.Core/Compat/ScalarKeyFormatter.Modern.cs`
- Create: `DataVo.Core/Compat/ScalarKeyFormatter.Portable.cs`
- Modify: `DataVo.Core/BTree/IndexKeyEncoder.cs`
- Modify: `DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs`
- Modify: `DataVo.Core/Runtime/Reactive/CellValue.cs`
- Create: `DataVo.Core/Runtime/Reactive/CellValue.DecimalBits.Modern.cs`
- Create: `DataVo.Core/Runtime/Reactive/CellValue.DecimalBits.Portable.cs`

- [ ] Move `WalCrc32C` out of `WalAppender.cs`. The modern file restores the baseline 8/4/2/1-byte `BitOperations.Crc32C` implementation; the portable file owns the current polynomial loop. Keep checksum output byte-identical.
- [ ] Add typed `ScalarKeyFormatter.FormatBracketed` overloads. Modern implementations use `string.Create(provider, $"[{value}]")`; portable implementations use `string.Format`. Route both key builders through the seam without changing their culture rules.
- [ ] Move `Int64BitsFromDecimal` to complete `CellValue` partial files. Modern uses `stackalloc int[4]` and `decimal.GetBits(value, span)`; portable retains the array overload.
- [ ] Run the Task 1 tests and both target builds:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj -c Release \
  --filter "FullyQualifiedName~WalAppenderTests|FullyQualifiedName~IndexKeyEncoderTypedTests|FullyQualifiedName~ScalarKeyFormattingAllocationTests|FullyQualifiedName~CellValueGuidTests"
dotnet build DataVo.Core/DataVo.Core.csproj -c Release -f net10.0
dotnet build DataVo.Core/DataVo.Core.csproj -c Release -f netstandard2.1 \
  -p:DataVoEnablePortableTarget=true
```

- [ ] Commit:

```bash
git add \
  DataVo.Core/Transactions/WalAppender.cs \
  DataVo.Core/Transactions/WalCrc32C.Modern.cs \
  DataVo.Core/Transactions/WalCrc32C.Portable.cs \
  DataVo.Core/Compat/ScalarKeyFormatter.Modern.cs \
  DataVo.Core/Compat/ScalarKeyFormatter.Portable.cs \
  DataVo.Core/BTree/IndexKeyEncoder.cs \
  DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs \
  DataVo.Core/Runtime/Reactive/CellValue.cs \
  DataVo.Core/Runtime/Reactive/CellValue.DecimalBits.Modern.cs \
  DataVo.Core/Runtime/Reactive/CellValue.DecimalBits.Portable.cs
git commit -m "perf: restore modern scalar and WAL hot paths"
```

---

### Task 3: Replace guard, cancellation, clock, queue, and lock shims

**Files:**

- Delete: `DataVo.Core/Compat/ThrowHelper.cs`
- Create: `DataVo.Core/Compat/ThrowHelper.Modern.cs`
- Create: `DataVo.Core/Compat/ThrowHelper.Portable.cs`
- Modify: `DataVo.Core/Compat/NetStandard21Polyfills.cs`
- Delete: `DataVo.Core/Compat/TaskCompatExtensions.cs`
- Create: `DataVo.Core/Compat/TaskCancellationWaiter.Modern.cs`
- Create: `DataVo.Core/Compat/TaskCancellationWaiter.Portable.cs`
- Delete: `DataVo.Core/Compat/EnvironmentCompat.cs`
- Create: `DataVo.Core/Compat/MonotonicClock.Modern.cs`
- Create: `DataVo.Core/Compat/MonotonicClock.Portable.cs`
- Delete: `DataVo.Core/Compat/PriorityQueueCompat.cs`
- Create: `DataVo.Core/Compat/DataVoPriorityQueue.Modern.cs`
- Create: `DataVo.Core/Compat/DataVoPriorityQueue.Portable.cs`
- Modify: `DataVo.Core/Transactions/GroupCommitter.cs`
- Modify: `DataVo.Core/Parser/DQL/Select.cs`
- Modify: `DataVo.Core/Indexing/HNSW/HNSWIndex.cs` (type-name substitution only)
- Modify: `DataVo.Core/Execution/Volcano/SortOperator.cs`
- Modify: `DataVo.Core/StorageEngine/Lsm/LsmCompactor.cs`
- Modify: `DataVo.Core/Indexing/IndexManager.cs`
- Create: `DataVo.Core/Indexing/IndexManager.Lock.Modern.cs`
- Create: `DataVo.Core/Indexing/IndexManager.Lock.Portable.cs`
- Modify: `DataVo.Core/StorageEngine/Lsm/LsmLatestRowStore.cs`
- Create: `DataVo.Tests/Compat/TaskCancellationWaiterTests.cs`
- Create: `DataVo.Tests/Compat/DataVoPriorityQueueTests.cs`

- [ ] Modern guard wrappers forward to BCL guards and use `[CallerArgumentExpression]`; portable wrappers reproduce the same exception types, parameter names, actual values, and full disposed type name. Add only an internal `CallerArgumentExpressionAttribute` compiler shim for `netstandard2.1`.
- [ ] Replace the `System.Threading.Tasks` extension shim with a DataVo-owned waiter. Modern calls `Task.WaitAsync`; portable uses `Task.WhenAny` and cancellation registration. Route `GroupCommitter` through it and remove the method-level target branch.
- [ ] Replace `EnvironmentCompat` with a monotonic-millisecond seam. Modern uses `Environment.TickCount64`; portable uses `Stopwatch.GetTimestamp()` conversion. Update query telemetry and file-handle LRU callers.
- [ ] Replace the BCL-namespace priority queue with `DataVoPriorityQueue<TElement, TPriority>` in `DataVo.Core.Compat`. Modern delegates to the BCL queue; portable owns the heap. Change only queue type names in HNSW, sort, and LSM code.
- [ ] Make `IndexManager` partial and put its `_lock` field in target files: `System.Threading.Lock` on modern .NET and `object` on portable.
- [ ] Route the existing negative row-id validation through the caller-preserving guard.
- [ ] Run guard/queue/cancellation tests, `GroupCommitterTests`, sort/LSM tests, and both builds.
- [ ] Commit:

```bash
git add DataVo.Core/Compat DataVo.Core/Transactions/GroupCommitter.cs \
  DataVo.Core/Parser/DQL/Select.cs \
  DataVo.Core/Indexing/HNSW/HNSWIndex.cs \
  DataVo.Core/Execution/Volcano/SortOperator.cs \
  DataVo.Core/StorageEngine/Lsm/LsmCompactor.cs \
  DataVo.Core/Indexing/IndexManager.cs \
  DataVo.Core/Indexing/IndexManager.Lock.Modern.cs \
  DataVo.Core/Indexing/IndexManager.Lock.Portable.cs \
  DataVo.Core/StorageEngine/Lsm/LsmLatestRowStore.cs \
  DataVo.Tests/Compat/ThrowHelperTests.cs \
  DataVo.Tests/Compat/TaskCancellationWaiterTests.cs \
  DataVo.Tests/Compat/DataVoPriorityQueueTests.cs \
  DataVo.Tests/Lsm/ArenaTests.cs
git commit -m "refactor: isolate core runtime compatibility seams"
```

---

### Task 4: Make portable durable I/O explicitly unsupported

**Files:**

- Delete: `DataVo.Core/Compat/RandomAccessCompat.cs`
- Create: `DataVo.Core/Compat/RandomAccessCompat.Modern.cs`
- Create: `DataVo.Core/Compat/RandomAccessCompat.Portable.cs`
- Delete: `DataVo.Core/Compat/FileCompat.cs`
- Create: `DataVo.Core/Compat/FileCompat.Modern.cs`
- Create: `DataVo.Core/Compat/FileCompat.Portable.cs`
- Delete: `DataVo.Core/StorageEngine/Disk/FileHandlePool.cs`
- Create: `DataVo.Core/StorageEngine/Disk/FileHandlePool.Modern.cs`
- Create: `DataVo.Core/StorageEngine/Disk/FileHandlePool.Portable.cs`
- Delete: `DataVo.Core/StorageEngine/Lsm/LsmDurableFileOperations.cs`
- Create: `DataVo.Core/StorageEngine/Lsm/LsmDurableFileOperations.Modern.cs`
- Create: `DataVo.Core/StorageEngine/Lsm/LsmDurableFileOperations.Portable.cs`
- Modify: `DataVo.Tests/StorageEngine/FileHandlePoolTests.cs`
- Modify: `tools/DataVo.PortablePackageSmoke/Program.cs`

- [ ] Put direct `RandomAccess` calls and the current modern handle pool in modern files with no nested target checks.
- [ ] Give the portable random-access facade and portable handle pool the signatures required by common code, but throw `PlatformNotSupportedException` before any handle or durability operation. This is an internal second fence behind `PortableStoragePolicy`, not a simulated file system.
- [ ] Restore the modern LSM publication implementation as one complete file using file fsync, same-directory rename, and directory fsync. The portable file must throw before invoking the write callback or creating a temp file.
- [ ] Split `FileCompat` into complete files. Its portable overwrite helper may support non-durable utility paths, but LSM and random-access persistence must never reach it on the portable target.
- [ ] Extend the exact-package portable smoke (using reflection only for these internal second-fence probes) so unsupported random access, handle acquisition, and LSM publication reject before directories, temp files, callbacks, or handles are created. Keep the modern file-handle and crash-point suites green.
- [ ] Run:

```bash
dotnet test DataVo.Tests/DataVo.Tests.csproj -c Release \
  --filter "FullyQualifiedName~FileHandlePoolTests|FullyQualifiedName~DiskStorageEngine|FullyQualifiedName~LsmTableTests"
dotnet build DataVo.Core/DataVo.Core.csproj -c Release -f netstandard2.1 \
  -p:DataVoEnablePortableTarget=true
bash scripts/verify-portable-candidate.sh
```

- [ ] Commit: `refactor: fence portable durable storage internals`.

---

### Task 5: Isolate password hashing and the remaining non-vector library paths

**Files:**

- Modify: `DataVo.Core/Runtime/Security/PasswordHasher.cs`
- Create: `DataVo.Core/Runtime/Security/PasswordHashPlatform.Modern.cs`
- Create: `DataVo.Core/Runtime/Security/PasswordHashPlatform.Portable.cs`
- Modify: `DataVo.Core/Runtime/Security/SessionSecurityStore.cs`
- Create: `DataVo.Core/Runtime/Security/SessionSecurityStore.Modern.cs`
- Create: `DataVo.Core/Runtime/Security/SessionSecurityStore.Portable.cs`
- Modify: `DataVo.Core/Runtime/Reactive/AggregateReactiveQuery.cs`
- Create: `DataVo.Core/Runtime/Reactive/AggregateReactiveQuery.Modern.cs`
- Create: `DataVo.Core/Runtime/Reactive/AggregateReactiveQuery.Portable.cs`
- Modify: `DataVo.Core/CompiledQueries/CompiledIntegerScan.cs`
- Create: `DataVo.Core/CompiledQueries/CompiledIntegerScan.Modern.cs`
- Create: `DataVo.Core/CompiledQueries/CompiledIntegerScan.Portable.cs`
- Delete: `DataVo.Core/Indexing/GuidSimdEqualityComparer.cs`
- Create: `DataVo.Core/Indexing/GuidSimdEqualityComparer.Modern.cs`
- Create: `DataVo.Core/Indexing/GuidSimdEqualityComparer.Portable.cs`
- Modify: `DataVo.Core/Parser/DML/InsertRowService.cs`
- Create: `DataVo.Core/Parser/DML/InsertRowService.Modern.cs`
- Create: `DataVo.Core/Parser/DML/InsertRowService.Portable.cs`
- Create: `DataVo.Tests/Security/PasswordHasherTests.cs`
- Modify: `DataVo.Tests/E2E/AuthorizationTests.cs`
- Modify: `DataVo.Tests/Reactive/AggregateBorrowedTests.cs`
- Modify: `DataVo.Tests/E2E/CompiledAccessPathTests.cs`
- Modify: `DataVo.Tests/BTree/IndexManagerGuidFastLaneTests.cs`
- Modify: `DataVo.Tests/E2E/InsertTypedTests.cs`

- [ ] Put random-byte generation and PBKDF2 derivation behind complete modern/portable files. Modern restores one-shot `RandomNumberGenerator.GetBytes` and `Rfc2898DeriveBytes.Pbkdf2`; portable uses instance APIs. Preserve iteration count, SHA-256, salt/hash sizes, Base64 representation, and fixed-time comparison.
- [ ] Move generic `Enum.GetValues<T>`, alternate dictionary lookup/span formatting, compiled integer intrinsics, GUID intrinsics, and `CollectionsMarshal.AsSpan` to target partial files. Portable implementations retain the scalar/allocation-tolerant fallback.
- [ ] Keep the common query, reactive, security, and insert algorithms free of target directives.
- [ ] Do not move or edit HNSW algorithms, `SimdDistanceKernels`, or vector norm routing in this task.
- [ ] Run focused suites and both target builds.
- [ ] Commit: `refactor: isolate modern library fast paths`.

---

### Task 6: Add repeatable layout and hot-path evidence

**Files:**

- Create: `DataVo.Tests/Performance/Stage2BHotPathProbeTests.cs`
- Create: `scripts/verify-stage2b-platform-seams.sh`
- Modify: `scripts/verify-portable-candidate.sh` only if the exact-package smoke needs a new semantic assertion
- Create (ignored evidence): `.superpowers/sdd/stage2b-hotpath-evidence.md`

- [ ] Add an opt-in Release probe controlled by `DATAVO_RUN_STAGE2B_PROBE=1`. Measure CRC32C, typed scalar-key formatting, and `CellValue.AsGuid()` in fresh processes; print checksum, iterations, allocated bytes/op, median time, and spread. Assert results and allocation contracts, never elapsed time.
- [ ] Make the layout verifier fail when target directives reappear inside the common files changed by this plan, when a DataVo collection/task shim appears under a BCL namespace, or when the normal Core package gains a portable asset. Explicitly allow the complete target files and the deferred HNSW/distance/norm files.
- [ ] Run the same probe from isolated checkouts of `2fb977d`, `f43632c`, and the candidate by copying only the probe test into each checkout. Record SDK, OS, architecture, commit, and five-process summaries in the ignored evidence file.
- [ ] Acceptance: candidate CRC behavior matches both revisions and restores the baseline order of throughput; typed scalar formatting no longer pays the compatibility boxing allocation; GUID reads are `0 B/op`, matching `2fb977d`; any remaining timing difference is explained before completion.
- [ ] Commit the probe and verifier: `test: verify modern platform seams`.

---

### Task 7: Run the Stage 2B verification matrix and final review

- [ ] Run focused tests for every task, then:

```bash
dotnet build DataVo.Core/DataVo.Core.csproj -c Release -f net10.0
dotnet build DataVo.Core/DataVo.Core.csproj -c Release -f netstandard2.1 \
  -p:DataVoEnablePortableTarget=true
dotnet test DataVo.Tests/DataVo.Tests.csproj -c Release
dotnet test DataVo.Generators.Tests/DataVo.Generators.Tests.csproj -c Release
dotnet test demos/Research.Benchmark/tests/Research.Benchmark.Tests/Research.Benchmark.Tests.csproj -c Release
bash scripts/verify-stage2b-platform-seams.sh
bash scripts/verify-portable-candidate.sh
bash scripts/verify-local-packages.sh
git diff --check f43632c..HEAD
```

- [ ] Confirm `rg -l '#if|#else|#endif' DataVo.Core` contains only complete target files, internal compiler polyfills, and the explicitly deferred HNSW/vector files.
- [ ] Confirm the exact candidate package still exposes no public `System.*` compatibility types, uses only in-memory mode in its smoke, and remains absent from normal package/publish output.
- [ ] Request a whole-range code review against this plan. Fix every Critical or Important finding, rerun affected tests, and request a final clean review.
- [ ] Update `.superpowers/sdd/progress.md` with commits, test counts, benchmark evidence, known warnings, and the Stage 2C handoff.

## Exit Criteria

- Modern CRC32C, scalar formatting, GUID reads, guards, task waiting, locking, random access, hashing, and non-vector intrinsic paths use the modern APIs available at `2fb977d`.
- Portable in-memory behavior remains correct; portable durable storage is rejected at both policy and internal I/O fences.
- Common files changed by this plan have no interleaved target branches.
- No DataVo queue or task compatibility type is declared under a BCL namespace.
- Modern GUID reads allocate zero bytes; scalar boxing is removed; the CRC probe shows no unexplained regression from `2fb977d`.
- Full modern tests, both Core target builds, exact candidate verification, normal package verification, and final code review pass.
- HNSW restructuring/defaults/diagnostics and vector distance/norm kernels remain explicitly queued for Stage 2C.
