# LSM Engine — Plan 1: RowId Audit + Allocation-Free Leaf Primitives

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Establish the RowId migration blast-radius audit, then build the three isolated, allocation-free primitives the LSM is composed from — `InternalKey`, `Arena`, and `BloomFilter` — fully tested in isolation.

**Architecture:** This is Plan 1 of a 5-plan sequence implementing the approved LSM design (`docs/superpowers/specs/2026-06-29-lsm-storage-engine-design.md`). It is leaf-to-root: these three primitives have no dependencies on each other or on the rest of the engine, so they are built and tested first. Later plans (MemTable → SSTable → Manifest → Engine/Compactor) consume them through the exact signatures declared in each task's **Produces** block.

**Tech Stack:** C# / .NET 10, xUnit 2.9.3, `System.Buffers` (`ArrayPool<T>`), `System.Buffers.Binary` (`BinaryPrimitives`). No new NuGet packages.

## Global Constraints

- **Target framework:** `net10.0` (matches `DataVo.Core` and `DataVo.Tests`).
- **Native AOT clean:** `DataVo.Core` is locked AOT-clean. New code MUST avoid reflection, `System.Reflection.Emit`, dynamic codegen, and runtime serializers. Use only AOT-safe primitives (spans, `BinaryPrimitives`, `ArrayPool`).
- **No new NuGet dependencies.** The Bloom hash is FNV-1a implemented inline; do NOT add `System.IO.Hashing`.
- **Production namespace/folder:** `DataVo.Core/StorageEngine/Lsm/` → namespace `DataVo.Core.StorageEngine.Lsm`.
- **Test namespace/folder:** `DataVo.Tests/Lsm/` → namespace `DataVo.Tests.Lsm`. Test framework is xUnit (`[Fact]`/`[Theory]`).
- **Allocation assertions** use the established pattern: warm the call ~200×, then measure `GC.GetAllocatedBytesForCurrentThread()` before/after an N-iteration loop and assert `perCall <= budget`.
- **Commit message trailer:** every commit body ends with `Claude-Session: https://claude.ai/code/session_01P9EXib7WpNKu16aZKiveoS`.
- **Branch:** `fix/concurrent-ops-alloc-and-locking` (do NOT push; commit locally only).
- **Build command:** `dotnet build DataVo.Core/DataVo.Core.csproj -c Debug`. **Test command:** `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "<filter>"`.

---

## Plan sequence (the whole shape)

| Plan | Subsystem | Spec §s | Independently testable deliverable |
|---|---|---|---|
| **1 (this)** | RowId audit + leaf primitives | §10, §14, §11 (InternalKey/Arena/BloomFilter) | Audit doc + 3 tested primitives |
| 2 | MemTable | §4, §11 | Arena-backed single-writer sorted skiplist with Put/Delete/Get/iterate |
| 3 | SSTable writer/reader | §6, §7, §11 | Flush a MemTable to an immutable `.sst` + point-get/iterate it back |
| 4 | Manifest + Versions | §6, §8, §9 | `VersionEdit` log, CURRENT pointer, refcounted Versions, crash-atomic install |
| 5 | `LsmStorageEngine` + Compactor + Recovery + secondary indexes | §3, §5, §8, §9, §10 | Config-selectable `IStorageEngine`; A/B benchmark vs B-tree engine |

Plan 1 covers spec §11's leaf units and the §10/§14 RowId-ripple audit. Plans 2–5 (their own documents, written when we reach them) cover the remainder.

---

## File structure (Plan 1)

- `docs/superpowers/specs/2026-06-29-rowid-ripple-audit.md` — the audit deliverable (Task 0).
- `DataVo.Core/StorageEngine/Lsm/InternalKey.cs` — order-preserving `userKey ‖ tag` codec + comparator (Task 1).
- `DataVo.Core/StorageEngine/Lsm/Arena.cs` — pooled single-writer bump allocator (Task 2).
- `DataVo.Core/StorageEngine/Lsm/BloomFilter.cs` — allocation-free double-hash Bloom filter (Task 3).
- `DataVo.Tests/Lsm/InternalKeyTests.cs`, `ArenaTests.cs`, `BloomFilterTests.cs` — the tests.

---

### Task 0: RowId-ripple audit

**Files:**
- Create: `docs/superpowers/specs/2026-06-29-rowid-ripple-audit.md`

**Interfaces:**
- Consumes: nothing.
- Produces: a written enumeration consumed by Plan 5 (the engine integration) to size the migration. No code symbols.

This task is investigative — its deliverable is a complete, categorized list of every place that assumes `RowId == physical byte offset`, so Plan 5 changes them deliberately. There is no production code; "the test" is grep-completeness (every hit is accounted for in the doc).

- [ ] **Step 1: Enumerate the raw call sites**

Run each command and keep the output:

```bash
# Storage seam: methods that take/return the byte-offset RowId
grep -rn "long rowId\|long RowId\|byte-offset\|rawByteOffset" DataVo.Core --include=*.cs
# Index → offset mappings (these become value->PK in the LSM)
grep -rn "InsertIntegerPrimaryKeys\|TryLookupIntegerPrimaryKey\|RemoveIntegerPrimaryKey\|FilterUsingIndex\|_integerPrimaryKeyMaps" DataVo.Core --include=*.cs
# MVCC + reactive consumers of RowId
grep -rn "RegisterUpdateVersion\|ValidateCanModifyRow\|RowVersion\|VersionStorageManager" DataVo.Core --include=*.cs
# Storage engine surface that exposes offsets
grep -rn "ReadRow\|ReadAllRows\|DeleteRow\|InsertRow\|InsertSerializedRow\|GetTableContents\|CompactTable" DataVo.Core --include=*.cs
# Compiled-query consumers (the zero-alloc paths)
grep -rn "rowId\|RowId" DataVo.Core/CompiledQueries --include=*.cs
```

- [ ] **Step 2: Write the audit document**

Create `docs/superpowers/specs/2026-06-29-rowid-ripple-audit.md` with these sections, each row recording `file:line — current meaning of RowId — LSM migration note`:

```markdown
# RowId Ripple Audit (LSM migration blast radius)

**Date:** 2026-06-29
**Purpose:** Enumerate every site that assumes `RowId == physical byte offset`, to scope the
`IStorageEngine` → `LsmStorageEngine` migration. Source of truth for Plan 5.

## 1. Storage-engine seam (IStorageEngine)
| Site | Today | LSM migration |
|------|-------|---------------|
| `IStorageEngine.ReadRow(db,table,rowId)` | seek to byte offset | resolve by PK through MemTable+SSTables |
| ... | ... | ... |

## 2. Index → location mapping
(value→offset becomes value→PK; integer fast lane becomes a PK presence/seqno map)

## 3. MVCC & reactive change capture
(RowVersion unifies into the LSM seqno; RegisterUpdateVersion semantics)

## 4. Compiled query fast paths
(DataVoCompiledQuery update/select: how they obtain and use rowId)

## 5. Snapshot / restore / catalog rebuild
(RebuildAllIndexesFromCatalog, GetTableContents, CompactTable callers)

## 6. Verdict
- Sites safe behind the IStorageEngine seam: <list>
- Sites that leak the offset above the seam (require change): <list>
- Recommended migration order for Plan 5.
```

Fill every table row from Step 1's grep output — no "etc.", no "and others". Each grep hit lands in exactly one section.

- [ ] **Step 3: Verify completeness**

Re-run the Step 1 greps; confirm every file:line appears somewhere in the document. Spot-check 3 of the "safe behind the seam" claims by opening the cited lines and confirming the `RowId` never escapes an `IStorageEngine` call.

- [ ] **Step 4: Commit**

```bash
git add -f docs/superpowers/specs/2026-06-29-rowid-ripple-audit.md
git commit -m "docs: RowId-ripple audit for LSM migration blast radius

Claude-Session: https://claude.ai/code/session_01P9EXib7WpNKu16aZKiveoS"
```

---

### Task 1: `InternalKey` — order-preserving key codec + comparator

**Files:**
- Create: `DataVo.Core/StorageEngine/Lsm/InternalKey.cs`
- Test: `DataVo.Tests/Lsm/InternalKeyTests.cs`

**Interfaces:**
- Consumes: `System.Buffers.Binary.BinaryPrimitives`.
- Produces (consumed by Plans 2–5):
  - `enum LsmValueType : byte { Deletion = 0, Put = 1 }`
  - `static class InternalKey` with:
    - `const int TagSize = 8`
    - `const ulong MaxSequenceNumber = (1UL << 56) - 1`
    - `int MeasureSize(int userKeyLength)`
    - `int Write(Span<byte> dest, ReadOnlySpan<byte> userKey, ulong seqno, LsmValueType valueType)`
    - `ReadOnlySpan<byte> UserKey(ReadOnlySpan<byte> internalKey)`
    - `ulong Sequence(ReadOnlySpan<byte> internalKey)`
    - `LsmValueType ValueType(ReadOnlySpan<byte> internalKey)`
    - `int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)`
    - `int EncodeInt64UserKey(Span<byte> dest, long primaryKey)`

- [ ] **Step 1: Write the failing tests**

Create `DataVo.Tests/Lsm/InternalKeyTests.cs`:

```csharp
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

public class InternalKeyTests
{
    private static byte[] Build(long pk, ulong seqno, LsmValueType type)
    {
        Span<byte> user = stackalloc byte[8];
        InternalKey.EncodeInt64UserKey(user, pk);
        var dest = new byte[InternalKey.MeasureSize(user.Length)];
        InternalKey.Write(dest, user, seqno, type);
        return dest;
    }

    [Fact]
    public void Write_Then_Extract_RoundTripsTagFields()
    {
        byte[] key = Build(pk: 42, seqno: 7, LsmValueType.Put);

        Assert.Equal(7UL, InternalKey.Sequence(key));
        Assert.Equal(LsmValueType.Put, InternalKey.ValueType(key));
        Assert.Equal(8, InternalKey.UserKey(key).Length);
    }

    [Fact]
    public void Compare_SameUserKey_HigherSeqnoSortsFirst()
    {
        byte[] newer = Build(pk: 42, seqno: 9, LsmValueType.Put);
        byte[] older = Build(pk: 42, seqno: 4, LsmValueType.Put);

        Assert.True(InternalKey.Compare(newer, older) < 0);
        Assert.True(InternalKey.Compare(older, newer) > 0);
    }

    [Fact]
    public void Compare_DifferentUserKeys_SortsAscendingRegardlessOfSeqno()
    {
        byte[] lowKeyOldSeq = Build(pk: 1, seqno: 1, LsmValueType.Put);
        byte[] highKeyNewSeq = Build(pk: 2, seqno: 999, LsmValueType.Put);

        Assert.True(InternalKey.Compare(lowKeyOldSeq, highKeyNewSeq) < 0);
    }

    [Fact]
    public void EncodeInt64UserKey_IsSignCorrect()
    {
        Span<byte> neg = stackalloc byte[8];
        Span<byte> zero = stackalloc byte[8];
        Span<byte> pos = stackalloc byte[8];
        InternalKey.EncodeInt64UserKey(neg, -5);
        InternalKey.EncodeInt64UserKey(zero, 0);
        InternalKey.EncodeInt64UserKey(pos, 1);

        Assert.True(neg.SequenceCompareTo(zero) < 0);
        Assert.True(zero.SequenceCompareTo(pos) < 0);
    }

    [Fact]
    public void Compare_SameUserKeyAndSeqno_PutSortsBeforeDeletion()
    {
        byte[] put = Build(pk: 42, seqno: 5, LsmValueType.Put);
        byte[] del = Build(pk: 42, seqno: 5, LsmValueType.Deletion);

        Assert.True(InternalKey.Compare(put, del) < 0);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~InternalKeyTests"`
Expected: FAIL to compile — `InternalKey` / `LsmValueType` do not exist.

- [ ] **Step 3: Write the implementation**

Create `DataVo.Core/StorageEngine/Lsm/InternalKey.cs`:

```csharp
using System.Buffers.Binary;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>The kind of a versioned LSM entry. Packed into the low byte of the InternalKey tag.</summary>
public enum LsmValueType : byte
{
    /// <summary>A deletion marker (tombstone) — an entry that carries no value.</summary>
    Deletion = 0,

    /// <summary>A live value: a full row image from an insert or update.</summary>
    Put = 1,
}

/// <summary>
/// Order-preserving encoding and comparison for LSM internal keys. An internal key is
/// <c>userKey ‖ tag</c>, where <c>tag</c> is the fixed 8-byte big-endian trailer
/// <c>(seqno &lt;&lt; 8) | (byte)valueType</c>. Internal keys sort by user key ascending, then by tag
/// descending, so the newest version (highest seqno) of a user key sorts first.
/// </summary>
public static class InternalKey
{
    /// <summary>Size, in bytes, of the packed tag trailer appended to every user key.</summary>
    public const int TagSize = 8;

    /// <summary>Maximum sequence number representable in the 56-bit seqno field of the tag.</summary>
    public const ulong MaxSequenceNumber = (1UL << 56) - 1;

    /// <summary>Bytes an internal key occupies for a user key of the given length.</summary>
    public static int MeasureSize(int userKeyLength) => userKeyLength + TagSize;

    /// <summary>
    /// Writes <paramref name="userKey"/> followed by the packed tag into <paramref name="dest"/> and
    /// returns the number of bytes written (<c>userKey.Length + <see cref="TagSize"/></c>).
    /// </summary>
    public static int Write(Span<byte> dest, ReadOnlySpan<byte> userKey, ulong seqno, LsmValueType valueType)
    {
        if (seqno > MaxSequenceNumber)
        {
            throw new ArgumentOutOfRangeException(nameof(seqno));
        }

        userKey.CopyTo(dest);
        ulong tag = (seqno << 8) | (byte)valueType;
        BinaryPrimitives.WriteUInt64BigEndian(dest.Slice(userKey.Length, TagSize), tag);
        return userKey.Length + TagSize;
    }

    /// <summary>Returns the user-key portion (everything except the trailing tag).</summary>
    public static ReadOnlySpan<byte> UserKey(ReadOnlySpan<byte> internalKey) => internalKey[..^TagSize];

    /// <summary>Returns the sequence number stored in the trailing tag.</summary>
    public static ulong Sequence(ReadOnlySpan<byte> internalKey) =>
        BinaryPrimitives.ReadUInt64BigEndian(internalKey[^TagSize..]) >> 8;

    /// <summary>Returns the value type stored in the trailing tag.</summary>
    public static LsmValueType ValueType(ReadOnlySpan<byte> internalKey) => (LsmValueType)internalKey[^1];

    /// <summary>Compares two internal keys: user key ascending, then tag descending (newest-first).</summary>
    public static int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)
    {
        int byUserKey = a[..^TagSize].SequenceCompareTo(b[..^TagSize]);
        if (byUserKey != 0)
        {
            return byUserKey;
        }

        // Tag descending: comparing b's tag to a's tag inverts the order. Big-endian byte order
        // equals numeric order, so a larger tag (newer) sorts first.
        return b[^TagSize..].SequenceCompareTo(a[^TagSize..]);
    }

    /// <summary>
    /// Encodes a signed 64-bit primary key into an order-preserving 8-byte big-endian user key
    /// (sign-flipped so negatives sort before positives), mirroring
    /// <see cref="DataVo.Core.BTree.IndexKeyEncoder"/>. Returns the number of bytes written (8).
    /// </summary>
    public static int EncodeInt64UserKey(Span<byte> dest, long primaryKey)
    {
        ulong flipped = unchecked((ulong)(primaryKey ^ long.MinValue));
        BinaryPrimitives.WriteUInt64BigEndian(dest[..8], flipped);
        return 8;
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~InternalKeyTests"`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add DataVo.Core/StorageEngine/Lsm/InternalKey.cs DataVo.Tests/Lsm/InternalKeyTests.cs
git commit -m "feat: LSM InternalKey order-preserving codec + comparator

Claude-Session: https://claude.ai/code/session_01P9EXib7WpNKu16aZKiveoS"
```

---

### Task 2: `Arena` — pooled single-writer bump allocator

**Files:**
- Create: `DataVo.Core/StorageEngine/Lsm/Arena.cs`
- Test: `DataVo.Tests/Lsm/ArenaTests.cs`

**Interfaces:**
- Consumes: `System.Buffers.ArrayPool<byte>`.
- Produces (consumed by Plan 2 — the MemTable):
  - `sealed class Arena : IDisposable`
    - `Arena(int slabSize = 1 << 20)`
    - `long BytesAllocated { get; }`
    - `Span<byte> Allocate(int size)` — span valid until next `Reset()`/`Dispose()`
    - `void Reset()`
    - `void Dispose()`

- [ ] **Step 1: Write the failing tests**

Create `DataVo.Tests/Lsm/ArenaTests.cs`:

```csharp
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

public class ArenaTests
{
    [Fact]
    public void Allocate_ReturnsRequestedSize_AndAccumulatesBytes()
    {
        using var arena = new Arena(slabSize: 1024);

        Span<byte> a = arena.Allocate(10);
        Span<byte> b = arena.Allocate(20);

        Assert.Equal(10, a.Length);
        Assert.Equal(20, b.Length);
        Assert.Equal(30, arena.BytesAllocated);
    }

    [Fact]
    public void Allocate_PreservesWrittenBytesAcrossCalls()
    {
        using var arena = new Arena(slabSize: 1024);

        Span<byte> a = arena.Allocate(4);
        a[0] = 1; a[1] = 2; a[2] = 3; a[3] = 4;
        Span<byte> b = arena.Allocate(4);
        b[0] = 9;

        // The first allocation's bytes are not disturbed by the second.
        Assert.Equal(1, a[0]);
        Assert.Equal(4, a[3]);
        Assert.Equal(9, b[0]);
    }

    [Fact]
    public void Allocate_CrossingSlabBoundary_StillSucceeds()
    {
        using var arena = new Arena(slabSize: 16);

        Span<byte> first = arena.Allocate(12);
        Span<byte> second = arena.Allocate(12); // forces a new slab

        Assert.Equal(12, first.Length);
        Assert.Equal(12, second.Length);
        Assert.Equal(24, arena.BytesAllocated);
    }

    [Fact]
    public void Allocate_OversizedRequest_GetsDedicatedSlab()
    {
        using var arena = new Arena(slabSize: 16);

        Span<byte> big = arena.Allocate(100);

        Assert.Equal(100, big.Length);
        Assert.Equal(100, arena.BytesAllocated);
    }

    [Fact]
    public void Reset_ZeroesCounter_AndArenaIsReusable()
    {
        using var arena = new Arena(slabSize: 64);
        arena.Allocate(40);

        arena.Reset();

        Assert.Equal(0, arena.BytesAllocated);
        Span<byte> after = arena.Allocate(8);
        Assert.Equal(8, after.Length);
        Assert.Equal(8, arena.BytesAllocated);
    }

    [Fact]
    public void Allocate_WithinSlab_IsAllocationFreeInSteadyState()
    {
        using var arena = new Arena(slabSize: 1 << 20);

        // Warm: prime the pool and JIT.
        for (int i = 0; i < 200; i++)
        {
            arena.Reset();
            _ = arena.Allocate(32);
        }

        arena.Reset();
        const int n = 10_000;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < n; i++)
        {
            _ = arena.Allocate(32); // 10_000 * 32 = 320 KB, well within the 1 MB slab
        }
        long perOp = (GC.GetAllocatedBytesForCurrentThread() - before) / n;

        Assert.True(perOp == 0, $"Arena.Allocate within a slab allocated {perOp} B/op (expected 0)");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~ArenaTests"`
Expected: FAIL to compile — `Arena` does not exist.

- [ ] **Step 3: Write the implementation**

Create `DataVo.Core/StorageEngine/Lsm/Arena.cs`:

```csharp
using System.Buffers;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>
/// A single-writer bump allocator backed by pooled byte slabs. Allocations carve forward through the
/// current slab with no per-call heap allocation; crossing a slab boundary rents a fresh slab from
/// <see cref="ArrayPool{T}.Shared"/>. <see cref="Reset"/> returns every slab to the pool, so an arena —
/// and thus a MemTable generation — recycles its memory with ~0 steady-state GC pressure.
/// <para>Not thread-safe for concurrent allocation; the LSM commit pipeline is the sole writer.</para>
/// </summary>
public sealed class Arena : IDisposable
{
    private readonly int _slabSize;
    private readonly List<byte[]> _slabs = [];
    private byte[] _current;
    private int _offset;
    private long _bytesAllocated;

    /// <summary>Creates an arena whose standard slab is <paramref name="slabSize"/> bytes.</summary>
    public Arena(int slabSize = 1 << 20)
    {
        if (slabSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(slabSize));
        }

        _slabSize = slabSize;
        _current = ArrayPool<byte>.Shared.Rent(slabSize);
        _slabs.Add(_current);
    }

    /// <summary>Total bytes handed out since construction or the last <see cref="Reset"/>.</summary>
    public long BytesAllocated => _bytesAllocated;

    /// <summary>
    /// Carves <paramref name="size"/> bytes from the arena and returns a writable span over them. The
    /// span stays valid until the next <see cref="Reset"/> or <see cref="Dispose"/>.
    /// </summary>
    public Span<byte> Allocate(int size)
    {
        if (size < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(size));
        }

        if (_offset + size > _current.Length)
        {
            // Oversized requests get a dedicated exact slab; otherwise rent a fresh standard slab.
            int rent = Math.Max(size, _slabSize);
            _current = ArrayPool<byte>.Shared.Rent(rent);
            _slabs.Add(_current);
            _offset = 0;
        }

        Span<byte> span = _current.AsSpan(_offset, size);
        _offset += size;
        _bytesAllocated += size;
        return span;
    }

    /// <summary>Returns every slab to the pool and re-arms the arena with a single fresh slab.</summary>
    public void Reset()
    {
        foreach (byte[] slab in _slabs)
        {
            ArrayPool<byte>.Shared.Return(slab);
        }

        _slabs.Clear();
        _current = ArrayPool<byte>.Shared.Rent(_slabSize);
        _slabs.Add(_current);
        _offset = 0;
        _bytesAllocated = 0;
    }

    /// <summary>Returns all slabs to the pool. The arena must not be used after disposal.</summary>
    public void Dispose()
    {
        foreach (byte[] slab in _slabs)
        {
            ArrayPool<byte>.Shared.Return(slab);
        }

        _slabs.Clear();
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~ArenaTests"`
Expected: PASS (6 tests).

- [ ] **Step 5: Commit**

```bash
git add DataVo.Core/StorageEngine/Lsm/Arena.cs DataVo.Tests/Lsm/ArenaTests.cs
git commit -m "feat: LSM Arena pooled single-writer bump allocator

Claude-Session: https://claude.ai/code/session_01P9EXib7WpNKu16aZKiveoS"
```

---

### Task 3: `BloomFilter` — allocation-free double-hash filter

**Files:**
- Create: `DataVo.Core/StorageEngine/Lsm/BloomFilter.cs`
- Test: `DataVo.Tests/Lsm/BloomFilterTests.cs`

**Interfaces:**
- Consumes: `System.Buffers.Binary.BinaryPrimitives`.
- Produces (consumed by Plan 3 — the SSTable writer/reader):
  - `sealed class BloomFilter`
    - `static BloomFilter Create(int expectedKeys, int bitsPerKey = 10)`
    - `static BloomFilter FromBytes(ReadOnlySpan<byte> serialized)`
    - `byte[] ToBytes()`
    - `void Add(ReadOnlySpan<byte> key)`
    - `bool MightContain(ReadOnlySpan<byte> key)` — allocation-free
    - `int BitCount { get; }`, `int NumProbes { get; }`

- [ ] **Step 1: Write the failing tests**

Create `DataVo.Tests/Lsm/BloomFilterTests.cs`:

```csharp
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

public class BloomFilterTests
{
    private static byte[] Key(int i)
    {
        var buf = new byte[8];
        InternalKey.EncodeInt64UserKey(buf, i);
        return buf;
    }

    [Fact]
    public void Add_Then_MightContain_HasNoFalseNegatives()
    {
        BloomFilter filter = BloomFilter.Create(expectedKeys: 1000);
        for (int i = 0; i < 1000; i++)
        {
            filter.Add(Key(i));
        }

        for (int i = 0; i < 1000; i++)
        {
            Assert.True(filter.MightContain(Key(i)), $"false negative for key {i}");
        }
    }

    [Fact]
    public void MightContain_FalsePositiveRate_IsNearTarget()
    {
        const int n = 10_000;
        BloomFilter filter = BloomFilter.Create(expectedKeys: n, bitsPerKey: 10);
        for (int i = 0; i < n; i++)
        {
            filter.Add(Key(i));
        }

        int falsePositives = 0;
        for (int i = n; i < 2 * n; i++) // keys that were never added
        {
            if (filter.MightContain(Key(i)))
            {
                falsePositives++;
            }
        }

        double fpr = (double)falsePositives / n;
        // 10 bits/key targets ~1%. Allow generous headroom for hash variance.
        Assert.True(fpr < 0.03, $"false-positive rate {fpr:P2} exceeds 3%");
    }

    [Fact]
    public void ToBytes_FromBytes_RoundTripsMembership()
    {
        BloomFilter source = BloomFilter.Create(expectedKeys: 500);
        for (int i = 0; i < 500; i++)
        {
            source.Add(Key(i));
        }

        BloomFilter reloaded = BloomFilter.FromBytes(source.ToBytes());

        Assert.Equal(source.BitCount, reloaded.BitCount);
        Assert.Equal(source.NumProbes, reloaded.NumProbes);
        for (int i = 0; i < 500; i++)
        {
            Assert.True(reloaded.MightContain(Key(i)));
        }
    }

    [Fact]
    public void MightContain_IsAllocationFree()
    {
        BloomFilter filter = BloomFilter.Create(expectedKeys: 1000);
        for (int i = 0; i < 1000; i++)
        {
            filter.Add(Key(i));
        }

        byte[] probe = Key(7);
        for (int i = 0; i < 200; i++)
        {
            _ = filter.MightContain(probe); // warm
        }

        const int n = 50_000;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < n; i++)
        {
            _ = filter.MightContain(probe);
        }
        long perOp = (GC.GetAllocatedBytesForCurrentThread() - before) / n;

        Assert.True(perOp == 0, $"MightContain allocated {perOp} B/op (expected 0)");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~BloomFilterTests"`
Expected: FAIL to compile — `BloomFilter` does not exist.

- [ ] **Step 3: Write the implementation**

Create `DataVo.Core/StorageEngine/Lsm/BloomFilter.cs`:

```csharp
using System.Buffers.Binary;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>
/// An allocation-free Bloom filter over user primary keys, used to skip SSTables that cannot contain a
/// key during a point lookup. Backed by a single <see cref="byte"/> bit array; the <c>k</c> probe
/// positions are synthesized from one 64-bit FNV-1a hash via double hashing
/// (Kirsch–Mitzenmacher: <c>g_i = h1 + i·h2</c>), so a membership test computes exactly one hash and
/// performs no heap allocation.
/// </summary>
public sealed class BloomFilter
{
    // Serialized layout: [int32 bitCount][byte numProbes][3 bytes reserved][bitset...].
    private const int HeaderSize = 8;

    private readonly byte[] _bits;
    private readonly int _bitCount;
    private readonly int _numProbes;

    private BloomFilter(byte[] bits, int bitCount, int numProbes)
    {
        _bits = bits;
        _bitCount = bitCount;
        _numProbes = numProbes;
    }

    /// <summary>Number of addressable bits in the filter.</summary>
    public int BitCount => _bitCount;

    /// <summary>Number of probe positions tested per key.</summary>
    public int NumProbes => _numProbes;

    /// <summary>Builds an empty filter sized for <paramref name="expectedKeys"/> at the given bits-per-key.</summary>
    public static BloomFilter Create(int expectedKeys, int bitsPerKey = 10)
    {
        if (expectedKeys < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(expectedKeys));
        }

        if (bitsPerKey <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(bitsPerKey));
        }

        int bitCount = Math.Max(64, expectedKeys * bitsPerKey);
        // Optimal probe count k = bitsPerKey * ln 2, clamped to a sane range.
        int numProbes = Math.Clamp((int)Math.Round(bitsPerKey * 0.69314718), 1, 30);
        var bits = new byte[(bitCount + 7) / 8];
        return new BloomFilter(bits, bitCount, numProbes);
    }

    /// <summary>Wraps a persisted header + bitset for read-time probing (used by the SSTable reader).</summary>
    public static BloomFilter FromBytes(ReadOnlySpan<byte> serialized)
    {
        int bitCount = BinaryPrimitives.ReadInt32LittleEndian(serialized);
        int numProbes = serialized[4];
        byte[] bits = serialized[HeaderSize..].ToArray();
        return new BloomFilter(bits, bitCount, numProbes);
    }

    /// <summary>Serializes the header + bitset for storage in an SSTable filter block.</summary>
    public byte[] ToBytes()
    {
        var buffer = new byte[HeaderSize + _bits.Length];
        BinaryPrimitives.WriteInt32LittleEndian(buffer, _bitCount);
        buffer[4] = (byte)_numProbes;
        _bits.CopyTo(buffer.AsSpan(HeaderSize));
        return buffer;
    }

    /// <summary>Records <paramref name="key"/> as present. Allocation-free.</summary>
    public void Add(ReadOnlySpan<byte> key)
    {
        (uint h1, uint h2) = DoubleHash(key);
        for (int i = 0; i < _numProbes; i++)
        {
            int bit = (int)(unchecked(h1 + (uint)i * h2) % (uint)_bitCount);
            _bits[bit >> 3] |= (byte)(1 << (bit & 7));
        }
    }

    /// <summary>Returns false only if <paramref name="key"/> is definitely absent. Allocation-free.</summary>
    public bool MightContain(ReadOnlySpan<byte> key)
    {
        (uint h1, uint h2) = DoubleHash(key);
        for (int i = 0; i < _numProbes; i++)
        {
            int bit = (int)(unchecked(h1 + (uint)i * h2) % (uint)_bitCount);
            if ((_bits[bit >> 3] & (byte)(1 << (bit & 7))) == 0)
            {
                return false;
            }
        }

        return true;
    }

    private static (uint H1, uint H2) DoubleHash(ReadOnlySpan<byte> key)
    {
        ulong hash = Fnv1a64(key);
        uint h1 = (uint)(hash & 0xFFFFFFFF);
        uint h2 = (uint)(hash >> 32);
        if (h2 == 0)
        {
            h2 = 0x9E3779B1; // avoid a zero stride collapsing every probe onto h1
        }

        return (h1, h2);
    }

    private static ulong Fnv1a64(ReadOnlySpan<byte> data)
    {
        const ulong offsetBasis = 14695981039346656037UL;
        const ulong prime = 1099511628211UL;

        ulong hash = offsetBasis;
        for (int i = 0; i < data.Length; i++)
        {
            hash ^= data[i];
            hash *= prime;
        }

        return hash;
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~BloomFilterTests"`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add DataVo.Core/StorageEngine/Lsm/BloomFilter.cs DataVo.Tests/Lsm/BloomFilterTests.cs
git commit -m "feat: LSM allocation-free double-hash Bloom filter

Claude-Session: https://claude.ai/code/session_01P9EXib7WpNKu16aZKiveoS"
```

---

### Task 4: Plan-1 gate — full suite green + AOT-clean build

**Files:** none (verification only).

- [ ] **Step 1: Run the full test suite**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj`
Expected: the three new test classes pass; the only failures are the 3 known pre-existing allocation micro-benchmarks (`CompiledQueryReadAllocationSpikeTests.Spike_PointLookupAllocationBreakdown`, `CompiledAccessPathTests.SelectManyTyped_ReclaimsMaterializationLayer_ScalingWithColumnCount`, `SelectManyTyped_StreamingProjected_PerRowAllocationIsNearMinimal`). No new failures.

- [ ] **Step 2: Confirm the core still builds AOT-clean**

Run: `dotnet build DataVo.Core/DataVo.Core.csproj -c Release`
Expected: `0 Warning(s) 0 Error(s)` (no new AOT/trim warnings from the `Lsm` namespace).

- [ ] **Step 3: Commit (if any incidental fixes were needed; otherwise skip)**

```bash
git commit -am "chore: Plan 1 gate — LSM leaf primitives green and AOT-clean

Claude-Session: https://claude.ai/code/session_01P9EXib7WpNKu16aZKiveoS"
```

---

## Roadmap — Plans 2–5 (forthcoming detailed plans)

Each becomes its own document with full bite-sized TDD tasks when we reach it. Listed here so the leaf interfaces above are designed against known consumers.

- **Plan 2 — MemTable (`MemTable.cs`):** arena-backed single-writer sorted skiplist keyed by `InternalKey`; `Put(userKey, seqno, type, value)`, `bool TryGet(userKey, snapshotSeqno, out value, out isTombstone)`, sorted forward iterator, `long ApproximateBytes`, freeze→immutable. Consumes Task 1 + Task 2. Tests: newest-wins, tombstone short-circuit, snapshot reads, sorted iteration, ~0-GC write loop.
- **Plan 3 — SSTable (`SsTableWriter.cs`, `SsTableReader.cs`):** data/filter/index/footer layout from §6; writer streams a sorted MemTable to an immutable `.sst` building a `BloomFilter` (Task 3) inline; reader loads footer→index→bloom, point-get gated by bloom, forward iterator. Consumes Tasks 1 + 3. Tests: round-trip, block-boundary keys, bloom-skip counter, torn-file rejection.
- **Plan 4 — Manifest (`Manifest.cs`, `Version.cs`):** `VersionEdit` append-log + `CURRENT` pointer + refcounted `Version`; crash-atomic install/compaction swap; torn-edit CRC rejection. Tests: edit replay, refcount-gated file deletion, crash matrix.
- **Plan 5 — `LsmStorageEngine` + `Compactor` + recovery + secondary indexes:** implement `IStorageEngine` over per-table memtables/levels; WAL frames as redo log (§5); hybrid L0-tiered + leveled compaction (§8); WAL-tail recovery into a fresh memtable (§9); secondary indexes re-keyed value→PK (§10); config switch + `--engine datavo-lsm` benchmark. Consumes Plans 1–4 + the Task 0 audit. Tests: end-to-end CRUD, restart durability, crash matrix, A/B benchmark vs B-tree engine.
