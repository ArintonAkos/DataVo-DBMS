# LSM Engine — Plan 2: The MemTable

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the arena-backed, single-writer, sorted skiplist `MemTable` keyed by `InternalKey`, supporting `Put`, `Delete` (tombstones), snapshot-filtered `TryGet`, sorted iteration, and freeze — with a ~0-GC steady-state write loop.

**Architecture:** Plan 2 of the 5-plan LSM sequence (spec: `docs/superpowers/specs/2026-06-29-lsm-storage-engine-design.md`). It consumes the Plan-1 leaf primitives `InternalKey` and `Arena`. The skiplist is **fully arena-resident**: every node — its header, forward pointers, internal-key bytes, and value bytes — is one contiguous record allocated from the `Arena`. Forward pointers are stable `long` handles (`slabIndex << 32 | offsetInSlab`), so the structure is pointer-linked without a single managed object per node, giving zero per-`Put` GC. The MemTable is single-writer / multi-reader (the engine's group-commit thread is the only mutator); reads take a snapshot sequence number.

**Tech Stack:** C# / .NET 10, xUnit 2.9.3, `System.Buffers` / `System.Buffers.Binary`. No new NuGet packages.

## Global Constraints

- **Target framework:** `net10.0`.
- **Native AOT clean:** no reflection, no `System.Reflection.Emit`, no runtime serializers. Spans, `BinaryPrimitives`, `ArrayPool` only.
- **No new NuGet dependencies.**
- **Production namespace/folder:** `DataVo.Core/StorageEngine/Lsm/` → namespace `DataVo.Core.StorageEngine.Lsm`.
- **Test namespace/folder:** `DataVo.Tests/Lsm/` → namespace `DataVo.Tests.Lsm`. xUnit (`[Fact]`/`[Theory]`).
- **Consumes (from Plan 1, already on the branch):**
  - `InternalKey`: `int Write(Span<byte> dest, ReadOnlySpan<byte> userKey, ulong seqno, LsmValueType valueType)`, `int MeasureSize(int userKeyLength)`, `ReadOnlySpan<byte> UserKey(ReadOnlySpan<byte>)`, `ulong Sequence(ReadOnlySpan<byte>)`, `LsmValueType ValueType(ReadOnlySpan<byte>)`, `int Compare(ReadOnlySpan<byte> a, ReadOnlySpan<byte> b)`, `int EncodeInt64UserKey(Span<byte>, long)`, `const int TagSize = 8`, `const ulong MaxSequenceNumber`. `enum LsmValueType : byte { Deletion = 0, Put = 1 }`.
  - `Arena`: `Arena(int slabSize = 1<<20)`, `Span<byte> Allocate(int size)`, `long BytesAllocated`, `Reset()`, `Dispose()`. (Task 1 extends it.)
- **Allocation assertions** use the established pattern: warm ~200×, then measure `GC.GetAllocatedBytesForCurrentThread()` over an N-iteration loop and assert `perOp <= budget`.
- **Commit message trailer:** every commit body ends with `Claude-Session: https://claude.ai/code/session_01P9EXib7WpNKu16aZKiveoS`.
- **Branch:** `fix/concurrent-ops-alloc-and-locking` (do NOT push; commit locally only).
- **Carry-forward contract for Plan 3 (DO NOT lose):** the Bloom filter must `Add`/probe with `InternalKey.UserKey(...)` (bare user key), never the tagged internal key.

## Concurrency model (explicit, v1)

Single-writer / multi-reader. The MemTable is append-only (nodes are never removed or moved; `Freeze` only stops further writes; the whole MemTable is swapped out atomically by the engine in Plan 5). A node's forward pointers are fully written **before** it is spliced in, and the splice is a single store. Tests in this plan are single-threaded (write, then read). Formal lock-free memory-ordering hardening (volatile/acquire-release on forward slots) is a documented carry-forward to a later plan; it is out of scope here.

---

## File structure (Plan 2)

- `DataVo.Core/StorageEngine/Lsm/Arena.cs` — **modify**: add handle-based `Allocate`/`Resolve` (Task 1).
- `DataVo.Core/StorageEngine/Lsm/MemTable.cs` — **create**: the skiplist (Tasks 2–5).
- `DataVo.Tests/Lsm/ArenaTests.cs` — **modify**: handle-addressing tests (Task 1).
- `DataVo.Tests/Lsm/MemTableTests.cs` — **create**: MemTable behavior tests (Tasks 2–5).

---

### Task 1: Arena handle addressing

**Files:**
- Modify: `DataVo.Core/StorageEngine/Lsm/Arena.cs`
- Test: `DataVo.Tests/Lsm/ArenaTests.cs` (append tests)

**Interfaces:**
- Consumes: existing `Arena`.
- Produces (consumed by Tasks 2–5):
  - `Span<byte> Allocate(int size, out long handle)` — same bump semantics as `Allocate(int)`, additionally returning a stable handle `((long)slabIndex << 32) | (uint)offsetInSlab`.
  - `Span<byte> Resolve(long handle, int length)` — returns the span for a previously returned handle. Valid until `Reset`/`Dispose`.
  - Existing `Span<byte> Allocate(int size)` keeps identical behavior (now delegates).

- [ ] **Step 1: Write the failing tests** (append to `DataVo.Tests/Lsm/ArenaTests.cs`)

```csharp
    [Fact]
    public void Allocate_WithHandle_ResolvesBackToSameBytes()
    {
        using var arena = new Arena(slabSize: 1024);

        Span<byte> a = arena.Allocate(4, out long ha);
        a[0] = 11; a[1] = 22; a[2] = 33; a[3] = 44;
        Span<byte> b = arena.Allocate(8, out long hb);
        b[0] = 99;

        Span<byte> ra = arena.Resolve(ha, 4);
        Span<byte> rb = arena.Resolve(hb, 8);
        Assert.Equal(11, ra[0]);
        Assert.Equal(44, ra[3]);
        Assert.Equal(99, rb[0]);
        Assert.NotEqual(ha, hb);
    }

    [Fact]
    public void Resolve_StaysValid_AcrossSlabBoundary()
    {
        using var arena = new Arena(slabSize: 16);

        Span<byte> first = arena.Allocate(12, out long h1);
        first[0] = 7;
        Span<byte> second = arena.Allocate(12, out long h2); // forces a new slab
        second[0] = 8;

        // The first handle still resolves correctly after the second allocation moved to a new slab.
        Assert.Equal(7, arena.Resolve(h1, 12)[0]);
        Assert.Equal(8, arena.Resolve(h2, 12)[0]);
        Assert.NotEqual(h1, h2);
    }

    [Fact]
    public void Allocate_WithHandle_AfterDispose_Throws()
    {
        var arena = new Arena(slabSize: 64);
        arena.Dispose();
        Assert.Throws<ObjectDisposedException>(() => arena.Allocate(8, out _));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~ArenaTests"`
Expected: FAIL to compile — `Allocate(int, out long)` / `Resolve` do not exist.

- [ ] **Step 3: Modify `Arena.cs`**

Replace the existing `Allocate(int size)` method with the following two methods (keep everything else — fields, ctor, `Reset`, `Dispose`, the `_disposed` guard — unchanged):

```csharp
    /// <summary>
    /// Carves <paramref name="size"/> bytes from the arena and returns a writable span over them. The
    /// span stays valid until the next <see cref="Reset"/> or <see cref="Dispose"/>.
    /// </summary>
    public Span<byte> Allocate(int size) => Allocate(size, out _);

    /// <summary>
    /// Carves <paramref name="size"/> bytes and returns both the writable span and a stable
    /// <paramref name="handle"/> that <see cref="Resolve"/> maps back to those bytes. The handle packs the
    /// slab index in the high 32 bits and the in-slab offset in the low 32 bits; it stays valid until the
    /// next <see cref="Reset"/> or <see cref="Dispose"/>.
    /// </summary>
    public Span<byte> Allocate(int size, out long handle)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        if (size < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(size));
        }

        if (_offset + size > _current.Length)
        {
            int rent = Math.Max(size, _slabSize);
            _current = ArrayPool<byte>.Shared.Rent(rent);
            _slabs.Add(_current);
            _offset = 0;
        }

        int slabIndex = _slabs.Count - 1;
        handle = ((long)slabIndex << 32) | (uint)_offset;

        Span<byte> span = _current.AsSpan(_offset, size);
        _offset += size;
        _bytesAllocated += size;
        return span;
    }

    /// <summary>
    /// Maps a handle returned by <see cref="Allocate(int, out long)"/> back to its bytes. The returned span
    /// is valid until the next <see cref="Reset"/> or <see cref="Dispose"/>.
    /// </summary>
    public Span<byte> Resolve(long handle, int length)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);

        int slabIndex = (int)(handle >> 32);
        int offset = (int)(handle & 0xFFFFFFFF);
        return _slabs[slabIndex].AsSpan(offset, length);
    }
```

Note: the original `Allocate(int)` body moves into `Allocate(int, out long)`; the `ObjectDisposedException.ThrowIf` guard that was at the top of `Allocate(int)` now lives in `Allocate(int, out long)` (the delegating one-liner inherits it).

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~ArenaTests"`
Expected: PASS — all prior Arena tests (10) plus the 3 new ones (13 total).

- [ ] **Step 5: Commit**

```bash
git add DataVo.Core/StorageEngine/Lsm/Arena.cs DataVo.Tests/Lsm/ArenaTests.cs
git commit -m "feat: Arena stable-handle addressing for pointer-linked structures

Claude-Session: https://claude.ai/code/session_01P9EXib7WpNKu16aZKiveoS"
```

---

### Task 2: MemTable skiplist core — Put + TryGet

**Files:**
- Create: `DataVo.Core/StorageEngine/Lsm/MemTable.cs`
- Test: `DataVo.Tests/Lsm/MemTableTests.cs`

**Interfaces:**
- Consumes: `InternalKey`, `LsmValueType`, `Arena` (incl. Task-1 `Allocate(out handle)`/`Resolve`).
- Produces (consumed by Tasks 3–5 and Plan 3):
  - `sealed class MemTable : IDisposable`
    - `MemTable(int slabSize = 1 << 20)`
    - `void Put(ReadOnlySpan<byte> userKey, ulong seqno, LsmValueType valueType, ReadOnlySpan<byte> value)`
    - `bool TryGet(ReadOnlySpan<byte> userKey, ulong snapshotSeqno, out ReadOnlySpan<byte> value, out bool isTombstone)`
    - `int Count { get; }`
    - `long ApproximateBytes { get; }`
    - private helpers used by later tasks: `GetForward`, `GetKey`, `GetValue`, head handle.

- [ ] **Step 1: Write the failing tests** (`DataVo.Tests/Lsm/MemTableTests.cs`)

```csharp
using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

public class MemTableTests
{
    private static byte[] Key(long pk)
    {
        var buf = new byte[8];
        InternalKey.EncodeInt64UserKey(buf, pk);
        return buf;
    }

    private static byte[] Val(string s) => System.Text.Encoding.UTF8.GetBytes(s);

    [Fact]
    public void Put_Then_TryGet_ReturnsValue()
    {
        using var table = new MemTable();
        table.Put(Key(1), seqno: 5, LsmValueType.Put, Val("hello"));

        bool found = table.TryGet(Key(1), snapshotSeqno: 10, out ReadOnlySpan<byte> value, out bool tomb);

        Assert.True(found);
        Assert.False(tomb);
        Assert.Equal("hello", System.Text.Encoding.UTF8.GetString(value));
        Assert.Equal(1, table.Count);
    }

    [Fact]
    public void TryGet_MissingKey_ReturnsFalse()
    {
        using var table = new MemTable();
        table.Put(Key(1), 5, LsmValueType.Put, Val("a"));

        Assert.False(table.TryGet(Key(2), 10, out _, out _));
    }

    [Fact]
    public void TryGet_ReturnsNewestVersionAtOrBelowSnapshot()
    {
        using var table = new MemTable();
        table.Put(Key(1), 3, LsmValueType.Put, Val("old"));
        table.Put(Key(1), 7, LsmValueType.Put, Val("new"));

        // Snapshot above both → newest (seqno 7).
        Assert.True(table.TryGet(Key(1), 10, out ReadOnlySpan<byte> v1, out _));
        Assert.Equal("new", System.Text.Encoding.UTF8.GetString(v1));

        // Snapshot between them → only the older version is visible.
        Assert.True(table.TryGet(Key(1), 5, out ReadOnlySpan<byte> v2, out _));
        Assert.Equal("old", System.Text.Encoding.UTF8.GetString(v2));

        // Snapshot below both → not visible.
        Assert.False(table.TryGet(Key(1), 2, out _, out _));
    }

    [Fact]
    public void TryGet_ManyKeys_AllRetrievable()
    {
        using var table = new MemTable();
        for (int i = 0; i < 1000; i++)
        {
            table.Put(Key(i), (ulong)(i + 1), LsmValueType.Put, Val($"v{i}"));
        }

        Assert.Equal(1000, table.Count);
        for (int i = 0; i < 1000; i++)
        {
            Assert.True(table.TryGet(Key(i), 2000, out ReadOnlySpan<byte> v, out _), $"missing key {i}");
            Assert.Equal($"v{i}", System.Text.Encoding.UTF8.GetString(v));
        }
    }

    [Fact]
    public void ApproximateBytes_GrowsWithInserts()
    {
        using var table = new MemTable();
        long before = table.ApproximateBytes;
        table.Put(Key(1), 1, LsmValueType.Put, Val("xyz"));
        Assert.True(table.ApproximateBytes > before);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~MemTableTests"`
Expected: FAIL to compile — `MemTable` does not exist.

- [ ] **Step 3: Create `DataVo.Core/StorageEngine/Lsm/MemTable.cs`**

```csharp
using System.Buffers.Binary;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>
/// A single-writer, multi-reader sorted skiplist keyed by <see cref="InternalKey"/>, backing one LSM
/// MemTable generation. Every node — header, forward pointers, internal-key bytes, and value bytes — is one
/// contiguous record carved from an <see cref="Arena"/>; forward pointers are stable arena handles, so the
/// structure is pointer-linked with no managed object per insert (zero per-<see cref="Put"/> GC).
/// <para>
/// Node record layout (little-endian): <c>[int valueLen][int keyLen][byte height][long forward[height]]
/// [internalKey bytes][value bytes]</c>. A null forward link is <see cref="Null"/> (-1). The head is a
/// keyless sentinel of maximum height.
/// </para>
/// <para>Single-writer: only one thread calls <see cref="Put"/>. Readers may run concurrently.</para>
/// </summary>
public sealed class MemTable : IDisposable
{
    private const int MaxHeight = 12;
    private const long Null = -1L;

    // Header field offsets within a node record.
    private const int ValueLenOffset = 0;
    private const int KeyLenOffset = 4;
    private const int HeightOffset = 8;
    private const int ForwardOffset = 9; // forward[0] starts here; each link is 8 bytes.

    private readonly Arena _arena;
    private readonly long _head;
    private int _maxHeight = 1;
    private int _count;
    private ulong _rngState = 0x9E3779B97F4A7C15UL; // fixed seed → deterministic structure for tests.
    private bool _frozen;
    private bool _disposed;

    /// <summary>Creates an empty MemTable whose arena uses <paramref name="slabSize"/>-byte slabs.</summary>
    public MemTable(int slabSize = 1 << 20)
    {
        _arena = new Arena(slabSize);
        _head = AllocateHead();
    }

    /// <summary>Number of entries (versions) inserted, including tombstones.</summary>
    public int Count => _count;

    /// <summary>Approximate resident size in bytes (the arena's handed-out total).</summary>
    public long ApproximateBytes => _arena.BytesAllocated;

    /// <summary>Whether the MemTable has been frozen (no further writes accepted).</summary>
    public bool IsFrozen => _frozen;

    /// <summary>
    /// Inserts a versioned entry. Each call inserts a new node; multiple versions of a user key coexist,
    /// newest (highest seqno) first. A tombstone is a <see cref="LsmValueType.Deletion"/> entry.
    /// </summary>
    public void Put(ReadOnlySpan<byte> userKey, ulong seqno, LsmValueType valueType, ReadOnlySpan<byte> value)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        if (_frozen)
        {
            throw new InvalidOperationException("Cannot Put into a frozen MemTable.");
        }

        int height = RandomHeight();
        long node = AllocateNode(userKey, seqno, valueType, value, height);
        ReadOnlySpan<byte> internalKey = GetKey(node);

        Span<long> update = stackalloc long[MaxHeight];
        long x = _head;
        for (int level = _maxHeight - 1; level >= 0; level--)
        {
            long next = GetForward(x, level);
            while (next != Null && InternalKey.Compare(GetKey(next), internalKey) < 0)
            {
                x = next;
                next = GetForward(x, level);
            }

            update[level] = x;
        }

        if (height > _maxHeight)
        {
            for (int level = _maxHeight; level < height; level++)
            {
                update[level] = _head;
            }

            _maxHeight = height;
        }

        for (int level = 0; level < height; level++)
        {
            SetForward(node, level, GetForward(update[level], level));
            SetForward(update[level], level, node);
        }

        _count++;
    }

    /// <summary>
    /// Returns the newest version of <paramref name="userKey"/> with sequence number ≤
    /// <paramref name="snapshotSeqno"/>. A tombstone sets <paramref name="isTombstone"/> and returns
    /// <see langword="false"/> (the key is logically absent at that snapshot).
    /// </summary>
    public bool TryGet(ReadOnlySpan<byte> userKey, ulong snapshotSeqno, out ReadOnlySpan<byte> value, out bool isTombstone)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        value = default;
        isTombstone = false;

        // Lookup key sorts at-or-before any real entry at seqno == snapshot: tag = (snapshot << 8) | 0xFF
        // is >= any real tag (type 0/1) at that seqno, so under descending-tag order it precedes them.
        int ikLen = InternalKey.MeasureSize(userKey.Length);
        Span<byte> lookup = ikLen <= 256 ? stackalloc byte[256] : new byte[ikLen];
        lookup = lookup[..ikLen];
        userKey.CopyTo(lookup);
        ulong tag = (snapshotSeqno << 8) | 0xFF;
        BinaryPrimitives.WriteUInt64BigEndian(lookup.Slice(userKey.Length, InternalKey.TagSize), tag);

        long node = FindGreaterOrEqual(lookup);
        if (node == Null)
        {
            return false;
        }

        ReadOnlySpan<byte> nodeKey = GetKey(node);
        if (!InternalKey.UserKey(nodeKey).SequenceEqual(userKey))
        {
            return false;
        }

        if (InternalKey.ValueType(nodeKey) == LsmValueType.Deletion)
        {
            isTombstone = true;
            return false;
        }

        value = GetValue(node);
        return true;
    }

    private long FindGreaterOrEqual(ReadOnlySpan<byte> internalKey)
    {
        long x = _head;
        for (int level = _maxHeight - 1; level >= 0; level--)
        {
            long next = GetForward(x, level);
            while (next != Null && InternalKey.Compare(GetKey(next), internalKey) < 0)
            {
                x = next;
                next = GetForward(x, level);
            }
        }

        return GetForward(x, 0);
    }

    private long AllocateHead()
    {
        int total = ForwardOffset + (8 * MaxHeight); // 8 bytes per forward link (a long handle).
        Span<byte> rec = _arena.Allocate(total, out long handle);
        BinaryPrimitives.WriteInt32LittleEndian(rec[ValueLenOffset..], 0);
        BinaryPrimitives.WriteInt32LittleEndian(rec[KeyLenOffset..], 0);
        rec[HeightOffset] = MaxHeight;
        for (int level = 0; level < MaxHeight; level++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(rec.Slice(ForwardOffset + (8 * level), 8), Null);
        }

        return handle;
    }

    private long AllocateNode(ReadOnlySpan<byte> userKey, ulong seqno, LsmValueType valueType, ReadOnlySpan<byte> value, int height)
    {
        int keyLen = InternalKey.MeasureSize(userKey.Length);
        int keyStart = ForwardOffset + (8 * height);
        int total = keyStart + keyLen + value.Length;

        Span<byte> rec = _arena.Allocate(total, out long handle);
        BinaryPrimitives.WriteInt32LittleEndian(rec[ValueLenOffset..], value.Length);
        BinaryPrimitives.WriteInt32LittleEndian(rec[KeyLenOffset..], keyLen);
        rec[HeightOffset] = (byte)height;
        for (int level = 0; level < height; level++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(rec.Slice(ForwardOffset + (8 * level), 8), Null);
        }

        InternalKey.Write(rec.Slice(keyStart, keyLen), userKey, seqno, valueType);
        value.CopyTo(rec.Slice(keyStart + keyLen, value.Length));
        return handle;
    }

    private int GetHeight(long node) => _arena.Resolve(node, ForwardOffset)[HeightOffset];

    private long GetForward(long node, int level)
    {
        Span<byte> rec = _arena.Resolve(node, ForwardOffset + (8 * (level + 1)));
        return BinaryPrimitives.ReadInt64LittleEndian(rec.Slice(ForwardOffset + (8 * level), 8));
    }

    private void SetForward(long node, int level, long next)
    {
        Span<byte> rec = _arena.Resolve(node, ForwardOffset + (8 * (level + 1)));
        BinaryPrimitives.WriteInt64LittleEndian(rec.Slice(ForwardOffset + (8 * level), 8), next);
    }

    private ReadOnlySpan<byte> GetKey(long node)
    {
        Span<byte> hdr = _arena.Resolve(node, ForwardOffset);
        int valueLen = BinaryPrimitives.ReadInt32LittleEndian(hdr[ValueLenOffset..]);
        int keyLen = BinaryPrimitives.ReadInt32LittleEndian(hdr[KeyLenOffset..]);
        int height = hdr[HeightOffset];
        int keyStart = ForwardOffset + (8 * height);
        Span<byte> full = _arena.Resolve(node, keyStart + keyLen + valueLen);
        return full.Slice(keyStart, keyLen);
    }

    private ReadOnlySpan<byte> GetValue(long node)
    {
        Span<byte> hdr = _arena.Resolve(node, ForwardOffset);
        int valueLen = BinaryPrimitives.ReadInt32LittleEndian(hdr[ValueLenOffset..]);
        int keyLen = BinaryPrimitives.ReadInt32LittleEndian(hdr[KeyLenOffset..]);
        int height = hdr[HeightOffset];
        int keyStart = ForwardOffset + (8 * height);
        Span<byte> full = _arena.Resolve(node, keyStart + keyLen + valueLen);
        return full.Slice(keyStart + keyLen, valueLen);
    }

    private int RandomHeight()
    {
        int height = 1;
        while (height < MaxHeight && (NextRandom() & 3) == 0) // p = 1/4
        {
            height++;
        }

        return height;
    }

    private ulong NextRandom()
    {
        ulong x = _rngState;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        _rngState = x;
        return x;
    }

    /// <summary>Releases the backing arena. The MemTable must not be used afterwards.</summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        _arena.Dispose();
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~MemTableTests"`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add DataVo.Core/StorageEngine/Lsm/MemTable.cs DataVo.Tests/Lsm/MemTableTests.cs
git commit -m "feat: LSM MemTable arena-resident skiplist (Put + snapshot TryGet)

Claude-Session: https://claude.ai/code/session_01P9EXib7WpNKu16aZKiveoS"
```

---

### Task 3: Delete (tombstones)

**Files:**
- Modify: `DataVo.Core/StorageEngine/Lsm/MemTable.cs`
- Test: `DataVo.Tests/Lsm/MemTableTests.cs` (append)

**Interfaces:**
- Produces: `void Delete(ReadOnlySpan<byte> userKey, ulong seqno)` — inserts a `Deletion` tombstone at `seqno`.

- [ ] **Step 1: Write the failing tests** (append to `MemTableTests.cs`)

```csharp
    [Fact]
    public void Delete_ShadowsOlderPut_AtNewerSnapshot()
    {
        using var table = new MemTable();
        table.Put(Key(1), 3, LsmValueType.Put, Val("alive"));
        table.Delete(Key(1), 7);

        bool found = table.TryGet(Key(1), 10, out _, out bool tomb);

        Assert.False(found);
        Assert.True(tomb);
    }

    [Fact]
    public void Delete_DoesNotAffectReadsBelowTombstoneSeqno()
    {
        using var table = new MemTable();
        table.Put(Key(1), 3, LsmValueType.Put, Val("alive"));
        table.Delete(Key(1), 7);

        // A snapshot below the tombstone still sees the live value.
        Assert.True(table.TryGet(Key(1), 5, out ReadOnlySpan<byte> v, out bool tomb));
        Assert.False(tomb);
        Assert.Equal("alive", System.Text.Encoding.UTF8.GetString(v));
    }

    [Fact]
    public void Put_AfterDelete_Resurrects()
    {
        using var table = new MemTable();
        table.Put(Key(1), 3, LsmValueType.Put, Val("v3"));
        table.Delete(Key(1), 7);
        table.Put(Key(1), 9, LsmValueType.Put, Val("v9"));

        Assert.True(table.TryGet(Key(1), 10, out ReadOnlySpan<byte> v, out bool tomb));
        Assert.False(tomb);
        Assert.Equal("v9", System.Text.Encoding.UTF8.GetString(v));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~MemTableTests"`
Expected: FAIL to compile — `Delete` does not exist.

- [ ] **Step 3: Add `Delete` to `MemTable.cs`** (place it just after `Put`)

```csharp
    /// <summary>Inserts a tombstone for <paramref name="userKey"/> at <paramref name="seqno"/>.</summary>
    public void Delete(ReadOnlySpan<byte> userKey, ulong seqno)
    {
        Put(userKey, seqno, LsmValueType.Deletion, ReadOnlySpan<byte>.Empty);
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~MemTableTests"`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```bash
git add DataVo.Core/StorageEngine/Lsm/MemTable.cs DataVo.Tests/Lsm/MemTableTests.cs
git commit -m "feat: LSM MemTable Delete (tombstone) with snapshot semantics

Claude-Session: https://claude.ai/code/session_01P9EXib7WpNKu16aZKiveoS"
```

---

### Task 4: Sorted iteration

**Files:**
- Modify: `DataVo.Core/StorageEngine/Lsm/MemTable.cs`
- Test: `DataVo.Tests/Lsm/MemTableTests.cs` (append)

**Interfaces:**
- Produces (consumed by Plan 3's flush):
  - `readonly ref struct MemTableEntry { public readonly ReadOnlySpan<byte> InternalKey; public readonly ReadOnlySpan<byte> Value; }`
  - `Enumerator GetEnumerator()` — a `ref struct` walking the level-0 chain in `InternalKey` order (user key ascending, seqno descending).

- [ ] **Step 1: Write the failing test** (append to `MemTableTests.cs`)

```csharp
    [Fact]
    public void Enumerator_YieldsEntriesInInternalKeyOrder()
    {
        using var table = new MemTable();
        // Insert out of order, including two versions of key 5.
        table.Put(Key(5), 2, LsmValueType.Put, Val("five-old"));
        table.Put(Key(1), 1, LsmValueType.Put, Val("one"));
        table.Put(Key(5), 8, LsmValueType.Put, Val("five-new"));
        table.Put(Key(3), 4, LsmValueType.Put, Val("three"));

        var keys = new List<byte[]>();
        foreach (MemTableEntry entry in table)
        {
            keys.Add(entry.InternalKey.ToArray());
        }

        // Adjacent internal keys are strictly non-decreasing under InternalKey.Compare.
        for (int i = 1; i < keys.Count; i++)
        {
            Assert.True(InternalKey.Compare(keys[i - 1], keys[i]) <= 0, $"out of order at {i}");
        }

        // For user key 5, the newer seqno (8) sorts before the older (2).
        var fiveSeqnos = new List<ulong>();
        foreach (byte[] k in keys)
        {
            if (InternalKey.UserKey(k).SequenceEqual(Key(5)))
            {
                fiveSeqnos.Add(InternalKey.Sequence(k));
            }
        }

        Assert.Equal(2, fiveSeqnos.Count);
        Assert.Equal(8UL, fiveSeqnos[0]);
        Assert.Equal(2UL, fiveSeqnos[1]);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~MemTableTests"`
Expected: FAIL to compile — `GetEnumerator` / `MemTableEntry` do not exist.

- [ ] **Step 3: Add the enumerator to `MemTable.cs`** (place before `Dispose`)

```csharp
    /// <summary>One entry yielded by the MemTable enumerator: its full internal key and value bytes.</summary>
    public readonly ref struct MemTableEntry
    {
        /// <summary>The entry's internal key (<c>userKey ‖ tag</c>).</summary>
        public readonly ReadOnlySpan<byte> InternalKey;

        /// <summary>The entry's value bytes (empty for a tombstone).</summary>
        public readonly ReadOnlySpan<byte> Value;

        internal MemTableEntry(ReadOnlySpan<byte> internalKey, ReadOnlySpan<byte> value)
        {
            InternalKey = internalKey;
            Value = value;
        }
    }

    /// <summary>Returns a forward iterator over all entries in <see cref="InternalKey"/> order.</summary>
    public Enumerator GetEnumerator() => new(this);

    /// <summary>Forward iterator over the skiplist's level-0 chain (fully sorted).</summary>
    public ref struct Enumerator
    {
        private readonly MemTable _table;
        private long _node;

        internal Enumerator(MemTable table)
        {
            _table = table;
            _node = table._head;
        }

        /// <summary>The current entry. Valid only after a successful <see cref="MoveNext"/>.</summary>
        public readonly MemTableEntry Current => new(_table.GetKey(_node), _table.GetValue(_node));

        /// <summary>Advances to the next entry; returns <see langword="false"/> at the end.</summary>
        public bool MoveNext()
        {
            _node = _table.GetForward(_node, 0);
            return _node != Null;
        }
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~MemTableTests"`
Expected: PASS (9 tests).

- [ ] **Step 5: Commit**

```bash
git add DataVo.Core/StorageEngine/Lsm/MemTable.cs DataVo.Tests/Lsm/MemTableTests.cs
git commit -m "feat: LSM MemTable sorted forward iterator for flush

Claude-Session: https://claude.ai/code/session_01P9EXib7WpNKu16aZKiveoS"
```

---

### Task 5: Freeze + zero-GC write loop

**Files:**
- Modify: `DataVo.Core/StorageEngine/Lsm/MemTable.cs`
- Test: `DataVo.Tests/Lsm/MemTableTests.cs` (append)

**Interfaces:**
- Produces: `void Freeze()` — marks the MemTable immutable; subsequent `Put`/`Delete` throw `InvalidOperationException`. (The `_frozen` field + the `Put` guard already exist from Task 2; this exposes `Freeze()`.)

- [ ] **Step 1: Write the failing tests** (append to `MemTableTests.cs`)

```csharp
    [Fact]
    public void Freeze_RejectsFurtherWrites_ButAllowsReads()
    {
        using var table = new MemTable();
        table.Put(Key(1), 1, LsmValueType.Put, Val("v"));
        table.Freeze();

        Assert.True(table.IsFrozen);
        Assert.Throws<InvalidOperationException>(() => table.Put(Key(2), 2, LsmValueType.Put, Val("x")));
        Assert.Throws<InvalidOperationException>(() => table.Delete(Key(3), 3));

        // Reads still work after freeze.
        Assert.True(table.TryGet(Key(1), 10, out _, out _));
    }

    [Fact]
    public void Put_SteadyState_IsAllocationFree()
    {
        // A large slab keeps the measured loop inside one slab (no slab rents → no GC).
        using var table = new MemTable(slabSize: 16 << 20);

        // Reuse the key buffer and value so the test harness itself allocates nothing per iteration —
        // any per-op allocation measured then comes from Put, which must be zero within a slab.
        var keyBuf = new byte[8];
        byte[] value = Val("payload");

        for (int i = 0; i < 200; i++) // warm
        {
            InternalKey.EncodeInt64UserKey(keyBuf, i);
            table.Put(keyBuf, (ulong)(i + 1), LsmValueType.Put, value);
        }

        const int n = 10_000;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < n; i++)
        {
            InternalKey.EncodeInt64UserKey(keyBuf, 1_000_000 + i);
            table.Put(keyBuf, (ulong)(i + 1), LsmValueType.Put, value);
        }
        long perOp = (GC.GetAllocatedBytesForCurrentThread() - before) / n;

        Assert.True(perOp == 0, $"MemTable.Put steady-state allocated {perOp} B/op (expected 0)");
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~MemTableTests"`
Expected: FAIL to compile — `Freeze` does not exist. (The allocation test will compile once `Freeze` is added; it must then pass.)

- [ ] **Step 3: Add `Freeze` to `MemTable.cs`** (place just after the `Put` method)

```csharp
    /// <summary>Marks the MemTable immutable. Subsequent writes throw; reads and iteration remain valid.</summary>
    public void Freeze()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        _frozen = true;
    }
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "FullyQualifiedName~MemTableTests"`
Expected: PASS (11 tests, including the 0 B/op allocation test).

- [ ] **Step 5: Commit**

```bash
git add DataVo.Core/StorageEngine/Lsm/MemTable.cs DataVo.Tests/Lsm/MemTableTests.cs
git commit -m "feat: LSM MemTable Freeze + zero-GC steady-state write loop

Claude-Session: https://claude.ai/code/session_01P9EXib7WpNKu16aZKiveoS"
```

---

### Task 6: Plan-2 gate — full suite green + AOT-clean build

**Files:** none (verification only).

- [ ] **Step 1: Run the full test suite**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj`
Expected: the new `MemTableTests` (11) and the updated `ArenaTests` (13) pass; the only failures are the 3 known pre-existing allocation micro-benchmarks (`CompiledQueryReadAllocationSpikeTests.Spike_PointLookupAllocationBreakdown`, `CompiledAccessPathTests.SelectManyTyped_ReclaimsMaterializationLayer_ScalingWithColumnCount`, `SelectManyTyped_StreamingProjected_PerRowAllocationIsNearMinimal`). No new failures.

- [ ] **Step 2: Confirm the core still builds AOT-clean**

Run: `dotnet build DataVo.Core/DataVo.Core.csproj -c Release`
Expected: `0 Warning(s) 0 Error(s)`.

- [ ] **Step 3: Commit (only if incidental fixes were needed; otherwise skip)**

```bash
git commit -am "chore: Plan 2 gate — LSM MemTable green and AOT-clean

Claude-Session: https://claude.ai/code/session_01P9EXib7WpNKu16aZKiveoS"
```

---

## Carry-forward into Plan 3

- Plan 3's SSTable writer consumes `MemTable.GetEnumerator()` (sorted `MemTableEntry` stream) to write data blocks, and builds the Bloom filter by `Add`-ing `InternalKey.UserKey(entry.InternalKey)` for each entry (bare user key — never the tagged internal key).
- Formal lock-free memory-ordering hardening of the skiplist forward-pointer publication (volatile/acquire-release) is deferred; document it where the engine wires concurrent readers.
- The `MemTable` exposes `Count` and `ApproximateBytes` so Plan 5's flush trigger can decide when to freeze.
