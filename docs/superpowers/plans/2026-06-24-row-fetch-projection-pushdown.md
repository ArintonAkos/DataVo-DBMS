# Row-Fetch Projection Pushdown Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring the compiled typed query path's per-row allocation to its theoretical minimum (projected string cells + result record) by reading rows with an allocation-free span reader, decoding only projected columns, and streaming each row to the mapper with no `StoredRow` and no `Dictionary<long,StoredRow>`.

**Architecture:** A shared `ByteSpanReader` (`ref struct`) replaces the per-row `MemoryStream`+`BinaryReader` in `RowSerializer.DeserializeCells` globally. A new `RowSerializer.DecodeProjectedCells` decodes only projected columns, skip-advancing the rest over the forward-only wire format. `DataVoCompiledQuery.ExecuteSelectTyped` resolves matching row ids via the index/tag/PK path, then streams each MVCC-visible row through `DecodeProjectedCells` → a reused buffer → `CompiledRowReader` → the mapper; the scan path falls back to the existing full-decode finder.

**Tech Stack:** C# / .NET 10 (`DataVo.Core`), xUnit.

**Spec:** `docs/superpowers/specs/2026-06-24-row-fetch-projection-pushdown-design.md`
**Branch:** `feature/roslyn-compile-time-access-path-poc` (already checked out)

## Global Constraints

- **Wire format is unchanged.** `ByteSpanReader` must decode byte-identical to what `BinaryWriter` writes: little-endian `Int32`/`Int64`; `bool` = 1 byte; `string` = 7-bit-encoded length prefix then UTF8. Decode and skip share ONE per-column-type switch so widths never diverge.
- **Column order = wire order.** `context.Engine.Catalog.GetTableColumns(table, db)` returns columns in the same order `RowSerializer` serialized them. Skip-advance depends on this.
- **MVCC parity.** Visibility is checked per row *before* decoding, identical in effect to `ApplyTypedMvccVisibilityFilter`: `MvccExecutionScope.CurrentSnapshot == null` ⇒ visible; else `SnapshotVisibilityEvaluator.IsVersionVisible(MvccCoordinator.EnsureRowVersionExists(engine, db, table, rowId), snapshot)`.
- **Scan fallback preserved.** When no index path resolves, the typed path uses the existing `TryReadMatchingStoredRows` full-decode finder + `CompiledRowReader` (unchanged behavior).
- **Type→cell map (matches `RowSerializer.ReadTypedCell`):** INT→`Int32`, FLOAT→`Int32` bits→`(double)Single`, BIT→`bool`, DATE→`Int64`→`DateTime.FromBinary`→`DateOnly`, VECTOR→`Int32` count + N×`Int32` bits, default(VARCHAR)→`string`.
- **Quality bars:** 0 new warnings (Core has CS1591 doc enforcement — document new public members), AOT-clean (span reader + ref struct + typed `CellValue`; no reflection), full suite green.

## File Structure

**Create:**
- `DataVo.Core/StorageEngine/Serialization/ByteSpanReader.cs` — allocation-free forward reader over `ReadOnlySpan<byte>`.
- `DataVo.Tests/Storage/RowSerializerSpanTests.cs` — round-trip + projection decode tests.

**Modify:**
- `DataVo.Core/StorageEngine/Serialization/RowSerializer.cs` — rewrite `DeserializeCells` on `ByteSpanReader`; add `DecodeProjectedCells` + `DecodeTypedCell`/`SkipTypedCell`.
- `DataVo.Core/StorageEngine/StorageContext.cs` — add `TryReadRowBytes` + `IsRowVisible`.
- `DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs` — add `TryResolveMatchingRowIds`; stream projected rows in `ExecuteSelectTyped`.
- `DataVo.Tests/E2E/CompiledAccessPathTests.cs` — projected-streaming parity, MVCC, and allocation tests.

---

### Task 1: `ByteSpanReader` + rewrite `DeserializeCells`

**Files:**
- Create: `DataVo.Core/StorageEngine/Serialization/ByteSpanReader.cs`
- Modify: `DataVo.Core/StorageEngine/Serialization/RowSerializer.cs` (`DeserializeCells`, ~line 182)
- Test: `DataVo.Tests/Storage/RowSerializerSpanTests.cs` (create)

**Interfaces:**
- Produces: `internal ref struct ByteSpanReader` with `ByteSpanReader(ReadOnlySpan<byte>)`, `bool ReadBoolean()`, `int ReadInt32()`, `long ReadInt64()`, `string ReadString()`, `void SkipString()`, `void Skip(int byteCount)`. `RowSerializer.DeserializeCells(byte[], IReadOnlyList<Column>)` keeps its signature, now span-based.

- [ ] **Step 1: Write the failing test**

Create `DataVo.Tests/Storage/RowSerializerSpanTests.cs`:

```csharp
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Serialization;

namespace DataVo.Tests.Storage;

public class RowSerializerSpanTests
{
    private static List<Column> Schema() =>
    [
        new Column { Name = "Id", Type = "INT" },
        new Column { Name = "Name", Type = "VARCHAR" },
        new Column { Name = "Score", Type = "FLOAT" },
        new Column { Name = "Active", Type = "BIT" },
        new Column { Name = "Day", Type = "DATE" },
        new Column { Name = "Note", Type = "VARCHAR" },
    ];

    [Fact]
    public void DeserializeCells_RoundTripsEveryType_IncludingUtf8AndNull()
    {
        List<Column> columns = Schema();
        CellValue[] original =
        [
            CellValue.From(42),
            CellValue.From("héllo wörld"),     // multi-byte UTF8
            CellValue.From((double)1.5f),       // FLOAT stored as single-bits
            CellValue.From(true),
            CellValue.From(new DateOnly(2026, 6, 24)),
            CellValue.Null,                     // NULL string
        ];

        byte[] bytes = RowSerializer.SerializeCells(columns, original);
        CellValue[] decoded = RowSerializer.DeserializeCells(bytes, columns);

        Assert.Equal(42, decoded[0].AsInt32());
        Assert.Equal("héllo wörld", decoded[1].AsString());
        Assert.Equal(1.5, decoded[2].AsDouble(), 3);
        Assert.True(decoded[3].AsBoolean());
        Assert.Equal(new DateOnly(2026, 6, 24), decoded[4].AsDate());
        Assert.True(decoded[5].IsNull);
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj -c Debug --filter "FullyQualifiedName~RowSerializerSpanTests"`
Expected: PASS today (the current `DeserializeCells` already round-trips). This test is the **regression guard** for the rewrite — it must stay green after Step 3. (If it fails now, the `Column`/`CellValue` usings are wrong — fix before continuing.)

- [ ] **Step 3: Create `ByteSpanReader`**

Create `DataVo.Core/StorageEngine/Serialization/ByteSpanReader.cs`:

```csharp
using System.Buffers.Binary;
using System.Text;

namespace DataVo.Core.StorageEngine.Serialization;

/// <summary>
/// An allocation-free forward reader over a row's serialized bytes, decoding byte-identical to the format
/// <see cref="System.IO.BinaryWriter"/> writes: little-endian primitives, a 1-byte boolean, and a
/// 7-bit-length-prefixed UTF8 string. Replaces the per-row MemoryStream + BinaryReader in the hot read path.
/// </summary>
internal ref struct ByteSpanReader
{
    private readonly ReadOnlySpan<byte> _data;
    private int _position;

    public ByteSpanReader(ReadOnlySpan<byte> data)
    {
        _data = data;
        _position = 0;
    }

    public bool ReadBoolean() => _data[_position++] != 0;

    public int ReadInt32()
    {
        int value = BinaryPrimitives.ReadInt32LittleEndian(_data.Slice(_position, sizeof(int)));
        _position += sizeof(int);
        return value;
    }

    public long ReadInt64()
    {
        long value = BinaryPrimitives.ReadInt64LittleEndian(_data.Slice(_position, sizeof(long)));
        _position += sizeof(long);
        return value;
    }

    public string ReadString()
    {
        int length = Read7BitEncodedInt();
        string value = Encoding.UTF8.GetString(_data.Slice(_position, length));
        _position += length;
        return value;
    }

    public void SkipString() => _position += Read7BitEncodedInt();

    public void Skip(int byteCount) => _position += byteCount;

    // Matches BinaryReader.Read7BitEncodedInt / BinaryWriter's length prefix (LEB128, max 5 bytes).
    private int Read7BitEncodedInt()
    {
        int result = 0;
        int shift = 0;
        byte current;
        do
        {
            current = _data[_position++];
            result |= (current & 0x7F) << shift;
            shift += 7;
        }
        while ((current & 0x80) != 0);

        return result;
    }
}
```

- [ ] **Step 4: Rewrite `DeserializeCells` on `ByteSpanReader`**

In `DataVo.Core/StorageEngine/Serialization/RowSerializer.cs`, replace the `DeserializeCells` method body:

```csharp
    public static CellValue[] DeserializeCells(byte[] data, IReadOnlyList<Column> columns)
    {
        var cells = new CellValue[columns.Count];
        var reader = new ByteSpanReader(data);

        for (int i = 0; i < columns.Count; i++)
        {
            cells[i] = reader.ReadBoolean() ? CellValue.Null : DecodeTypedCell(ref reader, columns[i]);
        }

        return cells;
    }

    /// <summary>Decodes one non-null typed cell from the span reader (mirrors <see cref="ReadTypedCell"/>).</summary>
    private static CellValue DecodeTypedCell(ref ByteSpanReader reader, Column column)
    {
        switch (column.Type.ToUpperInvariant())
        {
            case "INT":
                return CellValue.From(reader.ReadInt32());
            case "FLOAT":
                return CellValue.From((double)BitConverter.Int32BitsToSingle(reader.ReadInt32()));
            case "BIT":
                return CellValue.From(reader.ReadBoolean());
            case "DATE":
                return CellValue.From(DateOnly.FromDateTime(DateTime.FromBinary(reader.ReadInt64())));
            case "VECTOR":
            {
                int count = reader.ReadInt32();
                float[] vector = new float[count];
                for (int i = 0; i < count; i++)
                {
                    vector[i] = BitConverter.Int32BitsToSingle(reader.ReadInt32());
                }

                return CellValue.From(vector);
            }
            default:
                return CellValue.From(reader.ReadString());
        }
    }

    /// <summary>Advances the reader past one non-null typed cell without materializing it.</summary>
    private static void SkipTypedCell(ref ByteSpanReader reader, Column column)
    {
        switch (column.Type.ToUpperInvariant())
        {
            case "INT":
            case "FLOAT":
                reader.Skip(sizeof(int));
                return;
            case "BIT":
                reader.Skip(sizeof(bool));
                return;
            case "DATE":
                reader.Skip(sizeof(long));
                return;
            case "VECTOR":
                reader.Skip(reader.ReadInt32() * sizeof(int));
                return;
            default:
                reader.SkipString();
                return;
        }
    }
```

(Leave the old `ReadTypedCell` method in place — `Deserialize` (dict path) still uses it via `ReadNonNullValue`/`BinaryReader`. `SkipTypedCell` is consumed by Task 2.)

- [ ] **Step 5: Run the round-trip test + the typed compiled-query suites (regression)**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj -c Debug --filter "FullyQualifiedName~RowSerializerSpanTests|FullyQualifiedName~CompiledAccessPathTests|FullyQualifiedName~SourceGeneratedCompiledQueryTests"`
Expected: PASS — the round-trip proves the span reader matches the writer; the compiled-query suites prove the typed read path (which calls `DeserializeCells`) is unchanged in behavior.

- [ ] **Step 6: Commit**

```bash
git add DataVo.Core/StorageEngine/Serialization/ByteSpanReader.cs DataVo.Core/StorageEngine/Serialization/RowSerializer.cs DataVo.Tests/Storage/RowSerializerSpanTests.cs
git commit -m "perf(storage): allocation-free ByteSpanReader; DeserializeCells drops per-row MemoryStream/BinaryReader

Claude-Session: https://claude.ai/code/session_01QkLBxzCs27vvcFG2si5Vg2"
```

---

### Task 2: `DecodeProjectedCells` (projection pushdown)

**Files:**
- Modify: `DataVo.Core/StorageEngine/Serialization/RowSerializer.cs`
- Test: `DataVo.Tests/Storage/RowSerializerSpanTests.cs`

**Interfaces:**
- Consumes: `ByteSpanReader`, `DecodeTypedCell`, `SkipTypedCell` (Task 1).
- Produces: `public static void RowSerializer.DecodeProjectedCells(ReadOnlySpan<byte> data, IReadOnlyList<Column> columns, ReadOnlySpan<bool> isProjected, Span<CellValue> destination)` — writes the projected columns, in storage order, into `destination[0..projectedCount)`; skip-advances the rest.

- [ ] **Step 1: Write the failing test**

Add to `DataVo.Tests/Storage/RowSerializerSpanTests.cs`:

```csharp
    [Fact]
    public void DecodeProjectedCells_DecodesOnlyProjected_SkippingTheRest()
    {
        List<Column> columns = Schema(); // Id, Name, Score, Active, Day, Note
        CellValue[] full =
        [
            CellValue.From(7),
            CellValue.From("skip-me"),         // not projected (a string before a projected column)
            CellValue.From((double)2.5f),
            CellValue.From(false),             // not projected
            CellValue.From(new DateOnly(2026, 1, 2)),
            CellValue.From("note!"),
        ];
        byte[] bytes = RowSerializer.SerializeCells(columns, full);

        // Project Id, Score, Day, Note (skip Name and Active — including a skipped string).
        bool[] isProjected = [true, false, true, false, true, true];
        var dest = new CellValue[4];

        RowSerializer.DecodeProjectedCells(bytes, columns, isProjected, dest);

        Assert.Equal(7, dest[0].AsInt32());                       // Id
        Assert.Equal(2.5, dest[1].AsDouble(), 3);                 // Score
        Assert.Equal(new DateOnly(2026, 1, 2), dest[2].AsDate()); // Day
        Assert.Equal("note!", dest[3].AsString());                // Note
    }

    [Fact]
    public void DecodeProjectedCells_ProjectedNull_IsNull()
    {
        List<Column> columns = [new Column { Name = "Id", Type = "INT" }, new Column { Name = "Note", Type = "VARCHAR" }];
        byte[] bytes = RowSerializer.SerializeCells(columns, [CellValue.From(1), CellValue.Null]);

        bool[] isProjected = [false, true];
        var dest = new CellValue[1];
        RowSerializer.DecodeProjectedCells(bytes, columns, isProjected, dest);

        Assert.True(dest[0].IsNull);
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj -c Debug --filter "FullyQualifiedName~DecodeProjectedCells"`
Expected: COMPILE FAILURE — `RowSerializer.DecodeProjectedCells` does not exist.

- [ ] **Step 3: Implement `DecodeProjectedCells`**

In `RowSerializer.cs`, add:

```csharp
    /// <summary>
    /// Decodes only the columns flagged in <paramref name="isProjected"/> into <paramref name="destination"/>
    /// (in storage order), advancing past the rest without allocating. <paramref name="destination"/> must have
    /// room for the number of projected columns. The forward-only wire format is walked once.
    /// </summary>
    public static void DecodeProjectedCells(
        ReadOnlySpan<byte> data,
        IReadOnlyList<Column> columns,
        ReadOnlySpan<bool> isProjected,
        Span<CellValue> destination)
    {
        var reader = new ByteSpanReader(data);
        int next = 0;

        for (int i = 0; i < columns.Count; i++)
        {
            bool isNull = reader.ReadBoolean();
            if (isProjected[i])
            {
                destination[next++] = isNull ? CellValue.Null : DecodeTypedCell(ref reader, columns[i]);
            }
            else if (!isNull)
            {
                SkipTypedCell(ref reader, columns[i]);
            }
        }
    }
```

- [ ] **Step 4: Run to verify it passes**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj -c Debug --filter "FullyQualifiedName~DecodeProjectedCells"`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add DataVo.Core/StorageEngine/Serialization/RowSerializer.cs DataVo.Tests/Storage/RowSerializerSpanTests.cs
git commit -m "perf(storage): DecodeProjectedCells decodes only projected columns, skips the rest

Claude-Session: https://claude.ai/code/session_01QkLBxzCs27vvcFG2si5Vg2"
```

---

### Task 3: `StorageContext` row-fetch primitives

**Files:**
- Modify: `DataVo.Core/StorageEngine/StorageContext.cs`

**Interfaces:**
- Produces: `internal byte[]? TryReadRowBytes(string tableName, string databaseName, long rowId)` (the stored bytes, or null if deleted/missing); `internal bool IsRowVisible(string tableName, string databaseName, long rowId)` (per-row MVCC visibility).

- [ ] **Step 1: Add the primitives**

In `DataVo.Core/StorageEngine/StorageContext.cs`, ensure `using DataVo.Core.Exceptions;` is present (add if missing), then add these methods to the class (e.g., after `GetTypedTableContents`):

```csharp
    /// <summary>
    /// Returns the stored serialized bytes for one row (in-memory: the stored reference, no copy), or
    /// <c>null</c> when the row was deleted or does not exist.
    /// </summary>
    internal byte[]? TryReadRowBytes(string tableName, string databaseName, long rowId)
    {
        try
        {
            return _storageEngine.ReadRow(databaseName, tableName, rowId);
        }
        catch (RowDeletedException)
        {
            return null;
        }
        catch (RowNotFoundException)
        {
            return null;
        }
    }

    /// <summary>
    /// Per-row MVCC visibility, identical in effect to <see cref="ApplyTypedMvccVisibilityFilter"/>: visible when
    /// there is no active snapshot, otherwise gated by the row version's snapshot visibility.
    /// </summary>
    internal bool IsRowVisible(string tableName, string databaseName, long rowId)
    {
        TransactionSnapshot? snapshot = MvccExecutionScope.CurrentSnapshot;
        if (snapshot == null)
        {
            return true;
        }

        DataVoEngine engine = DataVoEngine.Current();
        RowVersion version = MvccCoordinator.EnsureRowVersionExists(engine, databaseName, tableName, rowId);
        return SnapshotVisibilityEvaluator.IsVersionVisible(version, snapshot);
    }
```

- [ ] **Step 2: Build Core to confirm it compiles**

Run: `dotnet build DataVo.Core/DataVo.Core.csproj -c Debug 2>&1 | grep -E 'error|Build succeeded' | head`
Expected: `Build succeeded.` (The MVCC types `TransactionSnapshot`/`MvccExecutionScope`/`MvccCoordinator`/`RowVersion`/`SnapshotVisibilityEvaluator` are already used by `ApplyTypedMvccVisibilityFilter` in this file, so they are in scope.)

- [ ] **Step 3: Commit**

```bash
git add DataVo.Core/StorageEngine/StorageContext.cs
git commit -m "feat(storage): TryReadRowBytes + per-row IsRowVisible primitives for streaming reads

Claude-Session: https://claude.ai/code/session_01QkLBxzCs27vvcFG2si5Vg2"
```

---

### Task 4: Stream projected rows in `ExecuteSelectTyped`

**Files:**
- Modify: `DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs`
- Test: `DataVo.Tests/E2E/CompiledAccessPathTests.cs`

**Interfaces:**
- Consumes: `RowSerializer.DecodeProjectedCells` (Task 2); `StorageContext.TryReadRowBytes`/`IsRowVisible` (Task 3); `ReactiveRowSchema(IReadOnlyList<string>)`, `StoredRowView`, `CompiledRowReader`, `context.Engine.Catalog.GetTableColumns`, `FilterUsingIndex`, `TryResolveSingleColumnIndex`, `IsMissingPrimaryKeyIndex`, `TryReadMatchingStoredRows`.
- Produces: `ExecuteSelectTyped<T>` streams index-resolved rows with projection pushdown; falls back to the full-decode finder for the scan path. Adds `private static List<long>? TryResolveMatchingRowIds(...)`.

- [ ] **Step 1: Write the failing tests**

Append to `DataVo.Tests/E2E/CompiledAccessPathTests.cs`:

```csharp
    [Fact]
    public void SelectManyTyped_StreamsProjected_WideRow_MatchesDictPath()
    {
        using var context = CreateContext();
        // Wide row, narrow projection: only Id/Tag/Score are projected; C1..C5 must be skipped, not decoded.
        context.Execute("CREATE TABLE Wide (Id INT PRIMARY KEY, Tag VARCHAR(20), Score FLOAT, C1 VARCHAR(20), C2 VARCHAR(20), C3 VARCHAR(20), C4 VARCHAR(20), C5 VARCHAR(20))");
        context.BulkInsert(
            "Wide",
            [
                new Dictionary<string, object?> { ["Id"] = 1, ["Tag"] = "x", ["Score"] = 1.5, ["C1"] = "a", ["C2"] = "b", ["C3"] = "c", ["C4"] = "d", ["C5"] = "e" },
                new Dictionary<string, object?> { ["Id"] = 2, ["Tag"] = "x", ["Score"] = 2.5, ["C1"] = "a", ["C2"] = "b", ["C3"] = "c", ["C4"] = "d", ["C5"] = "e" }
            ]);
        context.Execute("CREATE INDEX ix_wide_tag ON Wide (Tag)");

        IReadOnlyList<Hit> typed = DataVoCompiledQuery.SelectManyTyped<Hit>(
            context,
            DataVoCompiledQueryPlan.SelectMany("Wide", ["Id", "Tag", "Score"], "Tag", "tag",
                accessPath: CompiledAccessPath.SingleColumnIndex, resolvedIndexName: "ix_wide_tag"),
            [new DataVoCompiledQueryParameter("tag", "x")],
            static r => new Hit(r.GetInt32("Id"), r.GetString("Tag")!, r.GetDouble("Score")));

        Assert.Equal(
            new[] { new Hit(1, "x", 1.5), new Hit(2, "x", 2.5) },
            typed.OrderBy(h => h.Id));
    }

    [Fact]
    public void SelectSingleTyped_ByPrimaryKey_StreamsProjected()
    {
        using var context = CreateContext();
        SeedHits(context); // Hits(Id INT PK, Name VARCHAR, Score FLOAT), 3 rows

        Hit? hit = DataVoCompiledQuery.SelectSingleTyped<Hit>(
            context,
            DataVoCompiledQueryPlan.SelectSingle("Hits", ["Id", "Name", "Score"], "Id", "id"),
            [new DataVoCompiledQueryParameter("id", 2)],
            MapHit);

        Assert.Equal(new Hit(2, "Grace", 2.5), hit);
    }

    [Fact]
    public void SelectManyTyped_NullProjectedColumn_StreamsNull()
    {
        using var context = CreateContext();
        context.Execute("CREATE TABLE Notes (Id INT PRIMARY KEY, Tag VARCHAR(20), Body VARCHAR(50))");
        context.BulkInsert(
            "Notes",
            [
                new Dictionary<string, object?> { ["Id"] = 1, ["Tag"] = "n", ["Body"] = null }
            ]);
        context.Execute("CREATE INDEX ix_notes_tag ON Notes (Tag)");

        IReadOnlyList<(int, string?)> rows = DataVoCompiledQuery.SelectManyTyped<(int, string?)>(
            context,
            DataVoCompiledQueryPlan.SelectMany("Notes", ["Id", "Body"], "Tag", "tag",
                accessPath: CompiledAccessPath.SingleColumnIndex, resolvedIndexName: "ix_notes_tag"),
            [new DataVoCompiledQueryParameter("tag", "n")],
            static r => (r.GetInt32("Id"), r.GetString("Body")));

        Assert.Equal((1, (string?)null), Assert.Single(rows));
    }

    [Fact]
    public void SelectManyTyped_ScanFallback_UnindexedColumn_StillReturnsRows()
    {
        using var context = CreateContext();
        SeedHits(context); // no index on Name

        IReadOnlyList<Hit> rows = DataVoCompiledQuery.SelectManyTyped<Hit>(
            context,
            DataVoCompiledQueryPlan.SelectMany("Hits", ["Id", "Name", "Score"], "Name", "name"),
            [new DataVoCompiledQueryParameter("name", "Ada")],
            MapHit);

        Assert.Equal(
            new[] { new Hit(1, "Ada", 1.5), new Hit(3, "Ada", 3.5) },
            rows.OrderBy(h => h.Id));
    }
```

- [ ] **Step 2: Run to verify they fail**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj -c Debug --filter "FullyQualifiedName~CompiledAccessPathTests.SelectManyTyped_StreamsProjected_WideRow_MatchesDictPath"`
Expected: PASS today (current `ExecuteSelectTyped` returns correct rows via the full-decode path). These tests pin the *behavior* the streaming rewrite must preserve; they stay green through Step 3-4. The new behavior (streaming, no full StoredRow) is proven by the allocation test in Task 5.

- [ ] **Step 3: Add the row-id resolver**

In `DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs`, add (next to `TryReadMatchingStoredRows`):

```csharp
    // Returns the matching row ids when an index access path resolves them (compile-time tag, then primary key,
    // then a single-column secondary index); returns null to signal the caller should use the full-decode scan
    // path. Mirrors the fall-through behavior of TryReadMatchingStoredRows (empty result or IndexException on an
    // index path falls through to scan).
    private static List<long>? TryResolveMatchingRowIds(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        string databaseName,
        string expectedKey)
    {
        if (plan.AccessPath == CompiledAccessPath.SingleColumnIndex && plan.ResolvedIndexName is not null)
        {
            try
            {
                var ids = new List<long>(context.Engine.IndexManager.FilterUsingIndex(expectedKey, plan.ResolvedIndexName, plan.TableName, databaseName));
                if (ids.Count > 0)
                {
                    return ids;
                }
            }
            catch (IndexException)
            {
            }
        }

        List<string> primaryKeys = context.Engine.Catalog.GetTablePrimaryKeys(plan.TableName, databaseName);
        if (primaryKeys.Contains(plan.WhereColumn!, StringComparer.OrdinalIgnoreCase))
        {
            string primaryKeyIndexName = $"_PK_{plan.TableName}";
            try
            {
                var ids = new List<long>(context.Engine.IndexManager.FilterUsingIndex(expectedKey, primaryKeyIndexName, plan.TableName, databaseName));
                if (ids.Count > 0)
                {
                    return ids;
                }
            }
            catch (IndexException ex) when (IsMissingPrimaryKeyIndex(ex, primaryKeyIndexName, plan.TableName))
            {
            }
        }
        else if (TryResolveSingleColumnIndex(context, plan.TableName, databaseName, plan.WhereColumn!, out string secondaryIndexName))
        {
            try
            {
                var ids = new List<long>(context.Engine.IndexManager.FilterUsingIndex(expectedKey, secondaryIndexName, plan.TableName, databaseName));
                if (ids.Count > 0)
                {
                    return ids;
                }
            }
            catch (IndexException)
            {
            }
        }

        return null;
    }
```

- [ ] **Step 4: Stream projected rows in `ExecuteSelectTyped`**

In the same file, replace the body of `ExecuteSelectTyped<T>` with:

```csharp
        string databaseName = ResolveCurrentDatabase(context);
        Dictionary<string, object?> parameterDictionary = ToParameterDictionary(parameters);
        object? expected = RequiredParameter(parameterDictionary, plan.WhereParameterName!);
        string expectedKey = BuildComparisonKey(plan.WhereColumn!, expected);

        List<long>? rowIds = TryResolveMatchingRowIds(context, plan, databaseName, expectedKey);
        if (rowIds is null)
        {
            // Scan fallback: no index path resolved — reuse the full-decode finder + reader (unchanged behavior).
            List<KeyValuePair<long, StoredRow>> scanned =
                TryReadMatchingStoredRows(context, plan, databaseName, expectedKey);
            var scannedResults = new T[scanned.Count];
            for (int i = 0; i < scanned.Count; i++)
            {
                scannedResults[i] = mapper(new CompiledRowReader(scanned[i].Value.AsView()));
            }

            return scannedResults;
        }

        // Streaming projection pushdown: decode only the projected columns of each visible row into a reused
        // buffer, mapping straight to T — no StoredRow, no dictionary, no decode of unprojected columns.
        IReadOnlyList<Column> columns = context.Engine.Catalog.GetTableColumns(plan.TableName, databaseName);
        bool[] isProjected = new bool[columns.Count];
        var projectedNames = new List<string>(plan.ProjectedColumns.Count);
        for (int i = 0; i < columns.Count; i++)
        {
            if (ContainsIgnoreCase(plan.ProjectedColumns, columns[i].Name))
            {
                isProjected[i] = true;
                projectedNames.Add(columns[i].Name);
            }
        }

        var projectedSchema = new ReactiveRowSchema(projectedNames);
        var buffer = new CellValue[projectedNames.Count];

        var results = new List<T>(rowIds.Count);
        foreach (long rowId in rowIds)
        {
            if (!context.Engine.StorageContext.IsRowVisible(plan.TableName, databaseName, rowId))
            {
                continue;
            }

            byte[]? bytes = context.Engine.StorageContext.TryReadRowBytes(plan.TableName, databaseName, rowId);
            if (bytes is null)
            {
                continue;
            }

            RowSerializer.DecodeProjectedCells(bytes, columns, isProjected, buffer);
            results.Add(mapper(new CompiledRowReader(new StoredRowView(projectedSchema, buffer))));
        }

        return results;
    }

    private static bool ContainsIgnoreCase(IReadOnlyList<string> values, string candidate)
    {
        for (int i = 0; i < values.Count; i++)
        {
            if (string.Equals(values[i], candidate, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }
```

Add the required usings at the top of `DataVoCompiledQuery.cs` if not present: `using DataVo.Core.Models.Catalog;` (for `Column`), `using DataVo.Core.Runtime.Reactive;` (for `ReactiveRowSchema`, `CellValue`), `using DataVo.Core.StorageEngine.Serialization;` (for `RowSerializer`). `StoredRow`/`StoredRowView` are already used via `using DataVo.Core.StorageEngine;`.

- [ ] **Step 5: Run the parity tests + the full compiled-query suites**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj -c Debug --filter "FullyQualifiedName~CompiledAccessPathTests|FullyQualifiedName~SourceGeneratedCompiledQueryTests"`
Expected: PASS — streamed projected results equal the dict path; scan fallback intact; NULL handled; source-generated typed queries still correct.

- [ ] **Step 6: Commit**

```bash
git add DataVo.Core/CompiledQueries/DataVoCompiledQuery.cs DataVo.Tests/E2E/CompiledAccessPathTests.cs
git commit -m "perf(query): stream index-resolved typed rows with projection pushdown (no StoredRow, no dict)

Claude-Session: https://claude.ai/code/session_01QkLBxzCs27vvcFG2si5Vg2"
```

---

### Task 5: MVCC parity + allocation proof

**Files:**
- Test: `DataVo.Tests/E2E/CompiledAccessPathTests.cs`

**Interfaces:**
- Consumes: streaming `ExecuteSelectTyped` (Task 4); existing `MapHit`/`SeedHits`/`Hit`/`PerRow` helpers.

- [ ] **Step 1: Write the MVCC parity + allocation tests**

Append to `DataVo.Tests/E2E/CompiledAccessPathTests.cs`:

```csharp
    [Fact]
    public void SelectManyTyped_UnderSnapshot_MatchesDictPathVisibility()
    {
        using var context = CreateContext();
        SeedHits(context);
        context.Execute("CREATE INDEX ix_hits_name ON Hits (Name)");

        // A read snapshot in an open transaction sees the committed rows; typed (streaming) must match dict.
        context.Execute("BEGIN TRANSACTION");
        try
        {
            IReadOnlyList<Hit> typed = DataVoCompiledQuery.SelectManyTyped<Hit>(
                context,
                DataVoCompiledQueryPlan.SelectMany("Hits", ["Id", "Name", "Score"], "Name", "name",
                    accessPath: CompiledAccessPath.SingleColumnIndex, resolvedIndexName: "ix_hits_name"),
                [new DataVoCompiledQueryParameter("name", "Ada")],
                MapHit);

            IReadOnlyList<Hit> dict = DataVoCompiledQuery.SelectMany<Hit>(
                context,
                DataVoCompiledQueryPlan.SelectMany("Hits", ["Id", "Name", "Score"], "Name", "name"),
                [new DataVoCompiledQueryParameter("name", "Ada")],
                static row => new Hit((int)row["Id"]!, (string)row["Name"]!, (double)row["Score"]!));

            Assert.Equal(dict.OrderBy(h => h.Id), typed.OrderBy(h => h.Id));
        }
        finally
        {
            context.Execute("ROLLBACK");
        }
    }

    [Fact]
    public void SelectManyTyped_StreamingProjected_PerRowAllocationIsNearMinimal()
    {
        const int iterations = 2_000;
        using var context = CreateContext();
        // Wide row (8 cols), narrow projection (3). After projection pushdown the typed per-row allocation is
        // just the projected string (Tag) + the Hit record — the ~1,456 B/row deserialization slice is gone.
        context.Execute("CREATE TABLE Bench (Id INT PRIMARY KEY, Tag VARCHAR(20), Score FLOAT, C1 VARCHAR(20), C2 VARCHAR(20), C3 VARCHAR(20), C4 VARCHAR(20), C5 VARCHAR(20))");
        Dictionary<string, object?> Row(int id, string tag) => new()
        {
            ["Id"] = id, ["Tag"] = tag, ["Score"] = id + 0.5,
            ["C1"] = "c1", ["C2"] = "c2", ["C3"] = "c3", ["C4"] = "c4", ["C5"] = "c5"
        };
        var seed = new List<Dictionary<string, object?>> { Row(1, "m1") };
        for (int i = 0; i < 8; i++)
        {
            seed.Add(Row(i + 2, "m8"));
        }
        context.BulkInsert("Bench", seed);
        context.Execute("CREATE INDEX ix_bench_tag ON Bench (Tag)");

        var plan = DataVoCompiledQueryPlan.SelectMany(
            "Bench", ["Id", "Tag", "Score"], "Tag", "tag",
            accessPath: CompiledAccessPath.SingleColumnIndex, resolvedIndexName: "ix_bench_tag");

        IReadOnlyList<Hit> Typed(string tag)
            => DataVoCompiledQuery.SelectManyTyped<Hit>(context, plan, [new DataVoCompiledQueryParameter("tag", tag)],
                static r => new Hit(r.GetInt32("Id"), r.GetString("Tag")!, r.GetDouble("Score")));

        double typedPerRow = PerRow(() => Typed("m1"), () => Typed("m8"), iterations);
        _output.WriteLine($"streaming typed per-row : {typedPerRow:F0} B/row");

        // Pre-pushdown this path was ~1,386 B/row; with projection pushdown it must fall far below the old
        // deserialization-dominated cost. Budget is generous to stay robust across runtimes.
        Assert.True(typedPerRow < 400, $"Expected streaming typed per-row well under 400 B/row, got {typedPerRow:F0}.");
    }
```

- [ ] **Step 2: Run the tests (Release, with output for the allocation number)**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj -c Release --filter "FullyQualifiedName~CompiledAccessPathTests.SelectManyTyped_UnderSnapshot_MatchesDictPathVisibility|FullyQualifiedName~CompiledAccessPathTests.SelectManyTyped_StreamingProjected_PerRowAllocationIsNearMinimal" --logger "console;verbosity=detailed" 2>&1 | grep -E 'per-row|Passed!|Failed!|\[FAIL\]'`
Expected: PASS — MVCC parity holds; the printed `streaming typed per-row` is well under 400 B/row (down from ~1,386). If the allocation number is above 400, do NOT relax the budget — investigate whether an unprojected column is still being decoded (Task 2 skip) or whether the buffer/schema is being rebuilt per row (Task 4 per-query setup).

- [ ] **Step 3: Commit**

```bash
git add DataVo.Tests/E2E/CompiledAccessPathTests.cs
git commit -m "test(query): MVCC parity + near-minimal per-row allocation proof for streaming projected reads

Claude-Session: https://claude.ai/code/session_01QkLBxzCs27vvcFG2si5Vg2"
```

---

### Task 6: Full-suite verification

**Files:** none (verification + final report only).

- [ ] **Step 1: Build (warning-clean for new code)**

Run: `dotnet build DataVo.sln -c Release 2>&1 | grep -iE 'warning|error|Build succeeded' | grep -ivE 'xUnit2017'`
Expected: `Build succeeded.` with no new warnings. (`xUnit2017` in `TableValidationMetadataCacheTests` is pre-existing and unrelated.) If a CS1591 appears, document the new public member (`DecodeProjectedCells`).

- [ ] **Step 2: Full suite**

Run: `dotnet test DataVo.sln -c Release --nologo 2>&1 | grep -E 'Passed!|Failed!|Passed:|Failed:'`
Expected: all green; counts grown by the new tests; zero failures.

- [ ] **Step 3: AOT smoke**

Run: `dotnet publish DataVo.AotSmoke/DataVo.AotSmoke.csproj -c Release -r osx-arm64 --nologo 2>&1 | grep -iE 'IL[0-9]{4}|warning|error|Generating native' | head`
Expected: `Generating native code`, no IL warnings. Then run `./DataVo.AotSmoke/bin/Release/net10.0/DataVo.AotSmoke` → `ALL SMOKE CHECKS PASSED` (exit 0).

- [ ] **Step 4: Clean status**

Run: `git status -s`
Expected: only the pre-existing untracked `.DS_Store`/`test.md`. Do **not** merge — the user keeps the branch.

## Self-Review

**1. Spec coverage:**
- `ByteSpanReader` (allocation-free, BinaryWriter-compatible incl. 7-bit string length) → Task 1. ✓
- `DeserializeCells` rewritten on it (global removal of MemoryStream/BinaryReader) → Task 1. ✓
- `DecodeProjectedCells` (decode projected, skip-advance rest, shared type switch) → Task 2. ✓
- `StorageContext` `TryReadRowBytes` + `IsRowVisible` → Task 3. ✓
- `TryResolveMatchingRowIds` + streaming projected `ExecuteSelectTyped` (no dict/StoredRow), scan fallback → Task 4. ✓
- Wire-format fidelity test → Task 1 Step 1; exact-skip test → Task 2 Step 1; MVCC parity → Task 5; allocation proof → Task 5. ✓
- 0 warnings / AOT / full suite → Task 6. ✓
- Scope out (dict Deserialize, disk pooling, scan projection, Layer 2) — untouched: `Deserialize`/`ReadTypedCell` left intact, scan uses the full-decode fallback. ✓

**2. Placeholder scan:** No TBD/TODO/"handle edge cases" — every code step has complete code; the Task-5 budget note is an explicit investigate-don't-relax instruction.

**3. Type consistency:** `ByteSpanReader` (`ReadBoolean/ReadInt32/ReadInt64/ReadString/SkipString/Skip`), `DecodeTypedCell`/`SkipTypedCell`, `DecodeProjectedCells(ReadOnlySpan<byte>, IReadOnlyList<Column>, ReadOnlySpan<bool>, Span<CellValue>)`, `TryReadRowBytes`/`IsRowVisible`, `TryResolveMatchingRowIds`, `ReactiveRowSchema(IReadOnlyList<string>)`, `StoredRowView(ReactiveRowSchema, ReadOnlySpan<CellValue>)`, `CompiledRowReader(StoredRowView)`, `context.Engine.Catalog.GetTableColumns` are referenced identically across tasks. The wire type→cell switch in Task 1 (`DecodeTypedCell`/`SkipTypedCell`) matches `RowSerializer.ReadTypedCell` exactly.
