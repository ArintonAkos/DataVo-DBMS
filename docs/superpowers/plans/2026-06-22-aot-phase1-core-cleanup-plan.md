# Native AOT — Phase 1 Core Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans (or
> superpowers:subagent-driven-development) to implement this plan task-by-task. Steps use checkbox
> (`- [ ]`) syntax. Design: [`../specs/2026-06-22-native-aot-phase1-design.md`](../specs/2026-06-22-native-aot-phase1-design.md).

**Goal:** Drive `DataVo.Core`'s AOT/trim warning baseline from **184 → 0** so the engine core publishes and
runs as a Native AOT binary, then lock it like `DataVo.Data`. The end state: the `DataVo.AotSmoke` native
binary prints `ALL SMOKE CHECKS PASSED`.

**Architecture:** Five independent targets, ordered by impact × independence. Targets 1–3 remove the
reflection/dynamic-code **serializers** (XmlSerializer → reflection-free `System.Xml.Linq`; Newtonsoft and
reflection-mode `System.Text.Json` → STJ **source generator**). Target 4 removes `dynamic`/DLR from the
SQL evaluators and joins (→ typed `CellValue`/`object`). Target 5 removes `Activator.CreateInstance` from
the aggregation factory. After each target the ratchet baseline in `scripts/check-aot-baseline.sh` is
lowered to the new measured count.

**Tech Stack:** C# / .NET 10, `System.Text.Json` source generation (`JsonSerializerContext`), `System.Xml.Linq`,
xUnit. AOT/trim analyzers (`IsAotCompatible`) provide the warning baseline; `dotnet publish /p:PublishAot=true`
of `DataVo.AotSmoke` is the end-to-end gate.

## Global Constraints

- **Phase 1 = `DataVo.Core` + `DataVo.Data` only.** `DataVo.EntityFrameworkCore` is OUT (later phase).
- **No public API change** — `DataVoContext`, the ADO.NET surface, and the reactive APIs are unchanged.
- **On-disk format:** the **catalog (`Catalog.xml`) format is PRESERVED byte-compatibly** (Target 1 keeps XML).
  Breaking the **WAL / HNSW / BTree** JSON formats (Targets 2–3) is **approved** (Newtonsoft→STJ).
- **Behavior oracle:** the full suite (`dotnet test DataVo.Tests/DataVo.Tests.csproj`, baseline **1005/1005**)
  must stay green at every target boundary, including Disk round-trip, WAL, HNSW/vector, MVCC, reactive.
- **No new reflection/dynamic code.** Every replacement must be analyzer-clean (no IL2026/IL3050/IL2104/IL3053).
- **The ratchet only goes down.** After each target, lower `CORE_BASELINE` in `scripts/check-aot-baseline.sh`
  to the new measured count and confirm `bash scripts/check-aot-baseline.sh` passes.
- TDD per task: failing test first where behavior is observable; small commits; one logical change each.

## File structure

| File | Responsibility | Change |
|---|---|---|
| `DataVo.Core/Runtime/Catalog/CatalogXml.cs` | Reflection-free model↔`XElement` mappers (Target 1) | Create |
| `DataVo.Core/Runtime/CatalogStore.cs` | Use the mappers; drop `XmlSerializer` | Modify (T1) |
| `DataVo.Core/Models/Catalog/*.cs` | Drop `[Serializable]`/`[Xml*]` attributes (no longer needed) | Modify (T1) |
| `DataVo.Core/Serialization/DataVoJsonContext.cs` | STJ source-gen context for all persisted DTOs | Create (T2/T3) |
| `DataVo.Core/Transactions/WalFileStore.cs`, `WalEntry.cs` | Newtonsoft → STJ source-gen | Modify (T2) |
| `DataVo.Core/Indexing/HNSW/HNSWIndexPersistence.cs` | Newtonsoft → STJ source-gen | Modify (T2) |
| `DataVo.Core/BTree/Core/JsonBTreeIndex.cs`, `BTree/BTreeNode.cs` | Newtonsoft → STJ source-gen | Modify (T2) |
| `DataVo.Core/Execution/Volcano/SortOperator.cs`, `HashAggregateOperator.cs` | STJ reflection → context | Modify (T3) |
| `DataVo.Core/Parser/DQL/Select.cs` | STJ reflection → context (HNSW snapshot dict) | Modify (T3) |
| `DataVo.Core/Parser/Utils/ScalarEvaluator.cs` | `dynamic` arithmetic → typed | Modify (T4) |
| `DataVo.Core/Parser/Statements/JoinStrategies/*.cs` | `dynamic` key → `object` | Modify (T4) |
| `DataVo.Core/Parser/Aggregations/*.cs` | `dynamic` Apply → typed | Modify (T4) |
| `DataVo.Core/Parser/Statements/Mechanism/StatementEvaluator*.cs` | `dynamic` equality → typed | Modify (T4) |
| `DataVo.Core/Parser/DDL/AlterTableModifyColumn.cs`, `DML/Update.cs`, `Models/Catalog/Column.cs` | `dynamic` → typed | Modify (T4) |
| `DataVo.Core/Services/AggregationService.cs` | `Activator.CreateInstance` → factory switch | Modify (T5) |
| `DataVo.Core/DataVo.Core.csproj` | `<WarningsAsErrors>` IL codes once at 0 (lock) | Modify (T5) |
| `scripts/check-aot-baseline.sh` | Lower `CORE_BASELINE` after each target | Modify (all) |

---

## Target 1: Catalog `XmlSerializer` → reflection-free `System.Xml.Linq`

**Why first:** XmlSerializer generates code at runtime → the native binary fails on the very first
`CREATE DATABASE`. `System.Xml.Linq` is AOT-safe, so hand-written `XElement` mappers remove the blocker
while keeping the `Catalog.xml` on-disk format and all existing catalog/disk tests green.

The catalog model types and their current XML attribute shapes (must be reproduced exactly by the mappers):
- `Database`: `[XmlAttribute] DatabaseName`; `[XmlArray("Tables")] [XmlArrayItem("Table")] List<Table> Tables`.
- `Table`, `Column`, `Field`, `ForeignKey`, `IndexFile`, `Reference`: read each file for its `[Xml*]` shape.

**Interfaces:**
- Produces: `internal static class CatalogXml` with, per model type `T` in {Database, Table, Column, Field,
  ForeignKey, IndexFile, Reference}: `XElement ToXElement(T model)` and `T <Name>FromXElement(XElement e)`.
- Consumes (in CatalogStore): the same `XContainer root` insertion points and `XNode` read points that
  `InsertIntoXml`/`ConvertFromXml` use today (call sites: `CatalogStore.cs:59,79,217,288,311,360`).

- [ ] **Step 1: Characterize the current on-disk format (oracle test).** Add a Disk-mode catalog
  round-trip test that asserts the exact XML produced today, so the mappers must reproduce it byte-for-byte.

```csharp
// DataVo.Tests/Storage/CatalogXmlFormatTests.cs (create)
using System.Xml.Linq;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Storage;

public class CatalogXmlFormatTests
{
    [Fact]
    public void Catalog_DatabaseAndTable_RoundTripsThroughDiskXml()
    {
        string dir = Path.Combine(Path.GetTempPath(), "catxml_" + Guid.NewGuid().ToString("N"));
        try
        {
            var store = new CatalogStore(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = dir });
            store.CreateDatabase(new Database { DatabaseName = "DbA", Tables = [] });
            store.CreateTable(new Table
            {
                TableName = "T1",
                Fields = [new Field { Name = "Id", Type = "INT" }],
            }, "DbA");

            string xml = File.ReadAllText(Path.Combine(dir, "Catalog.xml"));
            XDocument doc = XDocument.Parse(xml);
            Assert.Equal("Databases", doc.Root!.Name.LocalName);
            Assert.Equal("DbA", doc.Root!.Element("Database")!.Attribute("DatabaseName")!.Value);
            Assert.Equal("T1", doc.Descendants("Table").First().Attribute("TableName")!.Value);
        }
        finally { if (Directory.Exists(dir)) Directory.Delete(dir, true); }
    }
}
```
> Before writing the mappers, open each of the 7 model files and record its exact `[XmlRoot]`/`[XmlAttribute]`/
> `[XmlArray]`/`[XmlArrayItem]`/`[XmlElement]` shape; the mapper must emit identical element/attribute names.

- [ ] **Step 2: Run it — expect PASS (characterizes current XmlSerializer output).**
  Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter CatalogXmlFormatTests` → PASS.

- [ ] **Step 3: Write `CatalogXml` mappers (reflection-free).** Create
  `DataVo.Core/Runtime/Catalog/CatalogXml.cs` with `ToXElement`/`FromXElement` for each type, reproducing the
  recorded attribute/element names. Example shape for `Database` (mirror for the other 6 per their `[Xml*]`):

```csharp
using System.Xml.Linq;
using DataVo.Core.Models.Catalog;

namespace DataVo.Core.Runtime.Catalog;

internal static class CatalogXml
{
    public static XElement ToXElement(Database d) =>
        new("Database",
            new XAttribute("DatabaseName", d.DatabaseName),
            new XElement("Tables", (d.Tables ?? []).Select(ToXElement)));

    public static Database DatabaseFromXElement(XElement e) => new()
    {
        DatabaseName = e.Attribute("DatabaseName")!.Value,
        Tables = e.Element("Tables")?.Elements("Table").Select(TableFromXElement).ToList() ?? [],
    };

    public static XElement ToXElement(Table t) => /* mirror Table's [Xml*] shape */ null!;
    public static Table TableFromXElement(XElement e) => /* mirror */ null!;
    // ... Column, Field, ForeignKey, IndexFile, Reference — each ToXElement + <Name>FromXElement
}
```
> Implement every type fully (no stubs) against the recorded shapes; the Step-1 test + the existing catalog
> suite are the oracle that the names/structure match.

- [ ] **Step 4: Swap the two helpers in `CatalogStore`.** Replace the bodies of `InsertIntoXml<T>`
  (`CatalogStore.cs:554`) and `ConvertFromXml<T>` (`:583`) to dispatch to `CatalogXml` by `typeof(T)`, and
  remove `using System.Xml.Serialization;`. Insertion call sites (`:59,79,217`) and read sites
  (`:288,311,360`) keep their signatures.

```csharp
private void InsertIntoXml<T>(T obj, XContainer root) where T : class
{
    XElement element = obj switch
    {
        Database d => CatalogXml.ToXElement(d),
        Table t => CatalogXml.ToXElement(t),
        IndexFile i => CatalogXml.ToXElement(i),
        _ => throw new NotSupportedException($"No catalog XML mapper for {typeof(T).Name}"),
    };
    root.Add(element);
    SaveDocument();
}

private static T? ConvertFromXml<T>(XNode node) where T : class
{
    XElement e = (XElement)node;
    object result = typeof(T) switch
    {
        var t when t == typeof(ForeignKey) => CatalogXml.ForeignKeyFromXElement(e),
        var t when t == typeof(IndexFile) => CatalogXml.IndexFileFromXElement(e),
        _ => throw new NotSupportedException($"No catalog XML mapper for {typeof(T).Name}"),
    };
    return (T)result;
}
```

- [ ] **Step 5: Drop the now-unused XML attributes** from the 7 `Models/Catalog/*.cs` types
  (`[Serializable]`, `[XmlRoot]`, `[XmlAttribute]`, `[XmlArray]`, `[XmlArrayItem]`, `[XmlElement]`) and their
  `using System.Xml.Serialization;`. (Pure cleanup; the mappers no longer read attributes.)

- [ ] **Step 6: Run catalog + disk + full reactive suite — expect PASS unchanged.**
  Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --filter "CatalogXmlFormat|FullyQualifiedName~Catalog|FullyQualifiedName~Disk"` → PASS.

- [ ] **Step 7: Verify the AOT binary now passes `CREATE DATABASE`.**
  Run: `dotnet publish DataVo.AotSmoke/DataVo.AotSmoke.csproj -c Release -r osx-arm64` then
  `./DataVo.AotSmoke/bin/Release/net10.0/osx-arm64/publish/DataVo.AotSmoke`.
  Expected: gets past `[1] engine + InsertTyped` (no XmlSerializer error). (It may still fail later on a
  remaining dynamic/JSON path — that's Targets 2–4.)

- [ ] **Step 8: Lower the ratchet + full suite + commit.**
  Re-measure: `dotnet build DataVo.Core/DataVo.Core.csproj -c Release -t:Rebuild 2>&1 | grep -cE "warning IL"`.
  Set `CORE_BASELINE` in `scripts/check-aot-baseline.sh` to the new count; run `bash scripts/check-aot-baseline.sh` → OK.
  Run full suite → 1005+/1005+ green.
  ```bash
  git add DataVo.Core/Runtime/Catalog/CatalogXml.cs DataVo.Core/Runtime/CatalogStore.cs \
          DataVo.Core/Models/Catalog/*.cs DataVo.Tests/Storage/CatalogXmlFormatTests.cs \
          scripts/check-aot-baseline.sh
  git commit -m "perf(aot): catalog XmlSerializer -> reflection-free XElement (Native AOT P1 T1)"
  ```

---

## Target 2: Newtonsoft.Json eradication → STJ source generator (WAL, HNSW, BTree)

**Why:** Newtonsoft has no AOT mode (`Newtonsoft.Json.dll` emits IL2104/IL3053) and trims silently. Replace
every `JsonConvert` call with `System.Text.Json` via a source-gen `JsonSerializerContext`. Breaking the WAL/
HNSW/BTree on-disk JSON shape is approved; the disk round-trip tests (write+read with the new serializer)
remain the oracle.

**Interfaces:**
- Produces: `DataVo.Core/Serialization/DataVoJsonContext.cs` — `[JsonSerializable(typeof(X))]` for every
  persisted DTO (HNSW `HnswSnapshot`/`FallbackSnapshot`/`FallbackEntry`/`HNSWIndex.FlatState`; WAL
  `WalRecordEnvelope`/`WalEntry` + payload types; BTree `JsonBTreeIndex`/`BTreeNode`).
- Consumes: nothing new; replaces `JsonConvert.SerializeObject/DeserializeObject` at the sites below.

Known call sites: `HNSWIndexPersistence.cs:55,69,86,101`; `WalFileStore.cs:207,218,311,332,344`;
`JsonBTreeIndex.cs:208,231`; `BTreeNode.cs` + `WalEntry.cs` (`using Newtonsoft.Json[.Linq]`).

- [ ] **Step 1: Create the STJ source-gen context.**
```csharp
// DataVo.Core/Serialization/DataVoJsonContext.cs (create)
using System.Text.Json.Serialization;
using DataVo.Core.Indexing.HNSW; // FlatState etc. (make the nested DTOs internal/public as needed)

namespace DataVo.Core.Serialization;

[JsonSourceGenerationOptions(WriteIndented = false)]
[JsonSerializable(typeof(/* HnswSnapshot */ object))]
// add [JsonSerializable(typeof(T))] for every persisted DTO discovered in Steps 2-4
internal partial class DataVoJsonContext : JsonSerializerContext;
```
> As each DTO is migrated below, add its `[JsonSerializable(typeof(T))]` line here. DTOs that are private
> nested types (e.g. `HNSWIndexPersistence.HnswSnapshot`) must be made at least `internal` and use
> public get/set properties so the generator can emit metadata.

- [ ] **Step 2: Migrate HNSW persistence (round-trip test first).** Add an HNSW save/load round-trip test
  (Disk mode) asserting a known index reloads identically; run → PASS (current Newtonsoft). Then replace the
  4 `JsonConvert` calls in `HNSWIndexPersistence.cs` with `JsonSerializer.Serialize/Deserialize(..,
  DataVoJsonContext.Default.HnswSnapshot)` etc.; add the DTOs to the context; remove `using Newtonsoft.Json`.
  Re-run the round-trip + `FullyQualifiedName~Hnsw|FullyQualifiedName~Vector` → PASS.

- [ ] **Step 3: Migrate WAL (`WalFileStore.cs`, `WalEntry.cs`).** With a WAL replay round-trip test as
  oracle, replace `JsonConvert` at `WalFileStore.cs:207,218,311,332,344` with STJ context calls; add
  `WalRecordEnvelope`/`WalEntry`/payload DTOs to the context; remove Newtonsoft usings. Run
  `FullyQualifiedName~Wal|FullyQualifiedName~Transaction` → PASS.

- [ ] **Step 4: Migrate BTree (`JsonBTreeIndex.cs`, `BTreeNode.cs`).** Replace `JsonConvert` at
  `JsonBTreeIndex.cs:208,231` with STJ context calls; add DTOs; remove Newtonsoft usings. Run
  `FullyQualifiedName~BTree|FullyQualifiedName~Index` → PASS.

- [ ] **Step 5: Remove the Newtonsoft package reference** from `DataVo.Core.csproj` once `rg "Newtonsoft"
  DataVo.Core --glob '!**/bin/**' --glob '!**/obj/**'` returns nothing. Confirm the build has no IL2104/IL3053.

- [ ] **Step 6: Lower ratchet + full suite + commit.** Re-measure Core IL count, lower `CORE_BASELINE`,
  `bash scripts/check-aot-baseline.sh` → OK; full suite → green.
  ```bash
  git commit -am "perf(aot): eradicate Newtonsoft.Json -> STJ source-gen (WAL/HNSW/BTree) (Native AOT P1 T2)"
  ```

---

## Target 3: STJ source-gen contexts for remaining reflection-mode JSON (Volcano spill, Select snapshot)

**Why:** `JsonSerializer.Serialize/Deserialize<T>` without a context uses reflection (IL2026/IL3050). Route
them through `DataVoJsonContext`.

Sites: `Execution/Volcano/SortOperator.cs:293,471` and `HashAggregateOperator.cs:336,352`
(`TypedExecutionRow`); `Parser/DQL/Select.cs:2052,2097` (`Dictionary<string,double>` HNSW snapshot).

- [ ] **Step 1: Add `TypedExecutionRow` and `Dictionary<string,double>` to `DataVoJsonContext`.**
  `[JsonSerializable(typeof(TypedExecutionRow))]`, `[JsonSerializable(typeof(Dictionary<string, double>))]`.

- [ ] **Step 2: Replace the Volcano spill calls** at the 4 sites with
  `JsonSerializer.Serialize(typed, DataVoJsonContext.Default.TypedExecutionRow)` and the matching
  `Deserialize`. Run `FullyQualifiedName~Sort|FullyQualifiedName~Aggregate|FullyQualifiedName~Spill|FullyQualifiedName~Execution` → PASS.

- [ ] **Step 3: Replace the Select snapshot calls** at `Select.cs:2052,2097` with the
  `Dictionary<string,double>` context member. Run `FullyQualifiedName~Select|FullyQualifiedName~Hnsw` → PASS.

- [ ] **Step 4: Lower ratchet + full suite + commit.**
  ```bash
  git commit -am "perf(aot): route remaining JSON through STJ source-gen contexts (Native AOT P1 T3)"
  ```

---

## Target 4: `dynamic` removal across evaluators and joins → typed `CellValue`/`object`

**Why:** `dynamic` binds via `Microsoft.CSharp` DLR (IL2026 + IL3050 + runtime CallSite codegen). All 36
uses (22 files) become typed. Two flavours: (a) **key/value typing** (`dynamic` → `object`) where no operator
is applied (join lookups); (b) **typed operations** (arithmetic/comparison) where a small helper switches on
the runtime numeric type. The existing SQL semantics tests are the behavior oracle.

- [ ] **Step 1: `JoinLookupTable` + join strategies — `dynamic` key → `object`.**
  `JoinLookupTable : Dictionary<dynamic, List<Record>>` → `Dictionary<object, List<Record>>`;
  `AddRecord(dynamic key,…)` → `AddRecord(object key,…)`; in `InnerJoinStrategy.cs:172`,
  `LeftJoinStrategy.cs:203`, `RightJoinStrategy.cs:242`, `FullJoinStrategy.cs:264`, change `dynamic key =
  keyValue;` → `object key = keyValue!;`. (Equality/hash are object-based already — behavior identical.)
  Run `FullyQualifiedName~Join` → PASS. Commit.

- [ ] **Step 2: `ScalarEvaluator` — typed arithmetic.** Replace the `dynamic` arithmetic in
  `ScalarEvaluator.cs` with a typed numeric helper:
```csharp
public static object? Evaluate(ExpressionNode expression, Dictionary<string, object?> row)
{
    // literal/column branches return object? (unchanged)
    if (expression is BinaryExpressionNode binary)
    {
        object? left = Evaluate(binary.Left, row);
        object? right = Evaluate(binary.Right, row);
        if (left is null || right is null) return null;
        return ScalarArithmetic.Apply(binary.Operator, left, right);
    }
    ...
}
```
  Add `ScalarArithmetic.Apply(string op, object left, object right)` that switches on `(left, right)` numeric
  types (int/long/double/decimal) and applies `+ - * /`, preserving the current promotion behavior. Add a
  unit test covering int+int, double*int, decimal/int, and string-context cases. Run the new test +
  `FullyQualifiedName~Update|FullyQualifiedName~ScalarEvaluator|FullyQualifiedName~Set` → PASS. Commit.

- [ ] **Step 3: Aggregations — `dynamic? Apply` → typed.** In `Parser/Aggregations/*.cs` (`Aggregation.cs`,
  `Min/Max/Avg/Sum/Count.cs`) change `dynamic?` returns/locals to `object?` and use explicit numeric handling
  (reuse `ScalarArithmetic` / `Convert.ToDouble`/`decimal` as the code already does). Remove the
  `Convert.ChangeType(value, typeof(T))` `dynamic` cast in `Aggregation.cs:140` with a typed conversion. Run
  `FullyQualifiedName~Aggregate|FullyQualifiedName~GroupBy` → PASS. Commit.

- [ ] **Step 4: `StatementEvaluator` / `StatementEvaluatorWOJoin` — typed equality.** Replace the `dynamic`
  equality at `StatementEvaluator.cs:493,510` and `StatementEvaluatorWOJoin.cs:374` (`EvaluateEquality(dynamic,
  dynamic)`) with `EvaluateEquality(object?, object?)` using the existing value-comparison helper (the same
  one used by the reactive predicate / batch WHERE path) instead of DLR `==`. Run
  `FullyQualifiedName~Where|FullyQualifiedName~Select|FullyQualifiedName~Statement` → PASS. Commit.

- [ ] **Step 5: `AlterTableModifyColumn`, `Update`, `Column` — `dynamic` → `object`.** Change the remaining
  `dynamic` locals/params (`AlterTableModifyColumn.cs:166,176,203`, `Update.cs:279`, `Column.cs` ×3) to
  `object?` with explicit conversions. Run `FullyQualifiedName~Alter|FullyQualifiedName~Update|FullyQualifiedName~Column` → PASS. Commit.

- [ ] **Step 6: Confirm no `dynamic` remains + lower ratchet + full suite.**
  `rg -n '\bdynamic\b' DataVo.Core --glob '!**/bin/**' --glob '!**/obj/**'` → only comments/none.
  Lower `CORE_BASELINE`; `bash scripts/check-aot-baseline.sh` → OK; full suite → green. Commit.

---

## Target 5: `Activator.CreateInstance` cleanup (aggregation factory) + lock Core

**Why:** `AggregationService.cs:42` instantiates aggregation types via
`Activator.CreateInstance(type, args)` (IL2067). Replace the reflective lookup with an explicit factory.

- [ ] **Step 1: Replace the reflective factory with an explicit switch.** In `AggregationService.cs`, replace
  the `Dictionary<string, Type>` + `Activator.CreateInstance(type, column, expression, valueSelector,
  headerName)` with a `switch` on the aggregate name returning `new Sum(column, …)` / `new Count(...)` etc.
  directly (no reflection). Run `FullyQualifiedName~Aggregat` → PASS. Commit.

- [ ] **Step 2: Drive Core to 0 and LOCK it.** `dotnet build DataVo.Core -c Release -t:Rebuild 2>&1 |
  grep -cE "warning IL"` must be `0`. Then add to `DataVo.Core/DataVo.Core.csproj` the same
  `<WarningsAsErrors>…IL2026;…;IL3056</WarningsAsErrors>` line `DataVo.Data` uses, so Core is locked.

- [ ] **Step 3: Retire the ratchet, run the native gate.**
  `bash scripts/check-aot-baseline.sh` (Core baseline now 0). Publish + run the native smoke:
  `dotnet publish DataVo.AotSmoke/DataVo.AotSmoke.csproj -c Release -r osx-arm64` then run the native binary.
  Expected: **`ALL SMOKE CHECKS PASSED`** from the native image (zero AOT/trim warnings in the publish).

- [ ] **Step 4: Full suite + commit + docs.** Full suite → 1005+/1005+ green. Update the design spec +
  roadmap + memory with the final result (Core 184→0, native smoke green).
  ```bash
  git commit -am "perf(aot): aggregation factory (drop Activator) + lock DataVo.Core AOT-clean (Native AOT P1 T5)"
  ```

---

## Self-review

- **Spec coverage:** T1 = catalog XmlSerializer (spec Hit List B); T2 = Newtonsoft (A); T3 = STJ-reflection
  (C); T4 = `dynamic` (D); T5 = Activator (E) + Core lock. Trivial items (F) addressed incidentally. EF
  excluded per scope. Verification fence reused (ratchet + smoke). ✅
- **Format constraint:** T1 preserves `Catalog.xml` (oracle test asserts it); T2/T3 break WAL/HNSW/BTree JSON
  per approval (round-trip tests are symmetric). ✅
- **Placeholder note:** T1 is fully concrete (executed first). T2–T5 give exact files, call-site line numbers,
  the STJ-context/factory mechanism, and per-step verification commands; each migrates enumerated sites using
  the shown pattern (read each DTO/file for its members when adding `[JsonSerializable]`). ✅
- **Ratchet discipline:** every target ends by lowering `CORE_BASELINE` and proving `check-aot-baseline.sh`;
  Core is locked only at T5 step 2 once the count is 0. ✅
