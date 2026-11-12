# Roslyn Source Generators

Use source-generated queries for queries called often enough that parse and mapping overhead matters. Instead of parsing and planning the same SQL pattern repeatedly at runtime, you annotate a static partial method and let the generator emit the call path.

Start with a projection type.

```csharp
public sealed record PlayerProjection(int Id, string Name, int Level);
```

Then declare a static partial method with `[DataVoQuery]`.

```csharp
using DataVo.Core;
using DataVo.Core.CompiledQueries;

public static partial class GameQueries
{
    [DataVoQuery("SELECT Id, Name, Level FROM Players WHERE Id = @id")]
    public static partial PlayerProjection? GetPlayer(DataVoContext db, int id);
}
```

At runtime, call the generated method like ordinary C#.

```csharp
PlayerProjection? player = GameQueries.GetPlayer(db, 42);
```

The first parameter must be `DataVoContext`. SQL parameters such as `@id` must match C# method parameters after the context parameter.

```csharp
public static partial class PlayerWrites
{
    [DataVoQuery("INSERT INTO Players (Id, Name, Level) VALUES (@id, @name, @level)")]
    public static partial IReadOnlyList<long> InsertPlayer(DataVoContext db, int id, string name, int level);

    [DataVoQuery("UPDATE Players SET Level = @level WHERE Id = @id")]
    public static partial int UpdateLevel(DataVoContext db, int id, int level);
}
```

For indexed point lookups, provide a schema manifest as an additional file. The generator can use that manifest to pre-resolve single-column index hints.

```xml
<ItemGroup>
  <AdditionalFiles Include="datavo.schema.sql" DataVoSchemaManifest="true" />
</ItemGroup>
```

```sql
CREATE TABLE Players (
  Id INT PRIMARY KEY,
  Name VARCHAR(80),
  Level INT
);

CREATE INDEX IX_Players_Name ON Players (Name);
```

Use generated queries for stable point reads, inserts, and narrow fixed-shape updates. Use runtime SQL for exploratory queries, joins, aggregates, vector search, and complex SQL.

## Typed Table Queries

A model marked `[DataVoTable]` can be queried with lambdas through `DataVoContext.Table<T>()`. The generator reads each lambda when the project builds and replaces the call with a prepared statement, using C# interceptors. No expression tree is built, nothing is reflected over, and nothing is compiled at run time, so the calls are safe under Native AOT.

```csharp
[DataVoTable("players")]
public sealed class Player
{
    [PrimaryKey] public int Id { get; set; }
    [Indexed] public int Level { get; set; }
    public string Name { get; set; } = "";
    public bool IsActive { get; set; }
}

DataVoTable<Player> players = db.Table<Player>();

// Pass values as state and mark the lambda static: no closure, and the value goes straight to the statement.
Player? one = players.FirstOrDefault(targetId, static (p, id) => p.Id == id);

foreach (Player p in players.Where((min, max), static (p, r) => p.Level >= r.min && p.Level <= r.max))
{
    Console.WriteLine(p.Name);
}

// Literals and constants need no state.
Player? admin = players.FirstOrDefault(p => p.Id == 1);
List<Player> veterans = players.Where(p => p.Level >= 50 && p.IsActive).ToList();
```

`FirstOrDefault`, `SingleOrDefault` and `Where` are available with and without state. `Where` returns a `DataVoQuery` that runs when it is enumerated and offers `FirstOrDefault()`, `SingleOrDefault()`, `Any()`, `Count()`, `Sum()`, `Min()`, `Max()` and `ToList()`. Its `IsCompiled` property tells you whether the generator compiled it.

A primary-key equality on an LSM database reads one row and allocates only that row. A miss allocates nothing.

Enumerating a query streams. Rows are read in pages of 1,024 and each model is built as the loop reaches it, so a `foreach` over 100,000 matching rows keeps about one page alive, not the result. `ToList()` is the way to ask for everything at once. Scans that were not compiled stream the same way. Some queries read their matches before enumerating, as `StreamQuery` does: a query inside a transaction, a condition with an equality on an indexed column (few rows, read through the index), and a condition the paged scan cannot apply, which today means `||`, a null test, or `!=` on a nullable column.

### Counting, Summing And Finding Extremes Without Building Rows

`Count()` and `Any()` on a compiled query do not build model objects when the condition reads only unindexed `int`, `long`, `float`, `double`, `bool` or `Guid` columns (comparisons, null tests, `&&`, `||`). The generator also writes the condition as a struct that reads those columns from the stored bytes, and the storage scan tests each row with it where the row is stored: nothing is copied out, and only the cells up to the last column the condition reads are located. Once warm, such a count allocates nothing on the LSM and in-memory engines, where enumerating the same 100,000 matches allocates about 22 MB, and it runs 6 to 20 times faster. The read lock is held for 4,096 rows at a time, so writers run between batches. Inside a transaction the rows are streamed instead, so the count includes the transaction's own writes.

```csharp
int rich = db.Table<Player>().Where(5_000L, static (p, gold) => p.Gold > gold).Count(); // 0 B
```

`Sum` works the same way. `DataVoQuery` has `Sum` for `int`, `long`, `float` and `double` selectors. When the query's condition is compiled into the scan and the selector is one non-nullable column of its own type (`p => p.Gold`), the generator adds that column up inside the same scan; the total stays in the scan's own variables and no row is built. Any other selector (`p => p.Gold * 2`, a nullable column, a widening cast) runs as written over the rows. An integer sum that leaves its type's range throws `OverflowException`; a `float` sum is accumulated as a `double` and rounded once, as LINQ does.

```csharp
long gold = db.Table<Player>().Where(static p => p.IsActive).Sum(static p => p.Gold); // 0 B
```

`Min` and `Max` are found in the same scan. `DataVoQuery` has `Min(selector)` and `Max(selector)` for any value type, with the semantics of `Enumerable.Min` and `Enumerable.Max`:

- Null values are skipped.
- When no row matches, or every value is null, the call throws `InvalidOperationException` for a type that cannot be null (`int`) and returns `null` for one that can (`int?`, `string`).
- A floating-point `NaN` orders below every number, so `Min` returns `NaN` if any value is `NaN`, and `Max` returns `NaN` only when every value is.

When the query's condition is compiled into the scan and the selector is one `int`, `long`, `float`, `double`, `DataVoDate` or `DateOnly` column of its own type, nullable or not, the generator keeps the smallest or largest value in the scan's own variables: no row is built and, once warm, nothing is allocated on the LSM and in-memory engines. Any other selector (`p => p.Gold * 2`, a cast, a `string` column) runs as written over the rows. Inside a transaction the rows are streamed instead of read in place, so the result includes the transaction's own writes.

```csharp
var active = db.Table<Player>().Where(static p => p.IsActive);
long poorest = active.Min(static p => p.Gold);          // 0 B; throws when no player is active
int? bestBonus = active.Max(static p => p.Bonus);       // 0 B; null when no active player has a bonus
DataVoDate first = active.Min(static p => p.JoinedOn);  // 0 B
```

DataVo has no `DATETIME` column: a `DateTime` property is not mapped, and a point in time is stored as a `long` (ticks or Unix time), which `Min` and `Max` read as any other `long` column.

The scan is generic over the compiled condition all the way into the storage engine's row loop, so no delegate is called per row.

When every column up to the last one the query reads has a fixed width (`int`, `long`, `float`, `double`, `bool`, `Guid`, dates), values sit at the same byte offset in every row whose leading cells are all present. The condition is then also compiled in a form that reads stored bytes at those offsets, worked out once per scan, with no row view built and no branch between its tests. Each row is first checked against that layout by its leading null flags. A row that differs, such as one holding a NULL in a leading column, is tested the general way, so results are identical. An in-memory table goes one step further. Its rows are separate objects, so the first compiled scan of a table writes each row once more as a short fixed-length record, a null flag and the value's bytes for every column, with text and vector columns keeping only their flag. The records lie side by side in large blocks, and later scans read those: memory is read in order, and every column sits at the same offset in every row, including rows with NULLs and tables with text columns ahead of the ones read. Scans keep the records up to date as rows are added and deleted. They cost a few dozen bytes per row, and only tables that are scanned this way have them. Put fixed-width columns before text columns in a model to get this path; a condition on a column that follows a `string` still runs in the scan, through the general reader.

On 200,000 rows with 100,000 matches a warm count, sum, minimum or maximum takes about 0.4 to 0.5 ms in memory, 1.7 ms on the disk engine and 2.0 ms on LSM (2.4 ms once flushed to an SSTable). Most of what remains on disk and LSM is the storage iteration itself.

A condition on the primary key or an indexed column keeps using the index, and a condition that compares text is evaluated by the SQL engine; both count by reading the matching rows, as before.

### Columnar Indexes

Mark the columns that analytical queries filter and aggregate on with `[Columnar]`. `Ensure…Table` then creates one columnar index for the table, named `cx_<table>`, over the marked columns (the same index as `CREATE INDEX ... USING COLUMNAR`; see [CREATE TABLE](/manual/sql-language/create-table)).

```csharp
[DataVoTable("players")]
public sealed class Player
{
    [PrimaryKey] public int Id { get; set; }
    [Columnar] public int Level { get; set; }
    [Columnar] public long Gold { get; set; }
    [Columnar] public double Ratio { get; set; }
    public string Name { get; set; } = "";
}

long gold = db.Table<Player>().Where(50, static (p, min) => p.Level >= min).Sum(static p => p.Gold); // 0 B
```

`Count()`, `Any()`, `Sum`, `Min` and `Max` on a compiled query are answered from the index when every column the condition reads, and the column the selector reads, is marked. The condition may use comparisons, null tests, `&&` and `||`. The results are the ones the row scan gives, including the `OverflowException` of an integer sum and the `NaN` ordering of `Min` and `Max`, and nothing is allocated. On 200,000 in-memory rows with 100,000 matches these take 0.03 to 0.09 ms, against 0.4 to 0.5 ms for the row scan; see [Benchmark Results](/manual/performance/benchmarks#with-a-columnar-index).

- `[Columnar]` applies to `int`, `long`, `float`, `double`, `bool` and date properties, nullable or not. On any other type, ensuring the table fails with an error that names the column.
- The index exists for the in-memory storage mode. On other storage modes the attribute is accepted and queries read rows.
- A query that reads a column outside the index, and any query inside a transaction, read rows, as described above.
- The index is a second copy of the marked columns and is written on every insert and delete, so mark the columns such queries use, not every column.

### What Is Compiled

| Lambda | Becomes |
| --- | --- |
| `p.Id == value`, `value == p.Id` | `Id = @p0`; a point lookup when `Id` is the primary key or is indexed |
| `!=`, `<`, `<=`, `>`, `>=` | The matching SQL comparison |
| `p.IsActive`, `!p.IsActive` | `IsActive = true`, `IsActive = false` |
| `p.Guild == null`, `p.Guild is not null` | `IS NULL`, `IS NOT NULL` |
| `a && b`, `a \|\| b`, parentheses | `AND`, `OR`, parentheses |

A value can be a literal, a constant, the state argument, a field or property of the state (including tuple elements), or an accessible static field or property. Null follows C# rules, not SQL rules: a nullable column that holds null counts as different from a value in `!=`, and a state value that is null at run time is answered by running the lambda over the table.

### What Is Not Compiled

A call the generator cannot compile still returns the right rows. It reads the whole table and filters it with the lambda, and the build reports why:

| Warning | Meaning |
| --- | --- |
| `DATAVOL001` | The lambda captures a local, a parameter or an instance member. Pass it as state with a `static` lambda. |
| `DATAVOL002` | The lambda uses something that has no compiled form: a method call, arithmetic, two columns compared with each other, or a value whose type differs from the column's. |
| `DATAVOL003` | The project has not enabled interceptors for the generated namespace, so no call is compiled. |

Silence a deliberate scan with `#pragma warning disable DATAVOL001`. A delegate created elsewhere and passed in is treated as a deliberate scan and is not reported.

### Enabling Interceptors

The `DataVo.Generators` NuGet package enables interceptors for you. A project that references the generator another way, such as a project reference, needs this property:

```xml
<PropertyGroup>
  <InterceptorsNamespaces>$(InterceptorsNamespaces);DataVo.Generated</InterceptorsNamespaces>
</PropertyGroup>
```

Interceptors need the C# 12 compiler or newer. On an older compiler, such as Unity's, the same code compiles and returns the same rows but every query is a scan; use the generated `Get…ById` method or a prepared statement on hot paths there. Unity's compiler also has no module initializers, so call the model's `Ensure…Table` method before `Table<T>()`.

## Spatial Queries

A `System.Numerics.Vector3` property is stored as a `VECTOR(3)` column. `DataVoTable<T>` can then find rows by position:

```csharp
using System.Numerics;

[DataVoTable("ships")]
public sealed class Ship
{
    [PrimaryKey] public int Id { get; set; }
    public string Name { get; set; } = "";
    [SpatialIndex] public Vector3 Position { get; set; }
}

DataVoTable<Ship> ships = db.Table<Ship>();

// Rows within 50 units of a point, the sphere's surface included.
foreach (Ship ship in ships.WithinRadius(station, radius: 50f)) { }

// Rows inside an axis-aligned box. The corners may come in either order.
List<Ship> docked = ships.WithinBoundingBox(minCorner, maxCorner).ToList();

// The five closest rows, nearest first.
IReadOnlyList<Ship> closest = ships.NearestNeighbors(player, k: 5);
```

| Method | How it runs |
| --- | --- |
| `WithinRadius(center, radius)` | Streams the table a page at a time. Each row's stored coordinates are tested before the model is built, so rows outside the sphere cost no model and the test allocates nothing. Distances are compared in double precision, so a point exactly on the surface is included even far from the origin. |
| `WithinBoundingBox(min, max)` | Streams like `WithinRadius`. A `BoundingBox3` overload is also available. The box's surface is inside. |
| `NearestNeighbors(target, k)` | Uses the column's Euclidean HNSW index when it has one: it fetches twice `k` candidates and ranks them by exact distance. Without an index, inside an explicit transaction, or with `exact: true`, it streams the table once and keeps the best `k`, so the answer is exact. Ties keep storage order. |

`[SpatialIndex]` (or `[Indexed]` on a `Vector3`) makes `Ensure…Table` create a Euclidean HNSW index named `sx_<table>_<column>`. HNSW is an approximate search: it can, rarely, miss a true neighbor. Pass `exact: true` when every answer must be exact. `WithinRadius` and `WithinBoundingBox` always read the whole table, because an HNSW index answers nearest-neighbor questions, not range ones.

When a model has more than one `Vector3` property, name the column: `ships.WithinRadius(center, 10f, column: "Spawn")`. A nullable `Vector3?` is supported, and a row whose position is null never matches.

`Spatial3` and `BoundingBox3` expose the same math for your own code: `Distance`, `DistanceSquared`, `DistanceSquaredPrecise`, `IsWithinRadius`, `Contains`, `Intersects`, `ClosestPoint`. None of them allocate. They use `System.Numerics`, which the .NET JIT compiles to SIMD instructions and which Unity's Mono and IL2CPP run as plain scalar code. In Unity, convert at the boundary: `new System.Numerics.Vector3(v.x, v.y, v.z)`.

A lambda that compares a `Vector3` column, such as `Where(p, static (s, p) => s.Position == p)`, is not compiled to SQL (`DATAVOL002`); it scans and still returns the right rows.

## SQL DATE Projections

The cross-target projection type for SQL `DATE` is `DataVo.Core.DataVoDate`. Generated `DataVoDate` and `DataVoDate?` properties call `GetDate` and `GetDateOrNull`. On modern .NET only, generated `System.DateOnly` and `System.DateOnly?` properties are explicit adapters and call `GetDateOnly` and `GetDateOnlyOrNull`.

## Source Generator Support Summary

The generator supports `static partial` methods whose first parameter is a `DataVoContext`, with SQL/C# parameter validation, and it emits compiled paths for equality `SELECT`, `INSERT INTO ... VALUES (...)`, and `UPDATE ... SET ... WHERE ...`, plus typed projection mappers and single-column schema-manifest index hints. It supports `System.Numerics.Vector3` columns with `WithinRadius`, `WithinBoundingBox` and `NearestNeighbors` on typed tables. It does **not** generate `DELETE`, joins, aggregates, or general vector search, and its typed table queries cover single-table filters only, not a full LINQ provider — use runtime SQL for those shapes.
