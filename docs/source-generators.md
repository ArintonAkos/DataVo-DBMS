# Roslyn Source Generators

DataVo includes a Roslyn source-generator project, `DataVo.Generators`, packaged as `DataVo.Generators` and emitted as an analyzer under `analyzers/dotnet/cs`. Its current purpose is compiled queries: developers annotate static partial methods with `[DataVoQuery("...")]`, and the generator emits a `DataVoCompiledQueryPlan` plus invocation code.

This is not a full LINQ provider and not a full SQL compiler. It is a narrow fast path for SQL shapes the engine can validate and pre-plan at compile time.

## Basic Usage

```csharp
using DataVo.Core;
using DataVo.Core.CompiledQueries;

public sealed record PlayerProjection(int Id, string Name, int Level);

public static partial class GameQueries
{
    [DataVoQuery("SELECT Id, Name, Level FROM Players WHERE Id = @id")]
    public static partial PlayerProjection? GetPlayer(DataVoContext db, int id);
}
```

Requirements verified in `DataVoQueryGenerator`:

| Requirement | Reason |
| --- | --- |
| The containing type must be `static partial`. | The generator emits another partial declaration for that type. |
| The method must be `static partial`. | The generator supplies the implementation body. |
| The first parameter must be `DataVoContext`. | Runtime execution is bound to an existing embedded context. |
| SQL parameters such as `@id` must match method parameters after the context parameter. | Missing parameters produce diagnostic `DATAVOQ002`. |

Unsupported SQL or unsupported method shapes produce diagnostic `DATAVOQ001`.

## Supported SQL Shapes

The parser in `DataVo.Generators.Sql.DataVoQueryShapeParser` recognizes these forms:

```sql
SELECT Id, Name, Level
FROM Players
WHERE Id = @id;
```

```sql
INSERT INTO Players (Id, Name, Level)
VALUES (@id, @name, @level);
```

```sql
UPDATE Players
SET Level = @level
WHERE Id = @id;
```

| SQL shape | Required return shape | Runtime call emitted |
| --- | --- | --- |
| `SELECT ... FROM table WHERE column = @param` | A custom class/record/struct, nullable custom type, `List<T>`, or `IReadOnlyList<T>`. | `SelectSingle`, `SelectMany`, or typed variants. |
| `INSERT INTO table (...) VALUES (...)` | `IReadOnlyList<long>` | `DataVoCompiledQuery.Insert` |
| `UPDATE table SET col = @param WHERE col = @param` | `int` | `DataVoCompiledQuery.Update` |

The generator currently does not parse joins, aggregates, vector search, range predicates, `ORDER BY`, `LIMIT`, deletes, transactions, or arbitrary expressions. Use runtime SQL execution for those shapes.

## Typed Projection Fast Path

When a selected record/class constructor matches the projected column names and uses supported parameter types, the generator emits a typed row mapper using `CompiledRowReader`.

Supported typed getters include:

| C# type | Reader method |
| --- | --- |
| `int`, `int?` | `GetInt32`, `GetInt32OrNull` |
| `long`, `long?` | `GetInt64`, `GetInt64OrNull` |
| `double`, `double?` | `GetDouble`, `GetDoubleOrNull` |
| `decimal`, `decimal?` | `GetDecimal`, `GetDecimalOrNull` |
| `bool`, `bool?` | `GetBoolean`, `GetBooleanOrNull` |
| `DateOnly`, `DateOnly?` | `GetDate`, `GetDateOrNull` |
| `Guid`, `Guid?` | `GetGuid`, `GetGuidOrNull` |
| `string`, `string?` | `GetString` |
| `float[]` | `GetVector` |

If the constructor does not cleanly match the projection, or the parameter type is unsupported, generated code falls back to a dictionary-based mapper. That fallback is correct but gives up the zero-boxing typed reader advantage.

## Schema Manifest

The generator can pre-resolve single-column indexes from `AdditionalFiles` marked with `DataVoSchemaManifest="true"`.

```xml
<ItemGroup>
  <AdditionalFiles Include="datavo.schema.sql" DataVoSchemaManifest="true" />
</ItemGroup>
```

```sql
CREATE TABLE Players (
    Id INT PRIMARY KEY,
    Name TEXT,
    Level INT
);

CREATE INDEX IX_Players_Level ON Players (Level);
```

The manifest parser recognizes:

| Manifest DDL | Current behavior |
| --- | --- |
| Single-column `CREATE TABLE ... PRIMARY KEY` | Recorded in the compile-time catalog. |
| Single-column `CREATE UNIQUE INDEX ... ON t (col)` | Recorded as a single-column index. |
| Single-column `CREATE INDEX ... ON t (col)` | Recorded as a single-column index. |
| Composite keys/indexes | Ignored and safely degraded. |
| Unrecognized DDL | Ignored and safely degraded. |

When a `SELECT` equality predicate matches a manifest index, the generated plan sets `CompiledAccessPath.SingleColumnIndex` and stores the resolved index name. At runtime this is treated as a fast-path hint; if the runtime schema does not match, execution falls back to runtime resolution instead of corrupting results.

## Advantages

| Advantage | Why it matters |
| --- | --- |
| Compile-time parameter validation | SQL parameters must have matching C# method parameters. |
| Lower per-call parsing/planning overhead | The SQL shape becomes a generated static plan. |
| Typed row mapping | Eligible projections avoid dictionary materialization and boxing. |
| Pre-resolved access paths | Schema manifests can route equality selects directly to known single-column indexes. |
| Native AOT friendliness | The generated code avoids reflection-heavy mapper discovery for supported shapes. |

## Disadvantages And Limits

| Limit | Practical impact |
| --- | --- |
| Narrow SQL subset | Complex SQL still belongs on the runtime SQL path. |
| Static partial method pattern | The API is less ergonomic than ordinary methods until templates/packages improve. |
| Minimal DDL manifest parser | Composite indexes and richer schema metadata are not compiled yet. |
| Runtime schema can drift | Pre-resolved access paths are hints and must fail safe. |
| Typed vector getter clones | `GetVector` returns a defensive `float[]` clone, so vector projections are not zero-copy. |
| Compiled update fast path has constraints | Fixed-width byte patching only applies to narrow indexed single-row update shapes. |

## When To Use It

Use source-generated queries for stable point lookups, small equality result sets, insert calls, and fixed-width updates that sit on a hot path. Use normal SQL execution while exploring schemas, using complex SQL, or building features where ergonomics matter more than removing every allocation.
