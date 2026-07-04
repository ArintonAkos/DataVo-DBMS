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

## Source Generator Support Summary

| Feature | Status | Notes |
| --- | --- | --- |
| Static partial method generation | Supported | Containing type and method must be `static partial`. |
| `DataVoContext` first parameter | Supported | Generated methods execute against an existing context. |
| SQL parameter validation | Supported | SQL parameters must match C# parameters. |
| Equality `SELECT` | Supported | `SELECT ... FROM table WHERE column = @param`. |
| `INSERT INTO ... VALUES (...)` | Supported | Emits compiled insert path. |
| `UPDATE ... SET ... WHERE ...` | Supported | Emits compiled update path for supported shapes. |
| Typed projection mapper | Supported | Eligible projections use typed row readers. |
| Schema manifest index hints | Supported | Single-column indexes can be pre-resolved as hints. |
| Joins, aggregates, and vector search | Not Supported | Use runtime SQL for complex query shapes. |
| `DELETE` generation | Not Supported | Not emitted in v0.1. |
| Full LINQ provider generation | Not Supported | The generator covers a limited set of SQL patterns. |
