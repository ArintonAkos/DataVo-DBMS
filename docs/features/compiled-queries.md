# Source-Generated Compiled Queries

Compiled queries give fixed SQL shapes a generated, typed entry point. Dynamic SQL and ad hoc statements should still use `DataVoContext.Execute(...)`.

## Attribute model

Mark a `static partial` method with `[DataVoQuery]`:

```csharp
public sealed record PlayerRow(int Id, string Name, int Level);

public static partial class GameQueries
{
    [DataVoQuery("SELECT Id, Name, Level FROM Players WHERE Id = @id")]
    public static partial PlayerRow? GetPlayer(DataVoContext db, int id);
}
```

The generator validates the SQL at build time, creates a `DataVoCompiledQueryPlan`, and emits a method body that calls the runtime helpers in `DataVoCompiledQuery`.

## Supported V1 SQL shapes

The V1 source generator supports these fixed forms:

```sql
SELECT Id, Name FROM Players WHERE Id = @id
INSERT INTO Telemetry (Id, EventName, Frame) VALUES (@id, @eventName, @frame)
UPDATE Players SET Level = @level WHERE Id = @id
```

Current support is intentionally narrow:

| Shape | Generated helper |
| :---- | :--------------- |
| `SELECT ... WHERE col = @param` | `SelectSingle` or `SelectMany`, based on return type |
| `INSERT INTO ... VALUES (@param...)` | `Insert` |
| `UPDATE ... SET ... WHERE col = @param` | `Update` |

For runtime construction without source generation, create a `DataVoCompiledQueryPlan` and pass `DataVoCompiledQueryParameter` values to `DataVoCompiledQuery`.

## Diagnostics

The analyzer reports build errors for unsupported compiled-query declarations:

| Diagnostic | Meaning |
| :--------- | :------ |
| `DATAVOQ001` | SQL shape is not supported by the generator |
| `DATAVOQ002` | SQL parameter has no matching method parameter |

Use `DataVoContext.Execute(...)` for SQL that depends on runtime string construction, optional clauses, joins, grouping, ordering, or other shapes outside the V1 contract.

## Related pages

- [Runtime Observability](./runtime-observability.md)
- [Setup and Packaging](./setup-and-packaging.md)
- [SELECT and Querying](./select-and-querying.md)
