# Dapper

> Source route: /manual/client-interfaces/dapper
> Source file: manual/client-interfaces/dapper.md

Dapper works with DataVo through the standard ADO.NET provider. DataVo does not ship a Dapper-specific package or extension, and it does not need one: `DataVoConnection` is a `DbConnection`, and Dapper's extension methods operate on `IDbConnection`.

```bash
dotnet add package DataVo.Data --prerelease
dotnet add package Dapper
```

## Example

```csharp
using Dapper;
using DataVo.Data;

using var connection = new DataVoConnection("StorageMode=InMemory;DataSource=App");
connection.Open();

connection.Execute("CREATE TABLE Players (Id INT PRIMARY KEY, Name VARCHAR(40), Level INT)");

connection.Execute(
    "INSERT INTO Players (Id, Name, Level) VALUES (@Id, @Name, @Level)",
    new { Id = 1, Name = "Ada", Level = 12 });
connection.Execute(
    "INSERT INTO Players (Id, Name, Level) VALUES (@Id, @Name, @Level)",
    new { Id = 2, Name = "Linus", Level = 3 });

List<Player> strong = connection
    .Query<Player>("SELECT Id, Name, Level FROM Players WHERE Level >= @min", new { min = 5 })
    .ToList();

Player? linus = connection.QuerySingleOrDefault<Player>(
    "SELECT Id, Name, Level FROM Players WHERE Id = @id", new { id = 2 });

int total = connection.ExecuteScalar<int>("SELECT COUNT(*) FROM Players");

public sealed class Player
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public int Level { get; set; }
}
```

## Verified usage

The following calls have been run against `DataVoConnection` with Dapper 2.1.35 and behave as they do on other ADO.NET providers:

| Dapper call | Result |
| --- | --- |
| `Execute` with anonymous-object parameters | Rows-affected count is correct. |
| `Query<T>` and `QuerySingleOrDefault<T>` | Rows map to POCOs by column name. |
| `ExecuteScalar<T>` | Returns the first column of the first row. |
| `QueryAsync<T>` | Works; it completes synchronously because the engine is in-process (see [ADO.NET Provider](/manual/client-interfaces/ado-net#behavior-to-know-about)). |
| `Execute` with an `IDbTransaction` | `Rollback()` discards the changes. |

Anything outside that list, such as multi-mapping, `QueryMultiple` and list parameter expansion, goes through the same ADO.NET surface but is not part of the verified set. Test those against your own schema before depending on them.

## Dapper or compiled LINQ?

| | Dapper | [Compiled LINQ](/manual/client-interfaces/source-generators) |
| --- | --- | --- |
| Query language | Plain SQL strings | C# lambdas checked at compile time |
| Mapping | Reflection-based, per Dapper | Source-generated, no reflection |
| Allocation | One object per row, plus Dapper's own | Aggregates allocate nothing once warm |
| Native AOT | Governed by Dapper's own AOT support | Yes |
| Fits | Existing SQL, ad-hoc queries, joins | Hot-path lookups, filtered counts and aggregates |

The two coexist on one database: use Dapper for flexibility and compiled LINQ for the handful of queries that dominate your profile.

## Related

- [ADO.NET Provider (DataVo.Data)](/manual/client-interfaces/ado-net)
- [ADO.NET & Dapper tutorial](/manual/tutorial/ado-net-and-dapper)
- [Choosing an Interface](/manual/getting-started/choosing-an-interface)
