# Quickstart

DataVo embeds directly into a .NET process. The smallest useful path is to create a `DataVoContext`, select a database, define a table, insert data, and read a `QueryResult`.

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});

db.Execute("CREATE DATABASE Demo");
db.Execute("USE Demo");

db.Execute("""
CREATE TABLE Users (
  Id INT PRIMARY KEY,
  Name VARCHAR(50),
  Score FLOAT
);
""");

db.Execute("INSERT INTO Users (Id, Name, Score) VALUES (1, 'Alice', 95.5)");

var results = db.Execute("""
SELECT Id, Name, Score
FROM Users
WHERE Id = 1;
""");

var row = results[0].Data[0];
Console.WriteLine($"{row["Id"]}: {row["Name"]} ({row["Score"]})");
```

`DataVoContext.Execute(...)` returns a `List<QueryResult>`. Each `QueryResult` contains:

| Member | Meaning |
| --- | --- |
| `Fields` | Ordered result column names. |
| `Data` | Row data as dictionaries keyed by column name. |
| `Messages` | Engine messages such as row counts or validation notes. |
| `IsError` | Whether the statement failed. |

## Disk-backed start

For persistent local data, switch to `StorageMode.Disk` and provide a storage path:

```csharp
using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Disk,
    DiskStoragePath = "./datavo_data",
    WalEnabled = true
});
```

Disk mode enables WAL by default. See [Configuration](./configuration.md) before choosing durability options for important data.
