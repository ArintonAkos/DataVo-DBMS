# Quickstart

This quickstart creates a DataVo database entirely in memory. It is the fastest way to learn the API because it does not write files and it does not require a server.

Create a new console application and add the DataVo package when it is available from your package source. In the repository, use the project references or local package artifacts used by the solution.

```bash
dotnet new console -n DataVoQuickstart
cd DataVoQuickstart
dotnet add package DataVo.Core
```

Create a `DataVoContext` with `StorageMode.InMemory`.

```csharp
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});
```

DataVo can host multiple logical databases. Create one and make it the active database for the current session.

```csharp
db.Execute("CREATE DATABASE Demo");
db.Execute("USE Demo");
```

Define a table with a primary key and a few scalar columns.

```csharp
db.Execute("""
CREATE TABLE Users (
  Id INT PRIMARY KEY,
  Name VARCHAR(80),
  Score FLOAT,
  IsActive BIT
);
""");
```

Insert a row. Column-targeted inserts are easier to maintain than full-row inserts because the column order is visible at the call site.

```csharp
db.Execute("""
INSERT INTO Users (Id, Name, Score, IsActive)
VALUES (1, 'Ada', 98.5, true);
""");
```

Read the row back with ordinary SQL. `Execute` returns a list because a SQL string can contain multiple statements.

```csharp
List<QueryResult> results = db.Execute("""
SELECT Id, Name, Score
FROM Users
WHERE IsActive = true
ORDER BY Score DESC
LIMIT 10;
""");

QueryResult result = results[0];

foreach (Dictionary<string, object?> row in result.Data)
{
    Console.WriteLine($"{row["Id"]}: {row["Name"]} scored {row["Score"]}");
}
```

The same flow works with persistent LSM storage. Use strict LSM mode when you want writes to wait for WAL durability.

```csharp
using var persistentDb = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Lsm,
    DiskStoragePath = "./datavo_lsm_data",
    LsmStrictFsync = true
});
```

Here is the full quickstart as one file.

```csharp
using DataVo.Core;
using DataVo.Core.Contracts.Results;
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
  Name VARCHAR(80),
  Score FLOAT,
  IsActive BIT
);
""");

db.Execute("""
INSERT INTO Users (Id, Name, Score, IsActive)
VALUES (1, 'Ada', 98.5, true);
""");

List<QueryResult> results = db.Execute("""
SELECT Id, Name, Score
FROM Users
WHERE IsActive = true
ORDER BY Score DESC
LIMIT 10;
""");

foreach (Dictionary<string, object?> row in results[0].Data)
{
    Console.WriteLine($"{row["Id"]}: {row["Name"]} scored {row["Score"]}");
}

// Output:
// 1: Ada scored 98.5
```

Expected output:

```text
1: Ada scored 98.5
```

## Quickstart Support

Everything in this quickstart is part of the public subset: constructing `DataVoContext` with a `DataVoConfig` (which selects storage mode and durability), `CREATE DATABASE` and `USE`, `CREATE TABLE` with primary keys and scalar columns, column-targeted `INSERT` (the value count must match the column list), and `SELECT ... WHERE ... ORDER BY ... LIMIT`. The general query API returns `QueryResult`; a strongly typed general API is planned, and source generators already provide typed calls for narrow shapes.
