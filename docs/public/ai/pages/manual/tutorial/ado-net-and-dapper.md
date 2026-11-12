# ADO.NET & Dapper Tutorial

> Source route: /manual/tutorial/ado-net-and-dapper
> Source file: manual/tutorial/ado-net-and-dapper.md

This tutorial builds a small player store through the ADO.NET provider, then reads it back with Dapper. It uses LSM storage, so the data survives a restart. Reference details live in [ADO.NET Provider](/manual/client-interfaces/ado-net) and [Dapper](/manual/client-interfaces/dapper).

## 1. Create a project

```bash
dotnet new console -n PlayerStore
cd PlayerStore
dotnet add package DataVo.Data --prerelease
dotnet add package Dapper
```

## 2. Open a persistent database

```csharp
using Dapper;
using DataVo.Data;

using var connection = new DataVoConnection("StorageMode=Lsm;DataSource=./player-store");
connection.Open();
```

`StorageMode=Lsm` selects the LSM-Tree engine with strict fsync by default. `DataSource` is both the data directory and the database name. See [Configuration Reference](/manual/reference/configuration).

## 3. Create the schema

```csharp
connection.Execute(
    "CREATE TABLE IF NOT EXISTS Players (Id INT PRIMARY KEY, Name VARCHAR(40), Level INT)");
```

## 4. Insert in a transaction

```csharp
using (var transaction = connection.BeginTransaction())
{
    for (int id = 1; id <= 5; id++)
    {
        connection.Execute(
            "INSERT INTO Players (Id, Name, Level) VALUES (@Id, @Name, @Level)",
            new { Id = id, Name = $"player-{id}", Level = id * 3 },
            transaction);
    }

    transaction.Commit();
}
```

Run the program twice: the second run fails on the primary key, which is the point. The rows from the first run were persisted.

## 5. Query with Dapper

```csharp
IEnumerable<Player> veterans = connection.Query<Player>(
    "SELECT Id, Name, Level FROM Players WHERE Level >= @min ORDER BY Level DESC",
    new { min = 9 });

foreach (Player p in veterans)
{
    Console.WriteLine($"{p.Name}: level {p.Level}");
}

public sealed class Player
{
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public int Level { get; set; }
}
```

## 6. Drop to raw ADO.NET when you want to

Dapper and plain ADO.NET share the connection, so you can use either per call.

```csharp
using var command = connection.CreateCommand();
command.CommandText = "SELECT COUNT(*) FROM Players WHERE Level >= @min";
command.Parameters.AddWithValue("@min", 9);
Console.WriteLine(command.ExecuteScalar());
```

## Where to go next

- Move a hot query to [compiled LINQ](/manual/client-interfaces/source-generators).
- Use [vector search](/manual/tutorial/vector-search-example) for embeddings in the same database.
- Read [WAL & Durability](/manual/storage-engine/wal-and-durability) before relying on persisted data.
