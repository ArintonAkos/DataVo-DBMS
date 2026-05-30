# Unity and Godot Integration

This page provides practical guidance for teams using DataVo in game and simulation workflows.

## Why game teams use DataVo

- Local in-process SQL for profiles, inventory, progression, and save-state metadata.
- Deterministic behavior during development and automated testing.
- No mandatory external DB service for local gameplay tooling scenarios.

## Typical use cases

- Player progression and unlock tracking
- Offline progression caches
- Local analytics snapshots during playtests
- Authoring tools and editors backed by SQL

## Deterministic in-memory workflows

For gameplay tests, runtime simulations, and local playtest tooling, use `StorageMode.InMemory` with snapshots:

```csharp
using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });

db.Execute("CREATE DATABASE GameTests");
db.Execute("USE GameTests");
db.Execute("CREATE TABLE PlayerState (Id INT PRIMARY KEY, Level INT)");
db.Execute("INSERT INTO PlayerState VALUES (1, 5)");

DataVoSnapshot baseline = db.CreateSnapshot();

db.Execute("UPDATE PlayerState SET Level = 10 WHERE Id = 1");
db.RestoreSnapshot(baseline);
```

This lets game teams seed one database, run a scenario, restore instantly, and reuse the same database surface in runtime code and automated tests.

## Unity integration approach

1. Add DataVo packages to your .NET project references.
2. Initialize DataVo context in your data service layer.
3. Keep SQL scripts versioned with your game data model.

Minimal pattern:

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var db = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.Disk
});

db.Execute("CREATE DATABASE GameData");
db.Execute("USE GameData");
```

## Godot integration approach

Use the same .NET embedding pattern in your Godot C# project.

Recommended structure:

- place DataVo initialization in a dedicated data subsystem
- expose async-safe commands via your game service layer
- keep schema migrations and startup SQL deterministic

## Storage mode guidance

- InMemory: tests, prototyping, temporary state
- Disk: persistent save and profile data

## Security and auth notes

For multi-profile or tool-user workflows, DataVo includes principal and grant commands:

- CREATE USER, CREATE ROLE
- GRANT, REVOKE
- LOGIN, LOGOUT

See [Security and Authentication](./security-and-authentication.md).

## Related pages

- [Getting Started](./getting-started.md)
- [Setup and Packaging](./setup-and-packaging.md)
- [Runtime Observability](./runtime-observability.md)
- [Source-Generated Compiled Queries](./compiled-queries.md)
- [Security and Authentication](./security-and-authentication.md)
