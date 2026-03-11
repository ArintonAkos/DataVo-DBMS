# DataVoEngine

`DataVoEngine` is the runtime root for a single engine instance.

## Responsibilities

- own storage, catalog, index, lock, and transaction services
- expose an engine-scoped `Current()` for execution flows
- initialize recovery when disk mode and WAL are enabled
- provide a migration path away from legacy process-wide singletons

## Typical usage

```csharp
var engine = DataVoEngine.Initialize(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});
```

## Important properties

- `StorageContext`
- `Catalog`
- `Sessions`
- `TransactionManager`
- `LockManager`
- `IndexManager`
