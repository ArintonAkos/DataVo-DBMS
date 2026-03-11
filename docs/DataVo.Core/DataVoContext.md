# DataVoContext

`DataVoContext` is the simplest embedding entry point for developers who want to execute SQL against a dedicated `DataVoEngine` instance.

## What it does

- initializes an engine from `DataVoConfig`
- manages a default session identifier
- executes SQL through `QueryEngine`
- disposes engine-owned resources when the context is disposed

## Typical usage

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var context = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});

context.Execute("CREATE DATABASE Demo");
context.Execute("USE Demo");
context.Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR(50))");
var results = context.Execute("SELECT * FROM Users");
```

## When to use it

Use `DataVoContext` when you want a friendly application-facing API. Contributors working deeper in the engine will usually interact directly with `DataVoEngine`, `StorageContext`, and parser actions.

## Related files

- `Runtime/DataVoEngine.cs`
- `Parser/QueryEngine.cs`
- `StorageEngine/Config/DataVoConfig.cs`
