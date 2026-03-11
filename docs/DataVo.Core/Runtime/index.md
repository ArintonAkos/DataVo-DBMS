# Runtime Overview

The `Runtime` module owns the live engine-scoped services used during query execution.

## Responsibilities

- hold the active `DataVoEngine`
- manage session-to-database bindings
- expose catalog operations through `EngineCatalog`
- persist catalog metadata through `CatalogStore`

## Components

- `DataVoEngine.cs`
- `EngineCatalog.cs`
- `CatalogStore.cs`
- `SessionDatabaseStore.cs`

## File Documentation

- [DataVoEngine](./DataVoEngine.md)
- [EngineCatalog](./EngineCatalog.md)
- [SessionDatabaseStore](./SessionDatabaseStore.md)

## Contributor guidance

This folder is the bridge between parser actions and durable engine services. When the engine shifts further away from legacy singletons, much of that migration work will land here.
