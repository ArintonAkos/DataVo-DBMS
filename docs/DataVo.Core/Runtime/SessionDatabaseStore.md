# SessionDatabaseStore

`SessionDatabaseStore` maps logical sessions to their currently selected database.

## Supported operations

- `Get(Guid)`
- `Set(Guid, string)`
- `Clear()`

## Why it exists

This type gives each engine instance its own session state instead of relying on a single process-wide mapping.
