# Cache Overview

The `Cache` module contains small, process-local helpers used by some legacy execution paths.

## Component breakdown

| Component (File)  | Purpose |
| ----------------- | ------- |
| `CacheStorage.cs` | Thread-safe map of `sessionId -> active database name`. |

## File documentation

- [CacheStorage](./CacheStorage.md)

## Notes

- This is not a general-purpose page/data cache.
- Newer engine flows prefer the engine-owned session store (`Runtime/SessionDatabaseStore.cs` via `DataVoEngine.Sessions`).
