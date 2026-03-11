# CacheStorage

`CacheStorage` is a lightweight compatibility cache that maps a session identifier to the active database name.

## Responsibilities

- remember the current database for a session
- support older execution flows that still rely on process-wide session state
- provide a simple bridge while engine-local session stores continue to replace legacy paths

## Supported operations

- `Get(Guid)`
- `Set(Guid, string)`
- `Clear()`

## Example

```csharp
Guid sessionId = Guid.NewGuid();
CacheStorage.Set(sessionId, "DemoDb");
string? currentDb = CacheStorage.Get(sessionId);
```

## Notes for contributors

Newer code should prefer the engine-owned session store in `Runtime/SessionDatabaseStore.cs` when possible.
