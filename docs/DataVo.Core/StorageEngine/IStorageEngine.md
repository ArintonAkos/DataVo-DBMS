# IStorageEngine

`IStorageEngine` defines the physical row storage contract used by the engine.

## Responsibilities

- append row payloads
- read individual rows
- stream all rows in a table
- tombstone or delete rows
- drop tables and databases
- compact a table into a rewritten row set

## Implementations

- `StorageEngine/Memory/InMemoryStorageEngine.cs`
- `StorageEngine/Disk/DiskStorageEngine.cs`

## Contract design note

The interface operates on serialized `byte[]` row payloads, not rich domain objects. This keeps storage decoupled from catalog and query concerns.
