# EngineCatalog

`EngineCatalog` is the engine-facing façade over catalog persistence and metadata queries.

## Why it exists

It keeps parser and execution code away from the lower-level catalog store implementation while still exposing a focused API for schema and index operations.

## Supported operations

- database and table existence checks
- create/drop database and table metadata
- add/drop/modify columns
- create/drop indexes
- inspect keys, columns, indexes, and schema versions
