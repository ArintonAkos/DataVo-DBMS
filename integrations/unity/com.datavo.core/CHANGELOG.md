# Changelog

## 0.2.0-rc.2

- The source generator is included: mark a model with `[DataVoTable]` and Unity writes its data access when it compiles.
- New `BIGINT` and `DOUBLE` columns hold real 64-bit values.
- **Breaking:** `CREATE TABLE` and `ALTER TABLE` now reject a type DataVo cannot store. Before, `DECIMAL` or `LONG` silently became text and `DATETIME` silently lost its time.

## 0.2.0-rc.1

- `DataVoContext.StreamQuery` reads large tables without holding the whole result in memory.
- A `SELECT` inside a transaction sees that transaction's own writes.
- `SELECT` lists can contain constants, with or without `FROM`.

## 0.1.0-preview.1

- First installable preview of DataVo Core for Unity.
- Disk and LSM storage for save data.
