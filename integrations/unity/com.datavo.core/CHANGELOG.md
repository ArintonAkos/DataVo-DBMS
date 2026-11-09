# Changelog

## 0.2.0-rc.1

- `DataVoContext.StreamQuery` reads large tables without holding the whole result in memory.
- A `SELECT` inside a transaction sees that transaction's own writes.
- `SELECT` lists can contain constants, with or without `FROM`.

## 0.1.0-preview.1

- First installable preview of DataVo Core for Unity.
- Disk and LSM storage for save data.
