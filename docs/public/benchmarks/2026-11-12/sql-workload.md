## SQL workload, 1,000,000 orders (10,000 customers, 1,000,000 GUID rows)

| Operation | DataVo (LSM) [DataVo.Core net10.0] | SQLite | DuckDB | LiteDB |
|---|---|---|---|---|
| Load orders + customers | 1.87 s | 3.24 s | 538.5 ms | 8.94 s |
| Load GUID-keyed rows | 855.2 ms | 3.90 s | 656.8 ms | 6.25 s |
| Point read by INT key | 1.4 µs (p99 13.1 µs) | 2.0 µs (p99 3.3 µs) | 150.0 µs (p99 206.9 µs) | 23.2 µs (p99 37.0 µs) |
| Point read by GUID key | 1.2 µs (p99 2.3 µs) | 2.2 µs (p99 3.5 µs) | 126.4 µs (p99 167.5 µs) | 25.2 µs (p99 46.2 µs) |
| Secondary-index lookup (~100 rows) | 110.8 µs (p99 237.9 µs) | 86.6 µs (p99 108.3 µs) | 265.0 µs (p99 373.4 µs) | 413.2 µs (p99 2.6 ms) |
| Key range scan (100 rows) | 45.7 µs (p99 63.2 µs) | 7.4 µs (p99 12.8 µs) | 167.9 µs (p99 209.5 µs) | 549.2 µs (p99 1.6 ms) |
| COUNT with filter (full scan) | 46.8 ms (p99 60.1 ms) | 26.0 ms (p99 26.7 ms) | 397.8 µs (p99 434.5 µs) | 382.8 ms (p99 403.8 ms) |
| GROUP BY (full scan) | 87.2 ms (p99 87.5 ms) | 225.3 ms (p99 228.7 ms) | 905.1 µs (p99 973.3 µs) | 2.05 s (p99 2.12 s) |
| JOIN + GROUP BY | 133.3 ms (p99 143.8 ms) | 353.0 ms (p99 358.9 ms) | 1.5 ms (p99 6.0 ms) | 451.0 ms (p99 487.0 ms) |
| ORDER BY ... LIMIT 10 | 58.6 ms (p99 60.1 ms) | 36.9 ms (p99 45.5 ms) | 1.2 ms (p99 3.0 ms) | 923.5 ms (p99 942.4 ms) |
| UPDATE by key (autocommit) | 9.0 µs (p99 22.1 µs) | 6.5 µs (p99 27.9 µs) | 175.1 µs (p99 267.8 µs) | 42.0 µs (p99 253.8 µs) |
| DELETE by key (autocommit) | 13.0 µs (p99 21.0 µs) | 9.7 µs (p99 37.8 µs) | 187.4 µs (p99 354.8 µs) | 32.6 µs (p99 257.5 µs) |
| Close (flush to disk) | 302.6 ms | 4.1 ms | 184.9 ms | 9.8 ms |
| Size on disk (MB) | 73.2 MB | 75.3 MB | 99.1 MB | 384.2 MB |
| Reopen + first point read | 4.9 ms | 0.5 ms | 5.3 ms | 0.4 ms |
| Point read after reopen | 1.2 µs (p99 10.8 µs) | 2.4 µs (p99 7.0 µs) | 141.5 µs (p99 188.5 µs) | 14.3 µs (p99 215.4 µs) |

Repeated operations show the median and p99 latency of one call; single steps show total time.

- **DataVo (LSM) [DataVo.Core net10.0]**: LSM mode, commit log without fsync per commit; prepared statements with bound parameters, bulk load via InsertTypedBatch.
- **SQLite**: WAL, synchronous=NORMAL; prepared statements with parameters; bulk load in one transaction; GUIDs as 16-byte BLOB keys in a WITHOUT ROWID table.
- **DuckDB**: File database with its default WAL; appender for bulk load; prepared statements with parameters; GUIDs as UUID.
- **LiteDB**: Default journal; BsonDocument API with InsertBulk; GROUP BY and JOIN done in application code (LiteDB has no relational joins).

Result check (the same query must give the same answer everywhere):

- every other operation returned identical results on all engines.
