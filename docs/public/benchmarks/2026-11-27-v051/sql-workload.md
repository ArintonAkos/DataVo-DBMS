## SQL workload, 1,000,000 orders (10,000 customers, 1,000,000 GUID rows)

| Operation | DataVo (LSM) [DataVo.Core net10.0] | SQLite | DuckDB | LiteDB |
|---|---|---|---|---|
| Load orders + customers | 1.91 s | 2.66 s | 517.4 ms | 8.18 s |
| Load GUID-keyed rows | 840.6 ms | 2.92 s | 649.8 ms | 6.20 s |
| Point read by INT key | 1.2 µs (p99 13.7 µs) | 2.0 µs (p99 2.6 µs) | 146.0 µs (p99 205.6 µs) | 23.1 µs (p99 39.5 µs) |
| Point read by GUID key | 1.2 µs (p99 2.3 µs) | 2.3 µs (p99 3.2 µs) | 124.2 µs (p99 155.9 µs) | 25.3 µs (p99 48.2 µs) |
| Secondary-index lookup (~100 rows) | 114.7 µs (p99 215.6 µs) | 90.3 µs (p99 127.4 µs) | 274.0 µs (p99 762.7 µs) | 416.0 µs (p99 2.4 ms) |
| Key range scan (100 rows) | 47.0 µs (p99 63.5 µs) | 7.4 µs (p99 12.2 µs) | 170.6 µs (p99 939.5 µs) | 531.5 µs (p99 1.2 ms) |
| COUNT with filter (full scan) | 36.8 ms (p99 98.3 ms) | 25.1 ms (p99 25.8 ms) | 422.7 µs (p99 479.0 µs) | 382.4 ms (p99 399.9 ms) |
| GROUP BY (full scan) | 52.8 ms (p99 65.6 ms) | 217.8 ms (p99 220.4 ms) | 964.2 µs (p99 996.0 µs) | 1.86 s (p99 2.00 s) |
| JOIN + GROUP BY | 100.3 ms (p99 105.1 ms) | 343.1 ms (p99 358.1 ms) | 1.5 ms (p99 1.6 ms) | 446.6 ms (p99 462.6 ms) |
| ORDER BY ... LIMIT 10 | 52.8 ms (p99 55.9 ms) | 31.9 ms (p99 32.0 ms) | 1.1 ms (p99 1.2 ms) | 836.1 ms (p99 858.3 ms) |
| UPDATE by key (autocommit) | 5.8 µs (p99 11.2 µs) | 6.0 µs (p99 22.2 µs) | 172.5 µs (p99 237.2 µs) | 43.5 µs (p99 268.2 µs) |
| DELETE by key (autocommit) | 10.9 µs (p99 15.3 µs) | 9.1 µs (p99 30.8 µs) | 173.2 µs (p99 313.8 µs) | 32.2 µs (p99 299.1 µs) |
| Close (flush to disk) | 296.3 ms | 3.7 ms | 175.2 ms | 10.4 ms |
| Size on disk (MB) | 73.2 MB | 75.3 MB | 99.1 MB | 384.2 MB |
| Reopen + first point read | 3.4 ms | 0.4 ms | 4.7 ms | 0.4 ms |
| Point read after reopen | 1.0 µs (p99 10.5 µs) | 2.4 µs (p99 5.2 µs) | 144.0 µs (p99 353.9 µs) | 14.7 µs (p99 235.3 µs) |

Repeated operations show the median and p99 latency of one call; single steps show total time.

- **DataVo (LSM) [DataVo.Core net10.0]**: LSM mode, commit log without fsync per commit; prepared statements with bound parameters, bulk load via InsertTypedBatch.
- **SQLite**: WAL, synchronous=NORMAL; prepared statements with parameters; bulk load in one transaction; GUIDs as 16-byte BLOB keys in a WITHOUT ROWID table.
- **DuckDB**: File database with its default WAL; appender for bulk load; prepared statements with parameters; GUIDs as UUID.
- **LiteDB**: Default journal; BsonDocument API with InsertBulk; GROUP BY and JOIN done in application code (LiteDB has no relational joins).

Result check (the same query must give the same answer everywhere):

- every other operation returned identical results on all engines.
