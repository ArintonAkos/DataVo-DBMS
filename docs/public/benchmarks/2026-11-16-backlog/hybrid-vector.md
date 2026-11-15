## Hybrid vector + SQL: 50,000 documents x 384 dimensions (clustered), 2,000 authors, 200 held-out queries per shape

Machine: 12 logical cores, macOS 26.3.0, Arm64, .NET 10.0.3. Run at 2026-11-16 00:10 +02:00.

Recall@10 is measured against the exact top-10 among the rows that satisfy the query's WHERE clause (cosine distance). *Short* counts queries that returned fewer than min(10, matching rows) rows; *invalid* counts returned rows that fail the filter.

| Engine | Load + index build |
|---|---:|
| DataVo Flat [DataVo.Core net10.0] | 331 ms |
| DataVo HNSW [DataVo.Core net10.0] | 1,600 ms |
| SQLite + sqlite-vec | 644 ms |

### V0: top-10, no filter

| Engine | p50 | p99 | Recall@10 mean | min | Short | Invalid | Alloc/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| DataVo Flat [DataVo.Core net10.0] | 2.13 ms | 17.29 ms | 100.0 % | 100 % | 0 | 0 | 134.6 KB |
| DataVo HNSW [DataVo.Core net10.0] | 254.2 µs | 525.8 µs | 99.9 % | 90 % | 0 | 0 | 134.5 KB |
| SQLite + sqlite-vec | 18.50 ms | 19.20 ms | 100.0 % | 100 % | 0 | 0 | 2.2 KB |

- DataVo Flat [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 10 (max 10); expansion passes mean 0.00; full table scan 0/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.baseline 200/200
- DataVo HNSW [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 10 (max 10); expansion passes mean 0.00; full table scan 0/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.baseline 200/200
- DataVo Flat [DataVo.Core net10.0] SQL: `SELECT Id, Emb <=> @q AS dist FROM Docs ORDER BY dist LIMIT 10`
- DataVo HNSW [DataVo.Core net10.0] SQL: `SELECT Id, Emb <=> @q AS dist FROM Docs ORDER BY dist LIMIT 10`
- SQLite + sqlite-vec SQL: `SELECT id FROM vd WHERE emb MATCH @q AND k = 10 ORDER BY distance`

### V1: top-10 WHERE Category = c AND Price < 50 (about 5% of rows)

| Engine | p50 | p99 | Recall@10 mean | min | Short | Invalid | Alloc/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| DataVo Flat [DataVo.Core net10.0] | 3.61 ms | 8.73 ms | 100.0 % | 100 % | 0 | 0 | 5.0 MB |
| DataVo HNSW [DataVo.Core net10.0] | 6.71 ms | 19.27 ms | 100.0 % | 100 % | 0 | 0 | 5.1 MB |
| SQLite + sqlite-vec | 9.88 ms | 10.49 ms | 100.0 % | 100 % | 0 | 0 | 2.4 KB |

- DataVo Flat [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 286 (max 572); expansion passes mean 0.10; full table scan 0/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.adaptive 200/200
- DataVo HNSW [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 286 (max 572); expansion passes mean 0.11; full table scan 0/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.adaptive 200/200
- DataVo Flat [DataVo.Core net10.0] SQL: `SELECT Id, Emb <=> @q AS dist FROM Docs WHERE Category = @c AND Price < @p ORDER BY dist LIMIT 10`
- DataVo HNSW [DataVo.Core net10.0] SQL: `SELECT Id, Emb <=> @q AS dist FROM Docs WHERE Category = @c AND Price < @p ORDER BY dist LIMIT 10`
- SQLite + sqlite-vec SQL: `SELECT id FROM vd WHERE emb MATCH @q AND k = 10 AND category = @c AND price < @p ORDER BY distance`

### V2: top-10 WHERE Price < 50 (about 50% of rows)

| Engine | p50 | p99 | Recall@10 mean | min | Short | Invalid | Alloc/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| DataVo Flat [DataVo.Core net10.0] | 2.11 ms | 4.25 ms | 100.0 % | 100 % | 0 | 0 | 551.6 KB |
| DataVo HNSW [DataVo.Core net10.0] | 735.0 µs | 2.00 ms | 100.0 % | 90 % | 0 | 0 | 551.6 KB |
| SQLite + sqlite-vec | 14.49 ms | 15.18 ms | 100.0 % | 100 % | 0 | 0 | 2.3 KB |

- DataVo Flat [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 29 (max 58); expansion passes mean 0.05; full table scan 0/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.adaptive 200/200
- DataVo HNSW [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 29 (max 58); expansion passes mean 0.05; full table scan 0/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.adaptive 200/200
- DataVo Flat [DataVo.Core net10.0] SQL: `SELECT Id, Emb <=> @q AS dist FROM Docs WHERE Price < @p ORDER BY dist LIMIT 10`
- DataVo HNSW [DataVo.Core net10.0] SQL: `SELECT Id, Emb <=> @q AS dist FROM Docs WHERE Price < @p ORDER BY dist LIMIT 10`
- SQLite + sqlite-vec SQL: `SELECT id FROM vd WHERE emb MATCH @q AND k = 10 AND price < @p ORDER BY distance`

### V3: V1 + JOIN Authors (filter on the documents)

| Engine | p50 | p99 | Recall@10 mean | min | Short | Invalid | Alloc/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| DataVo Flat [DataVo.Core net10.0] | 4.76 ms | 13.05 ms | 100.0 % | 100 % | 0 | 0 | 4.9 MB |
| DataVo HNSW [DataVo.Core net10.0] | 7.77 ms | 22.53 ms | 100.0 % | 100 % | 0 | 0 | 4.9 MB |
| SQLite + sqlite-vec | 9.86 ms | 10.53 ms | 100.0 % | 100 % | 0 | 0 | 2.4 KB |

- DataVo Flat [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 286 (max 1073); expansion passes mean 0.10; full table scan 200/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.adaptive 200/200
- DataVo HNSW [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 286 (max 1073); expansion passes mean 0.11; full table scan 200/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.adaptive 200/200
- DataVo Flat [DataVo.Core net10.0] SQL: `SELECT d.Id, a.Name, d.Emb <=> @q AS dist FROM Docs d JOIN Authors a ON d.AuthorId = a.Id WHERE d.Category = @c AND d.Price < @p ORDER BY dist LIMIT 10`
- DataVo HNSW [DataVo.Core net10.0] SQL: `SELECT d.Id, a.Name, d.Emb <=> @q AS dist FROM Docs d JOIN Authors a ON d.AuthorId = a.Id WHERE d.Category = @c AND d.Price < @p ORDER BY dist LIMIT 10`
- SQLite + sqlite-vec SQL: `WITH knn AS MATERIALIZED (SELECT id, distance, author_id FROM vd WHERE emb MATCH @q AND k = 10 AND category = @c AND price < @p) SELECT knn.id, a.Name FROM knn JOIN Authors a ON a.Id = knn.author_id ORDER BY knn.distance, knn.id`

### V4: JOIN Authors WHERE a.Country = k (about 10% of rows, filter on the joined table)

| Engine | p50 | p99 | Recall@10 mean | min | Short | Invalid | Alloc/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| DataVo Flat [DataVo.Core net10.0] | 7.23 ms | 17.71 ms | 100.0 % | 100 % | 0 | 0 | 4.6 MB |
| DataVo HNSW [DataVo.Core net10.0] | 6.15 ms | 14.79 ms | 100.0 % | 100 % | 0 | 0 | 4.5 MB |
| SQLite + sqlite-vec | 13.20 ms | 13.74 ms | 100.0 % | 100 % | 0 | 0 | 2.2 KB |

- DataVo Flat [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 150 (max 600); expansion passes mean 1.49; full table scan 200/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.baseline 200/200
- DataVo HNSW [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 150 (max 600); expansion passes mean 1.48; full table scan 200/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.baseline 200/200
- DataVo Flat [DataVo.Core net10.0] SQL: `SELECT d.Id, a.Name, d.Emb <=> @q AS dist FROM Docs d JOIN Authors a ON d.AuthorId = a.Id WHERE a.Country = @k ORDER BY dist LIMIT 10`
- DataVo HNSW [DataVo.Core net10.0] SQL: `SELECT d.Id, a.Name, d.Emb <=> @q AS dist FROM Docs d JOIN Authors a ON d.AuthorId = a.Id WHERE a.Country = @k ORDER BY dist LIMIT 10`
- SQLite + sqlite-vec SQL: `WITH knn AS MATERIALIZED (SELECT id, distance, author_id FROM vd WHERE emb MATCH @q AND k = 10 AND author_id IN (SELECT Id FROM Authors WHERE Country = @k)) SELECT knn.id, a.Name FROM knn JOIN Authors a ON a.Id = knn.author_id ORDER BY knn.distance, knn.id`

### Engines

- **DataVo Flat [DataVo.Core net10.0]**: LSM, LsmStrictFsync=false; CREATE INDEX ... USING FLAT (default parameters); InsertTypedBatch load; prepared SQL with the query vector bound as float[].
- **DataVo HNSW [DataVo.Core net10.0]**: LSM, LsmStrictFsync=false; CREATE INDEX ... USING HNSW (default parameters); InsertTypedBatch load; prepared SQL with the query vector bound as float[].
- **SQLite + sqlite-vec**: File, WAL, synchronous=NORMAL; vec0(id, emb float[d] distance_metric=cosine, category, price, author_id as metadata columns); exact KNN with metadata filters; joins via MATERIALIZED CTE; author-country filter: KNN with author_id IN (SELECT Id FROM Authors WHERE Country = @k).
