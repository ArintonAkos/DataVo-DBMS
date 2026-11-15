## Hybrid vector + SQL: 50,000 documents x 384 dimensions (clustered), 2,000 authors, 200 held-out queries per shape

Machine: 12 logical cores, macOS 26.3.0, Arm64, .NET 10.0.3. Run at 2026-11-14 17:09 +02:00.

Recall@10 is measured against the exact top-10 among the rows that satisfy the query's WHERE clause (cosine distance). *Short* counts queries that returned fewer than min(10, matching rows) rows; *invalid* counts returned rows that fail the filter.

| Engine | Load + index build |
|---|---:|
| DataVo Flat [DataVo.Core net10.0] | 345 ms |
| DataVo HNSW [DataVo.Core net10.0] | 1,935 ms |
| SQLite + sqlite-vec | 643 ms |

### V0: top-10, no filter

| Engine | p50 | p99 | Recall@10 mean | min | Short | Invalid | Alloc/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| DataVo Flat [DataVo.Core net10.0] | 2.59 ms | 16.10 ms | 100.0 % | 100 % | 0 | 0 | 1.2 MB |
| DataVo HNSW [DataVo.Core net10.0] | 662.2 µs | 1.03 ms | 81.6 % | 0 % | 0 | 0 | 1.2 MB |
| SQLite + sqlite-vec | 18.61 ms | 19.85 ms | 100.0 % | 100 % | 0 | 0 | 2.2 KB |

- DataVo Flat [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 10 (max 10); expansion passes mean 0.00; full table scan 0/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.baseline 200/200
- DataVo HNSW [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 10 (max 10); expansion passes mean 0.00; full table scan 0/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.baseline 200/200
- DataVo Flat [DataVo.Core net10.0] SQL: `SELECT Id, Emb <=> @q AS dist FROM Docs ORDER BY dist LIMIT 10`
- DataVo HNSW [DataVo.Core net10.0] SQL: `SELECT Id, Emb <=> @q AS dist FROM Docs ORDER BY dist LIMIT 10`
- SQLite + sqlite-vec SQL: `SELECT id FROM vd WHERE emb MATCH @q AND k = 10 ORDER BY distance`

### V1: top-10 WHERE Category = c AND Price < 50 (about 5% of rows)

| Engine | p50 | p99 | Recall@10 mean | min | Short | Invalid | Alloc/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| DataVo Flat [DataVo.Core net10.0] | 4.24 ms | 9.72 ms | 100.0 % | 100 % | 0 | 0 | 6.8 MB |
| DataVo HNSW [DataVo.Core net10.0] | 7.10 ms | 19.72 ms | 92.0 % | 0 % | 4 | 0 | 6.6 MB |
| SQLite + sqlite-vec | 9.79 ms | 10.33 ms | 100.0 % | 100 % | 0 | 0 | 2.4 KB |

- DataVo Flat [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 286 (max 572); expansion passes mean 0.10; full table scan 0/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.adaptive 200/200
- DataVo HNSW [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 286 (max 1144); expansion passes mean 0.14; full table scan 0/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.adaptive 200/200
- DataVo Flat [DataVo.Core net10.0] SQL: `SELECT Id, Emb <=> @q AS dist FROM Docs WHERE Category = @c AND Price < @p ORDER BY dist LIMIT 10`
- DataVo HNSW [DataVo.Core net10.0] SQL: `SELECT Id, Emb <=> @q AS dist FROM Docs WHERE Category = @c AND Price < @p ORDER BY dist LIMIT 10`
- SQLite + sqlite-vec SQL: `SELECT id FROM vd WHERE emb MATCH @q AND k = 10 AND category = @c AND price < @p ORDER BY distance`

### V2: top-10 WHERE Price < 50 (about 50% of rows)

| Engine | p50 | p99 | Recall@10 mean | min | Short | Invalid | Alloc/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| DataVo Flat [DataVo.Core net10.0] | 2.75 ms | 5.36 ms | 100.0 % | 100 % | 0 | 0 | 2.2 MB |
| DataVo HNSW [DataVo.Core net10.0] | 1.31 ms | 2.69 ms | 83.0 % | 0 % | 0 | 0 | 2.1 MB |
| SQLite + sqlite-vec | 14.58 ms | 15.25 ms | 100.0 % | 100 % | 0 | 0 | 2.3 KB |

- DataVo Flat [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 29 (max 58); expansion passes mean 0.05; full table scan 0/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.adaptive 200/200
- DataVo HNSW [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 29 (max 58); expansion passes mean 0.02; full table scan 0/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.adaptive 200/200
- DataVo Flat [DataVo.Core net10.0] SQL: `SELECT Id, Emb <=> @q AS dist FROM Docs WHERE Price < @p ORDER BY dist LIMIT 10`
- DataVo HNSW [DataVo.Core net10.0] SQL: `SELECT Id, Emb <=> @q AS dist FROM Docs WHERE Price < @p ORDER BY dist LIMIT 10`
- SQLite + sqlite-vec SQL: `SELECT id FROM vd WHERE emb MATCH @q AND k = 10 AND price < @p ORDER BY distance`

### V3: V1 + JOIN Authors (filter on the documents)

| Engine | p50 | p99 | Recall@10 mean | min | Short | Invalid | Alloc/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| DataVo Flat [DataVo.Core net10.0] | 5.31 ms | 14.26 ms | 100.0 % | 100 % | 0 | 0 | 6.6 MB |
| DataVo HNSW [DataVo.Core net10.0] | 8.40 ms | 273.19 ms | 93.7 % | 0 % | 0 | 0 | 16.5 MB |
| SQLite + sqlite-vec | 9.83 ms | 10.11 ms | 100.0 % | 100 % | 0 | 0 | 2.4 KB |

- DataVo Flat [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 286 (max 1073); expansion passes mean 0.10; full table scan 200/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.adaptive 200/200
- DataVo HNSW [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 286 (max 16088); expansion passes mean 0.08; full table scan 200/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.adaptive 200/200, hybrid.orderby.join.reject.expansion_exhausted 4/200
- DataVo Flat [DataVo.Core net10.0] SQL: `SELECT d.Id, a.Name, d.Emb <=> @q AS dist FROM Docs d JOIN Authors a ON d.AuthorId = a.Id WHERE d.Category = @c AND d.Price < @p ORDER BY dist LIMIT 10`
- DataVo HNSW [DataVo.Core net10.0] SQL: `SELECT d.Id, a.Name, d.Emb <=> @q AS dist FROM Docs d JOIN Authors a ON d.AuthorId = a.Id WHERE d.Category = @c AND d.Price < @p ORDER BY dist LIMIT 10`
- SQLite + sqlite-vec SQL: `WITH knn AS MATERIALIZED (SELECT id, distance, author_id FROM vd WHERE emb MATCH @q AND k = 10 AND category = @c AND price < @p) SELECT knn.id, a.Name FROM knn JOIN Authors a ON a.Id = knn.author_id ORDER BY knn.distance, knn.id`

### V4: JOIN Authors WHERE a.Country = k (about 10% of rows, filter on the joined table)

| Engine | p50 | p99 | Recall@10 mean | min | Short | Invalid | Alloc/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| DataVo Flat [DataVo.Core net10.0] | 8.19 ms | 16.51 ms | 100.0 % | 100 % | 0 | 0 | 6.4 MB |
| DataVo HNSW [DataVo.Core net10.0] | 6.71 ms | 360.98 ms | 91.4 % | 0 % | 0 | 0 | 20.7 MB |
| SQLite + sqlite-vec | 13.20 ms | 13.48 ms | 100.0 % | 100 % | 0 | 0 | 2.2 KB |

- DataVo Flat [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 150 (max 600); expansion passes mean 1.49; full table scan 200/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.baseline 200/200
- DataVo HNSW [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 150 (max 600); expansion passes mean 1.47; full table scan 200/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.baseline 200/200, hybrid.orderby.join.reject.expansion_exhausted 4/200
- DataVo Flat [DataVo.Core net10.0] SQL: `SELECT d.Id, a.Name, d.Emb <=> @q AS dist FROM Docs d JOIN Authors a ON d.AuthorId = a.Id WHERE a.Country = @k ORDER BY dist LIMIT 10`
- DataVo HNSW [DataVo.Core net10.0] SQL: `SELECT d.Id, a.Name, d.Emb <=> @q AS dist FROM Docs d JOIN Authors a ON d.AuthorId = a.Id WHERE a.Country = @k ORDER BY dist LIMIT 10`
- SQLite + sqlite-vec SQL: `WITH knn AS MATERIALIZED (SELECT id, distance, author_id FROM vd WHERE emb MATCH @q AND k = 10 AND author_id IN (SELECT Id FROM Authors WHERE Country = @k)) SELECT knn.id, a.Name FROM knn JOIN Authors a ON a.Id = knn.author_id ORDER BY knn.distance, knn.id`

### Engines

- **DataVo Flat [DataVo.Core net10.0]**: LSM, LsmStrictFsync=false; CREATE INDEX ... USING FLAT (default parameters); InsertTypedBatch load; prepared SQL with the query vector bound as float[].
- **DataVo HNSW [DataVo.Core net10.0]**: LSM, LsmStrictFsync=false; CREATE INDEX ... USING HNSW (default parameters); InsertTypedBatch load; prepared SQL with the query vector bound as float[].
- **SQLite + sqlite-vec**: File, WAL, synchronous=NORMAL; vec0(id, emb float[d] distance_metric=cosine, category, price, author_id as metadata columns); exact KNN with metadata filters; joins via MATERIALIZED CTE; author-country filter: KNN with author_id IN (SELECT Id FROM Authors WHERE Country = @k).
