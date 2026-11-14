## Hybrid vector + SQL: 20,000 documents x 1536 dimensions (clustered), 2,000 authors, 200 held-out queries per shape

Machine: 12 logical cores, macOS 26.3.0, Arm64, .NET 10.0.3. Run at 2026-11-14 13:25 +02:00.

Recall@10 is measured against the exact top-10 among the rows that satisfy the query's WHERE clause (cosine distance). *Short* counts queries that returned fewer than min(10, matching rows) rows; *invalid* counts returned rows that fail the filter.

| Engine | Load + index build |
|---|---:|
| DataVo Flat [DataVo.Core net10.0] | 481 ms |
| DataVo HNSW [DataVo.Core net10.0] | 1,862 ms |
| SQLite + sqlite-vec | 4,011 ms |

### V0: top-10, no filter

| Engine | p50 | p99 | Recall@10 mean | min | Short | Invalid | Alloc/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| DataVo Flat [DataVo.Core net10.0] | 6.06 ms | 27.23 ms | 100.0 % | 100 % | 0 | 0 | 4.7 MB |
| DataVo HNSW [DataVo.Core net10.0] | 2.22 ms | 2.58 ms | 86.8 % | 0 % | 0 | 0 | 4.7 MB |
| SQLite + sqlite-vec | 32.72 ms | 35.46 ms | 100.0 % | 100 % | 0 | 0 | 6.7 KB |

- DataVo Flat [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 10 (max 10); expansion passes mean 0.00; full table scan 0/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.baseline 200/200
- DataVo HNSW [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 10 (max 10); expansion passes mean 0.00; full table scan 0/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.baseline 200/200
- DataVo Flat [DataVo.Core net10.0] SQL: `SELECT Id, Emb <=> @q AS dist FROM Docs ORDER BY dist LIMIT 10`
- DataVo HNSW [DataVo.Core net10.0] SQL: `SELECT Id, Emb <=> @q AS dist FROM Docs ORDER BY dist LIMIT 10`
- SQLite + sqlite-vec SQL: `SELECT id FROM vd WHERE emb MATCH @q AND k = 10 ORDER BY distance`

### V1: top-10 WHERE Category = c AND Price < 50 (about 5% of rows)

| Engine | p50 | p99 | Recall@10 mean | min | Short | Invalid | Alloc/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| DataVo Flat [DataVo.Core net10.0] | 9.70 ms | 20.78 ms | 100.0 % | 100 % | 0 | 0 | 23.9 MB |
| DataVo HNSW [DataVo.Core net10.0] | 11.92 ms | 29.30 ms | 89.9 % | 0 % | 18 | 0 | 23.1 MB |
| SQLite + sqlite-vec | 16.09 ms | 17.80 ms | 100.0 % | 100 % | 0 | 0 | 6.9 KB |

- DataVo Flat [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 286 (max 572); expansion passes mean 0.12; full table scan 0/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.adaptive 200/200
- DataVo HNSW [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 286 (max 1144); expansion passes mean 0.37; full table scan 0/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.adaptive 200/200
- DataVo Flat [DataVo.Core net10.0] SQL: `SELECT Id, Emb <=> @q AS dist FROM Docs WHERE Category = @c AND Price < @p ORDER BY dist LIMIT 10`
- DataVo HNSW [DataVo.Core net10.0] SQL: `SELECT Id, Emb <=> @q AS dist FROM Docs WHERE Category = @c AND Price < @p ORDER BY dist LIMIT 10`
- SQLite + sqlite-vec SQL: `SELECT id FROM vd WHERE emb MATCH @q AND k = 10 AND category = @c AND price < @p ORDER BY distance`

### V2: top-10 WHERE Price < 50 (about 50% of rows)

| Engine | p50 | p99 | Recall@10 mean | min | Short | Invalid | Alloc/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| DataVo Flat [DataVo.Core net10.0] | 6.83 ms | 11.98 ms | 100.0 % | 100 % | 0 | 0 | 7.9 MB |
| DataVo HNSW [DataVo.Core net10.0] | 3.64 ms | 6.21 ms | 88.4 % | 0 % | 0 | 0 | 7.9 MB |
| SQLite + sqlite-vec | 24.22 ms | 25.43 ms | 100.0 % | 100 % | 0 | 0 | 6.8 KB |

- DataVo Flat [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 29 (max 58); expansion passes mean 0.02; full table scan 0/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.adaptive 200/200
- DataVo HNSW [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 29 (max 58); expansion passes mean 0.02; full table scan 0/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.adaptive 200/200
- DataVo Flat [DataVo.Core net10.0] SQL: `SELECT Id, Emb <=> @q AS dist FROM Docs WHERE Price < @p ORDER BY dist LIMIT 10`
- DataVo HNSW [DataVo.Core net10.0] SQL: `SELECT Id, Emb <=> @q AS dist FROM Docs WHERE Price < @p ORDER BY dist LIMIT 10`
- SQLite + sqlite-vec SQL: `SELECT id FROM vd WHERE emb MATCH @q AND k = 10 AND price < @p ORDER BY distance`

### V3: V1 + JOIN Authors (filter on the documents)

| Engine | p50 | p99 | Recall@10 mean | min | Short | Invalid | Alloc/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| DataVo Flat [DataVo.Core net10.0] | 4.72 ms | 10.30 ms | 5.5 % | 0 % | 200 | 0 | 1.8 MB |
| DataVo HNSW [DataVo.Core net10.0] | 884.4 µs | 6.21 ms | 5.2 % | 0 % | 200 | 0 | 1.8 MB |
| SQLite + sqlite-vec | 16.06 ms | 17.05 ms | 100.0 % | 100 % | 0 | 0 | 6.9 KB |

- DataVo Flat [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 10 (max 10); expansion passes mean 0.00; full table scan 92/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.baseline 200/200
- DataVo HNSW [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 10 (max 10); expansion passes mean 0.00; full table scan 97/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.baseline 200/200
- DataVo Flat [DataVo.Core net10.0] SQL: `SELECT d.Id, a.Name, d.Emb <=> @q AS dist FROM Docs d JOIN Authors a ON d.AuthorId = a.Id WHERE d.Category = @c AND d.Price < @p ORDER BY dist LIMIT 10`
- DataVo HNSW [DataVo.Core net10.0] SQL: `SELECT d.Id, a.Name, d.Emb <=> @q AS dist FROM Docs d JOIN Authors a ON d.AuthorId = a.Id WHERE d.Category = @c AND d.Price < @p ORDER BY dist LIMIT 10`
- SQLite + sqlite-vec SQL: `WITH knn AS MATERIALIZED (SELECT id, distance, author_id FROM vd WHERE emb MATCH @q AND k = 10 AND category = @c AND price < @p) SELECT knn.id, a.Name FROM knn JOIN Authors a ON a.Id = knn.author_id ORDER BY knn.distance, knn.id`

### V4: JOIN Authors WHERE a.Country = k (about 10% of rows, filter on the joined table)

| Engine | p50 | p99 | Recall@10 mean | min | Short | Invalid | Alloc/query |
|---|---:|---:|---:|---:|---:|---:|---:|
| DataVo Flat [DataVo.Core net10.0] | 7.38 ms | 10.43 ms | 11.1 % | 0 % | 0 | 1779 | 6.9 MB |
| DataVo HNSW [DataVo.Core net10.0] | 3.75 ms | 5.98 ms | 10.0 % | 0 % | 0 | 1786 | 6.9 MB |
| SQLite + sqlite-vec | 18.17 ms | 19.21 ms | 100.0 % | 100 % | 0 | 0 | 6.8 KB |

- DataVo Flat [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 10 (max 10); expansion passes mean 0.00; full table scan 200/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.baseline 200/200
- DataVo HNSW [DataVo.Core net10.0] planner: vector index 200/200; candidates (top-k) median 10 (max 10); expansion passes mean 0.00; full table scan 200/200; routes: hybrid.orderby.accept 200/200, hybrid.orderby.initial_topk.baseline 200/200
- DataVo Flat [DataVo.Core net10.0] SQL: `SELECT d.Id, a.Name, d.Emb <=> @q AS dist FROM Docs d JOIN Authors a ON d.AuthorId = a.Id WHERE a.Country = @k ORDER BY dist LIMIT 10`
- DataVo HNSW [DataVo.Core net10.0] SQL: `SELECT d.Id, a.Name, d.Emb <=> @q AS dist FROM Docs d JOIN Authors a ON d.AuthorId = a.Id WHERE a.Country = @k ORDER BY dist LIMIT 10`
- SQLite + sqlite-vec SQL: `WITH knn AS MATERIALIZED (SELECT id, distance, author_id FROM vd WHERE emb MATCH @q AND k = 10 AND author_id IN (SELECT Id FROM Authors WHERE Country = @k)) SELECT knn.id, a.Name FROM knn JOIN Authors a ON a.Id = knn.author_id ORDER BY knn.distance, knn.id`

### Engines

- **DataVo Flat [DataVo.Core net10.0]**: LSM, LsmStrictFsync=false; CREATE INDEX ... USING FLAT (default parameters); InsertTypedBatch load; prepared SQL with the query vector bound as float[].
- **DataVo HNSW [DataVo.Core net10.0]**: LSM, LsmStrictFsync=false; CREATE INDEX ... USING HNSW (default parameters); InsertTypedBatch load; prepared SQL with the query vector bound as float[].
- **SQLite + sqlite-vec**: File, WAL, synchronous=NORMAL; vec0(id, emb float[d] distance_metric=cosine, category, price, author_id as metadata columns); exact KNN with metadata filters; joins via MATERIALIZED CTE; author-country filter: KNN with author_id IN (SELECT Id FROM Authors WHERE Country = @k).
