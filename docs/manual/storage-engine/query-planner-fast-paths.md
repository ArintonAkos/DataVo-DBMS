# Query Planner And Fast Paths

DataVo does not have a general cost-based optimizer. It has one general evaluator that can run every supported query, and a set of **typed paths**, each of which recognises a family of query shapes and runs them directly against stored rows. A `SELECT` is offered to the typed paths first. A typed path either produces exactly the rows the general evaluator would, in the same order, or declines; declining is never an error. This page describes what each path accepts and how it works, because whether a query lands on a typed path is the main factor in its speed.

## Why the distinction matters

The general evaluator materializes every row it reads as a dictionary from column name to boxed value, then evaluates `WHERE`, joins, grouping and projection over those dictionaries. That is flexible and slow: 2–4 KB of allocation per row read, so a million-row scan allocates gigabytes and takes seconds. The typed paths decode only the columns a query names, keep values unboxed, and build a result row only for rows that are returned. On the 1,000,000-row benchmark, moving `OR`, `LIKE` and multi-table joins onto typed paths took those queries from 1.7–5.0 s to 20–100 ms ([DataVo vs SQLite](/manual/performance/datavo-vs-sqlite)).

## The order in which paths are tried

```text
SELECT
  -> typed single-table path       (one table, no join)
       index point lookup | primary-key range seek | columnar aggregate | full typed scan
  -> typed join path               (INNER/LEFT joins on integer keys)
       hash join | index-driven probe | key-only bit set
  -> general path
       vector predicate path | nearest-neighbour path | general evaluator
```

Both typed paths decline inside an explicit transaction (they do not read through the transaction's MVCC snapshot), and for `SELECT DISTINCT`, common table expressions, computed expressions in the select list, and window functions. Those queries run on the general path, as do `RIGHT`/`FULL`/`CROSS` joins, joins on text or `GUID` keys, and an `OR` that spans two joined tables.

## Typed single-table path

Accepts one table whose `WHERE` is built from `AND`, `OR`, `NOT`, comparisons between a column and a literal or bound parameter (`=`, `<>`, `<`, `<=`, `>`, `>=`), `IS [NOT] NULL`, `LIKE` and `NOT LIKE`, with any mix of aggregates, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT` and `OFFSET`. It picks the cheapest access method:

- **Index point lookup.** An equality on the primary key or on a column with a single-column B-tree index reads the matching row keys in one index lookup and then reads those rows by key.
- **Primary-key range seek.** On a clustered LSM table, a range on the key (`BETWEEN`, or `>=` with `<`) becomes a seek into the SSTables and a sequential read of the range. On the InMemory engine, the integer key map answers ranges from a sorted copy of its keys, built by the first range query (8 bytes per key) and kept current by logging later inserts; a range covering more than a quarter of the table is scanned instead.
- **Columnar aggregate.** A `COUNT`, `SUM`, `AVG`, `MIN` or `MAX` without `GROUP BY`, whose filter compares numeric columns of a columnar index with values (`AND` and `OR`, `IS [NOT] NULL`), is answered from the index's column buffers with SIMD kernels. On LSM the flushed part comes from columnar SSTable segments, and a chunk whose zone map (min, max, NULL and NaN flags) rules out the predicate is skipped without being read.
- **Full typed scan.** Otherwise every row is visited in storage order, the filtered columns are decoded, and the predicate is evaluated.

Details that matter for correctness and cost:

- **Three-valued logic.** The parser pushes `NOT` down into the comparisons, and `WHERE` keeps only rows that evaluate to true, so treating any comparison with a NULL as false gives SQL's result for `AND`, `OR` and `NOT`.
- **Text on stored bytes.** On the LSM engine rows are serialized, and decoding a .NET string per row per text column used to cost 36–46 MB per million-row query. Now the row walk records where each filtered text value lies, and `=`, `<>`, `LIKE`, `NOT LIKE` and `IS [NOT] NULL` read the UTF-8 bytes in place. Byte equality is ordinal string equality for valid UTF-8, and a `LIKE` pattern's literal runs can only match whole characters, so the answer is the same. The one case bytes cannot decide is `_` against non-ASCII text, because `_` matches one UTF-16 character; that value is decoded and matched as before. A text column the query also returns is decoded only for matching rows.
- **`LIKE` matching.** `SqlLikePattern` handles prefix (`'S0042%'`), suffix and substring (`'%042-%'`) patterns with span searches, and any other pattern with a wildcard matcher.
- **Grouping.** Aggregates go into a hash table keyed by the group value. A one-column key is looked up by its value without boxing, and each aggregate lives in an array indexed by group number. `HAVING`, `ORDER BY` and `LIMIT` are applied to the groups before any group becomes a result row. An aggregate without `GROUP BY` uses its single group directly.
- **Top-k.** `ORDER BY ... LIMIT k` without a usable index keeps the k best rows in a bounded list. A scanned row's sort keys are compared with the kept rows in place, and only a row that enters the list has its keys copied, so the scan allocates nothing per row.
- **Floating-point sums on LSM.** A columnar `SUM` or `AVG` of a `FLOAT` or `DOUBLE` column is left to the row scan whenever the index cannot read in key order, which is the case on LSM while an unflushed tail exists. Floating-point addition is not associative, and a different order can change the last bits of the result. The columnar path answers only when it can return exactly the row scan's bits. Integer sums, counts, minimums and maximums are always pushed.

## Typed join path

Accepts `INNER` and `LEFT` joins of any number of tables on integer equality keys, with per-table `WHERE` conditions (including `OR` and `LIKE` within one table), aggregates with or without `GROUP BY`, and joined rows, either all of them or a top-N with `ORDER BY ... LIMIT`.

- **Hash join.** One table is streamed: the largest when every join is INNER, the `FROM` table when a `LEFT JOIN` must keep its rows or rows are returned without a top-N. Every other table is hashed on its join key, keeping only the columns read after the join (select list, grouping, aggregates, sort keys, conditions of a `LEFT`-joined table, and the next join key). Each streamed row is extended table by table; a `LEFT` step pads an unmatched row with NULLs.
- **Condition placement.** A condition on an INNER-joined table filters it while it is hashed. A condition on a `LEFT`-joined table is tested after the join, against the matched row or the NULL padding, which is what SQL means. (Applying it before the join was correctness bug `B1`.)
- **Key-only tables as bit sets.** A joined table that contributes nothing but its key, as in `LEFT JOIN Customers c ... WHERE c.Id IS NULL`, `COUNT(c.Id)`, or an existence-only inner join, needs only to answer "is this key present, and how many times?" Its keys are collected while the table is scanned. If they are distinct and span at most 64 values per key, they become a bit per value of their range (`DenseInt64Set`): 12.5 KB for 100,000 consecutive keys, where a hash table with counts took 2.9 MB, and a probe becomes a bit test. Repeated or scattered keys are hashed with their counts as before.
- **Index-driven probe.** When every join is INNER and a hashed table has few keys, the streamed table may be read through a single-column index on its join column instead of scanned. A sample of 32 keys estimates how many rows the index would name; the index is used when that is at most an eighth of the table. Rows are read in key order, the order a scan would visit them, so results are identical.

## General path

**Vector search.** `ORDER BY <vector column> <=> @q LIMIT k` (or `<->`), with or without a filter or a join, is planned here; see [Vector Search and Indexes](/manual/sql-language/vector-search-syntax#filtered-searches) for how filtered searches choose between the HNSW graph, an exact scan, and a filter-first scan. A distance threshold in `WHERE` (`Emb <=> @q < 0.25`) can use the index to prefilter candidates (`EnableVectorPredicateFastPath`).

**The general evaluator.** Everything else. Since v0.4 it runs a join once per query: `WHERE` is split into top-level `AND` conjuncts, conjuncts on one table filter it before the join only when that gives the same result, and the rest are tested on the joined rows. An optional Volcano-style operator pipeline (`EnableVolcanoExecution`, off by default) exists for planner experiments and spill-to-disk tests; leave it off.

## Compiled queries

Source-generated queries skip SQL parsing at run time. `[DataVoQuery]` methods compile a fixed SQL statement, and `DataVoTable<T>` lambdas (`Where`, `Count`, `Sum`, `Min`, `Max`, `Any`, `FirstOrDefault`, `SingleOrDefault`) are intercepted at build time and turned into code that reads cells from stored bytes. See [Roslyn Source Generators](/manual/client-interfaces/source-generators).

```csharp
public static partial class PlayerQueries
{
    [DataVoQuery("SELECT Id, Name, Level FROM Players WHERE Id = @id")]
    public static partial PlayerRow? GetPlayer(DataVoContext db, int id);

    // Narrow fixed-width single-row update: when the row is found through the integer
    // primary-key lane and can be patched safely, the stored bytes are patched in place.
    [DataVoQuery("UPDATE Players SET Level = @level WHERE Id = @id")]
    public static partial int UpdateLevel(DataVoContext db, int id, int level);
}
```

## SQL DATE row contract

SQL `DATE` columns materialize as `DataVo.Core.DataVoDate` in the row APIs. `CellValue.AsDate()` and compiled `GetDate`/`GetDateOrNull` return it; on modern .NET, `DataVoDate.FromDateOnly(...)`, `ToDateOnly()`, `CellValue.AsDateOnly()` and compiled `GetDateOnly`/`GetDateOnlyOrNull` convert explicitly. Persisted `DATE` bytes are unchanged.

## What the planner does not do

- It does not use a secondary index to drive a `GROUP BY`. SQLite reads an indexed key in order and groups without a hash table, which is why it is 2.3 times faster on a 125,000-group `GROUP BY` over an indexed column.
- It does not reorder joins by cost beyond choosing the streamed table and the index-driven probe described above.
- The typed paths keep no table statistics; the index-probe decision samples keys at run time.
- Typed paths do not run inside explicit transactions; reads in a transaction take the general path.
