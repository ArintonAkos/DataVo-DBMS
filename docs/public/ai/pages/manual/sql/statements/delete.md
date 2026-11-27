# DELETE

> Source route: /manual/sql/statements/delete
> Source file: manual/sql/statements/delete.md

The `DELETE` statement removes rows matching an optional `WHERE` predicate from a table, enforcing foreign-key referential actions and updating all table indexes.

## Syntax

```sql
DELETE FROM table_name
[WHERE filter_condition];
```

## Parameters

| Parameter | Description |
| --- | --- |
| `table_name` | Target table from which rows will be removed. |
| `WHERE filter_condition` | Optional predicate selecting which rows to delete. When omitted, **all rows** in the table are deleted. |

## Filtered `DELETE`

Provide a `WHERE` clause to remove only rows matching your condition. `WHERE` supports comparisons, logical operators, `IN`, `BETWEEN`, `LIKE`/`NOT LIKE`, `IS [NOT] NULL`, subqueries, and vector distance predicates.

```sql
-- Delete a single row by primary key
DELETE FROM players
WHERE Id = 4;

-- Delete with compound predicates
DELETE FROM matches
WHERE PlayedOn < '2025-01-01'
  AND Points = 0;

-- Delete using a subquery
DELETE FROM players
WHERE NOT EXISTS (
    SELECT 1
    FROM matches m
    WHERE m.PlayerId = players.Id
);
```

## Full-Table `DELETE`

Omitting the `WHERE` clause removes every row in the table while preserving the table schema, constraints, and index definitions.

```sql
DELETE FROM matches;
```

::: warning Destructive Operation
DataVo v0.5 does not have a `TRUNCATE TABLE` statement; `DELETE FROM table_name;` removes every row and executes through normal constraint, index, and transaction paths. Wrap bulk deletes inside `BEGIN` ... `COMMIT` if you need an undo boundary.
:::

## Foreign Keys & Index Maintenance

- **Referential actions:** Deleting a parent row checks all referencing child tables. `ON DELETE CASCADE` removes dependent child rows automatically; `ON DELETE RESTRICT` rejects the `DELETE` if any child row still references the target key.
- **Automatic index cleanup:** Removed rows are purged from all B-Tree, `HNSW`, `FLAT`, and `COLUMNAR` indexes on the table.
- **Unsupported clauses:** `DELETE ... USING` joins and `RETURNING` clauses are not supported in DataVo v0.5.

## Transactions & Rollback Behavior

Inside an explicit transaction (`BEGIN` ... `COMMIT`), `DELETE` does not immediately destroy committed rows on disk. Instead, deletes are buffered in `TransactionContext`:

- **Read-your-own-writes:** Subsequent `SELECT`, `UPDATE`, and `DELETE` statements inside the same transaction immediately exclude the deleted rows.
- **Buffered inserts:** If a row was inserted earlier in the same uncommitted transaction, `DELETE` drops it from the transaction's insert buffer.
- **Full reversibility:** Executing `ROLLBACK` or `ROLLBACK TO SAVEPOINT` cleanly discards the buffered deletes so committed rows remain untouched.

```sql
BEGIN;

DELETE FROM players WHERE Region = 'Legacy';
SAVEPOINT after_cleanup;

DELETE FROM players WHERE Score < 200;

-- Undo only the second delete
ROLLBACK TO SAVEPOINT after_cleanup;

COMMIT;
```

## Reclaiming Space with `VACUUM`

Deleted rows remain as tombstones until storage compacts them. `Lsm` compacts them during background merges; to rewrite one table immediately (for example after a large purge), run `VACUUM` on it:

```sql
VACUUM players;
```

`VACUUM table_name` performs three steps:

1. Scans the table and strips all tombstoned (soft-deleted) rows.
2. Rewrites the surviving rows with fresh, contiguous internal row IDs.
3. Drops and rebuilds all B-Tree indexes on the table against the new row IDs.

::: info When You Need `VACUUM`
`Lsm` compacts deleted keys automatically during background and flush merges. `VACUUM <table>` is for reclaiming space at once after a large one-time purge.
:::

## See Also

- [INSERT](/manual/sql/statements/insert): Adding new rows to a table
- [UPDATE](/manual/sql/statements/update): Modifying existing rows
- [CREATE TABLE](/manual/sql/statements/create-table): Configuring `ON DELETE CASCADE` and `ON DELETE RESTRICT` foreign keys
- [Transactions](/manual/sql/statements/transactions): Using `BEGIN`, `SAVEPOINT`, `ROLLBACK TO SAVEPOINT`, and `COMMIT`
