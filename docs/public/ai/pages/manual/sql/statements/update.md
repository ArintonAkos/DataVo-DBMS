# UPDATE

> Source route: /manual/sql/statements/update
> Source file: manual/sql/statements/update.md

The `UPDATE` statement modifies one or more column values on existing rows matching an optional `WHERE` predicate, re-validating constraints and keeping all table indexes synchronized.

## Syntax

```sql
UPDATE table_name
SET column1 = expression1 [, column2 = expression2, ...]
[WHERE filter_condition];
```

## Parameters

| Parameter | Description |
| --- | --- |
| `table_name` | Target table whose rows will be updated. |
| `SET column = expression` | One or more comma-separated assignments. Supports literals, `@param` parameters, column references, binary arithmetic/string `+ - * /`, and scalar subqueries. |
| `WHERE filter_condition` | Optional predicate selecting which rows to modify. When omitted, **every row** in the table is updated. |

## Single & Multi-Column Updates

Update a single column or assign multiple columns in one `SET` clause:

```sql
-- Update a single column on one row
UPDATE players
SET Score = 1050
WHERE Id = 1;

-- Update multiple columns at once
UPDATE players
SET Score = 1200,
    Region = 'US'
WHERE Id = 2;
```

To update a `VECTOR(n)` column, provide a single-quoted float-array literal or a bound parameter matching the column's dimension:

```sql
UPDATE documents
SET Category = 'tutorial',
    Embedding = @newEmbedding
WHERE Id = 1;
```

## Expressions & Subqueries in `SET`

`SET` assignments evaluate column references, literals, bound parameters, binary operators (`+`, `-`, `*`, `/`), and scalar subqueries (`(SELECT ...)`).

```sql
-- Arithmetic update referencing the current row
UPDATE players
SET Score = Score + 50
WHERE Region = 'EU';

-- String concatenation using '+'
UPDATE players
SET Name = Name + ' (VIP)'
WHERE Score >= 1200;

-- Scalar subquery assignment
UPDATE players
SET Score = (SELECT MAX(Points) FROM matches WHERE PlayerId = 1)
WHERE Id = 1;
```

- **NULL propagation:** In binary arithmetic expressions (`Score + 10`), if either operand is `NULL`, the expression evaluates to `NULL`.
- **Unknown columns:** Referencing a column that does not exist in the target table fails immediately with an unknown-column binding error before any row is modified.

::: warning No Scalar Functions in `SET`
`UPDATE ... SET` does **not** evaluate scalar functions such as `LOWER(...)` or `UPPER(...)`. Compute string transformations in application code and bind the result as a literal or `@param` instead.

Additionally, `UPDATE ... FROM` joins and `RETURNING` clauses are not supported in DataVo v0.5.
:::

## Filtering & Full-Table Updates

`WHERE` accepts the same predicates as `SELECT`, including comparisons, `AND`/`OR`/`NOT`, `IN`, `BETWEEN`, `LIKE`, `IS [NOT] NULL`, and subqueries.

```sql
UPDATE matches
SET Points = Points + 10
WHERE PlayedOn BETWEEN '2026-01-01' AND '2026-03-31'
  AND Points < 100;
```

::: warning Omitting `WHERE` Updates Every Row
If you omit the `WHERE` clause, DataVo applies the `SET` assignments to **all rows** in the table:

```sql
-- Resets Score to 0 for every player in the table
UPDATE players
SET Score = 0;
```
:::

## Index Maintenance & Transactions

- **Automatic index updates:** Every `UPDATE` automatically maintains all B-Tree, `HNSW`, `FLAT`, and `COLUMNAR` indexes on the table. Updating an indexed or `VECTOR(n)` column removes stale entries and indexes the new values atomically.
- **Constraint checks:** Updating a `PRIMARY KEY`, `UNIQUE`, `NOT NULL`, or `FOREIGN KEY` column re-validates all constraints before the change takes effect.
- **MVCC & transaction buffering:** Inside an explicit `BEGIN` ... `COMMIT` transaction, `UPDATE` reads through the transaction's snapshot and its own prior uncommitted writes, coalesces buffered updates in `TransactionContext`, and is completely reversible via `ROLLBACK` or `ROLLBACK TO SAVEPOINT`.

## See Also

- [INSERT](/manual/sql/statements/insert): Adding new rows to a table
- [DELETE](/manual/sql/statements/delete): Removing rows and compacting storage with `VACUUM`
- [SELECT](/manual/sql/statements/select): Querying updated rows and `WHERE` predicate syntax
- [Transactions](/manual/sql/statements/transactions): Atomic multi-statement updates with `BEGIN`, `SAVEPOINT`, `COMMIT`, and `ROLLBACK`
