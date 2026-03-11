# SELECT and Query Features

This page explains the current query surface of `DataVo` in a usage-first way.

## Example dataset

### `Users`

|  Id | Name  | DepartmentId | Score |
| --: | :---- | -----------: | ----: |
|   1 | Alice |           10 |    95 |
|   2 | Bob   |           20 |    82 |
|   3 | Cara  |           10 |    90 |

### `Departments`

|  Id | Name        |
| --: | :---------- |
|  10 | Engineering |
|  20 | Finance     |
|  30 | Legal       |

## Basic projection

### Query

```sql
SELECT Id, Name
FROM Users;
```

### Result

|  Id | Name  |
| --: | :---- |
|   1 | Alice |
|   2 | Bob   |
|   3 | Cara  |

## Filtering with `WHERE`

### Query

```sql
SELECT *
FROM Users
WHERE Id = 1;
```

### Result

|  Id | Name  | DepartmentId | Score |
| --: | :---- | -----------: | ----: |
|   1 | Alice |           10 |    95 |

## Ordering and limiting

### Query

```sql
SELECT Id, Name, Score
FROM Users
ORDER BY Score DESC, Id ASC
LIMIT 2 OFFSET 0;
```

### Result

|  Id | Name  | Score |
| --: | :---- | ----: |
|   1 | Alice |    95 |
|   3 | Cara  |    90 |

## Grouping and aggregates

### Query

```sql
SELECT DepartmentId, COUNT(*) AS EmployeeCount
FROM Users
GROUP BY DepartmentId
HAVING COUNT(*) > 1;
```

### Result

| DepartmentId | EmployeeCount |
| -----------: | ------------: |
|           10 |             2 |

## Join behavior

Supported join families:

- `INNER JOIN`
- `LEFT JOIN`
- `RIGHT JOIN`
- `FULL JOIN`
- `CROSS JOIN`

### Join summary

| Join type | Keeps unmatched left rows | Keeps unmatched right rows |
| :-------- | :-----------------------: | :------------------------: |
| `INNER`   |            No             |             No             |
| `LEFT`    |            Yes            |             No             |
| `RIGHT`   |            No             |            Yes             |
| `FULL`    |            Yes            |            Yes             |
| `CROSS`   |     Cartesian product     |     Cartesian product      |

### `INNER JOIN` example

```sql
SELECT u.Name, d.Name AS Department
FROM Users u
INNER JOIN Departments d ON u.DepartmentId = d.Id;
```

Result:

| Name  | Department  |
| :---- | :---------- |
| Alice | Engineering |
| Bob   | Finance     |
| Cara  | Engineering |

### `LEFT JOIN` behavior

If a user row has no matching department, the user row is kept and the department columns become `NULL`.

## Predicate features

### `IN`

```sql
SELECT Name
FROM Users
WHERE Id IN (1, 3);
```

Result:

| Name  |
| :---- |
| Alice |
| Cara  |

### `BETWEEN`

```sql
SELECT Name, Score
FROM Users
WHERE Score BETWEEN 85 AND 100;
```

Result:

| Name  | Score |
| :---- | ----: |
| Alice |    95 |
| Cara  |    90 |

### `LIKE`

```sql
SELECT Name
FROM Users
WHERE Name LIKE 'A%';
```

Result:

| Name  |
| :---- |
| Alice |

## Set operations

### `UNION`

```sql
SELECT Name FROM ActiveUsers
UNION
SELECT Name FROM PendingUsers;
```

Use `UNION` when duplicates should collapse.

### `UNION ALL`

```sql
SELECT Name FROM ActiveUsers
UNION ALL
SELECT Name FROM PendingUsers;
```

Use `UNION ALL` when duplicates should be preserved.

## Subqueries

### `IN (SELECT ...)`

Supported for single-column uncorrelated subqueries:

```sql
SELECT *
FROM Users
WHERE DepartmentId IN (
  SELECT Id FROM Departments WHERE Name LIKE 'E%'
);
```

### `EXISTS`

```sql
SELECT *
FROM Users u
WHERE EXISTS (
  SELECT Id FROM Departments d
  WHERE d.Id = u.DepartmentId
);
```

### Scalar subqueries

```sql
SELECT *
FROM Users
WHERE Score > (
  SELECT AVG(Score) FROM Users
);
```

## Execution flow for a typical `SELECT`

1. SQL text is tokenized
2. AST nodes are built
3. a `SELECT` action is prepared
4. rows are retrieved from storage and indexes
5. joins, filters, grouping, ordering, and limits are applied
6. the final row set is returned as `QueryResult.Data`

## Important current rules

| Area                  | Current behavior                            |
| :-------------------- | :------------------------------------------ |
| result rows           | dictionaries keyed by projected field name  |
| `UNION`               | checks branch shape and type compatibility  |
| scalar subqueries     | must return exactly one row                 |
| correlated subqueries | rejected explicitly in unsupported surfaces |
