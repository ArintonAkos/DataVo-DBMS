# INSERT, UPDATE, DELETE, and VACUUM

This page explains how data-changing statements behave in `DataVo`.

## Example starting table

|  Id | Name     | Status |
| --: | :------- | :----- |
|   1 | Keyboard | Active |
|   2 | Mouse    | Active |

## `INSERT`

### Full-row insert

```sql
INSERT INTO Items VALUES (3, 'Monitor', 'Active');
```

### What it does

- validates the row shape
- validates types against catalog metadata
- checks keys and constraints
- writes the row to storage
- updates indexes

### Result

|  Id | Name     | Status |
| --: | :------- | :----- |
|   1 | Keyboard | Active |
|   2 | Mouse    | Active |
|   3 | Monitor  | Active |

### Column-targeted insert

```sql
INSERT INTO Items (Id, Name) VALUES (4, 'Dock');
```

If omitted columns have defaults, those defaults are applied.

## `UPDATE`

### Query

```sql
UPDATE Items
SET Status = 'Archived'
WHERE Id = 1;
```

### Result

|  Id | Name     | Status   |
| --: | :------- | :------- |
|   1 | Keyboard | Archived |
|   2 | Mouse    | Active   |

### Supported update patterns

| Pattern                    |                 Supported                  |
| :------------------------- | :----------------------------------------: |
| constant assignments       |                    Yes                     |
| filtered updates           |                    Yes                     |
| scalar subqueries in `SET` | Supported in the current implemented slice |

## `DELETE`

### Query

```sql
DELETE FROM Items
WHERE Id = 2;
```

### Result

|  Id | Name     | Status   |
| --: | :------- | :------- |
|   1 | Keyboard | Archived |

## `VACUUM`

`VACUUM` compacts a table after deletes.

```sql
VACUUM Items;
```

### What it does

- rewrites live rows densely
- removes deleted-row gaps
- gives the table a compact physical layout again

## Constraint behavior

Current modification flows enforce:

- primary key constraints
- unique constraints
- foreign key constraints

## End-to-end example

```sql
CREATE TABLE Items (
  Id INT PRIMARY KEY,
  Name VARCHAR(50),
  Status VARCHAR(20) DEFAULT 'Active'
);

INSERT INTO Items (Id, Name) VALUES (1, 'Keyboard');
INSERT INTO Items (Id, Name) VALUES (2, 'Mouse');
UPDATE Items SET Status = 'Archived' WHERE Id = 1;
DELETE FROM Items WHERE Id = 2;
VACUUM Items;
```
