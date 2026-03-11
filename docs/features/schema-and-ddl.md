# DDL and Schema Changes

This page explains what schema-definition and schema-evolution features are currently supported.

## `CREATE DATABASE`

```sql
CREATE DATABASE Demo;
```

Creates a new database entry in the catalog.

## `USE`

```sql
USE Demo;
```

Binds the current session to `Demo`.

## `CREATE TABLE`

```sql
CREATE TABLE Users (
  Id INT PRIMARY KEY,
  Name VARCHAR(50),
  DepartmentId INT REFERENCES Departments(Id)
);
```

This writes schema metadata into the catalog and prepares the table for future inserts.

## `DROP TABLE`

```sql
DROP TABLE Users;
```

Drops both logical metadata and physical storage for the table.

## `CREATE INDEX`

```sql
CREATE INDEX IX_Users_Name ON Users(Name);
```

This registers the index and builds it from existing rows.

## `ALTER TABLE ADD COLUMN`

### Initial table

|  Id | Name  |
| --: | :---- |
|   1 | Alice |
|   2 | Bob   |

### Query

```sql
ALTER TABLE Users ADD COLUMN Status VARCHAR(20) DEFAULT 'Active';
```

### Resulting table

|  Id | Name  | Status |
| --: | :---- | :----- |
|   1 | Alice | Active |
|   2 | Bob   | Active |

### How it works

- updates catalog metadata
- rewrites existing rows
- backfills `NULL` or the provided default
- rebuilds indexes after rewrite

## `ALTER TABLE DROP COLUMN`

### Initial table

|  Id | Name  | Status |
| --: | :---- | :----- |
|   1 | Alice | Active |
|   2 | Bob   | Active |

### Query

```sql
ALTER TABLE Users DROP COLUMN Status;
```

### Resulting table

|  Id | Name  |
| --: | :---- |
|   1 | Alice |
|   2 | Bob   |

### Rejected in current version

- primary key columns
- unique columns
- foreign key columns
- referenced parent columns
- indexed columns
- the last remaining column

## `ALTER TABLE MODIFY COLUMN`

### Initial table

|  Id | Name  | Score |
| --: | :---- | ----: |
|   1 | Alice |    95 |
|   2 | Bob   |    82 |

### Query

```sql
ALTER TABLE Users MODIFY COLUMN Name VARCHAR(100);
```

### What it does

- updates the catalog column definition
- rewrites rows
- converts existing values to the new type or length where possible

### Example default change

```sql
ALTER TABLE Users MODIFY COLUMN Score FLOAT DEFAULT 0;
```

### Rejected in current version

- primary key columns
- unique columns
- foreign key columns
- referenced parent columns
- indexed columns
- incompatible existing data

## Summary table

| Operation                   | Implemented | Notes               |
| :-------------------------- | :---------: | :------------------ |
| `CREATE DATABASE`           |     Yes     | supported           |
| `USE`                       |     Yes     | supported           |
| `CREATE TABLE`              |     Yes     | supported           |
| `CREATE INDEX`              |     Yes     | supported           |
| `ALTER TABLE ADD COLUMN`    |     Yes     | guarded first slice |
| `ALTER TABLE DROP COLUMN`   |     Yes     | guarded first slice |
| `ALTER TABLE MODIFY COLUMN` |     Yes     | guarded first slice |
