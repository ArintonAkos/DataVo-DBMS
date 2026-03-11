# Transactions

`DataVo` supports explicit transaction commands for multi-statement workflows.

## Supported commands

| Command             | Meaning                                     |
| :------------------ | :------------------------------------------ |
| `BEGIN TRANSACTION` | Start a transaction for the current session |
| `COMMIT`            | Persist the transaction's changes           |
| `ROLLBACK`          | Discard the transaction's pending changes   |

## Commit example

### Initial table

|  Id | Name  |
| --: | :---- |
|   1 | Alice |

### Statements

```sql
BEGIN TRANSACTION;

INSERT INTO Users VALUES (2, 'Bob');
UPDATE Users SET Name = 'Alicia' WHERE Id = 1;

COMMIT;
```

### Final table after commit

|  Id | Name   |
| --: | :----- |
|   1 | Alicia |
|   2 | Bob    |

## Rollback example

### Initial table

|  Id | Name  |
| --: | :---- |
|   1 | Alice |

### Statements

```sql
BEGIN TRANSACTION;
DELETE FROM Users WHERE Id = 1;
ROLLBACK;
```

### Final table after rollback

|  Id | Name  |
| --: | :---- |
|   1 | Alice |

## How it works conceptually

- transaction state is tracked per logical session
- execution paths coordinate with the transaction manager
- table-scoped locks reduce conflicting write behavior
- in disk mode, WAL-backed durability and recovery can participate in commit flows

## Current behavior summary

| Area          | Current behavior                                     |
| :------------ | :--------------------------------------------------- |
| Session scope | transaction state is session-bound                   |
| Locking       | table-scoped locking                                 |
| Durability    | WAL-enabled disk mode participates in recovery story |
