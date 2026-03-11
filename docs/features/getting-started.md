# Getting Started

This page walks through the first successful end-to-end experience with `DataVo`.

## The shortest useful flow

```sql
CREATE DATABASE Demo;
USE Demo;

CREATE TABLE Users (
  Id INT PRIMARY KEY,
  Name VARCHAR(50),
  Score FLOAT
);

INSERT INTO Users VALUES (1, 'Alice', 95.5);
INSERT INTO Users VALUES (2, 'Bob', 88.0);

SELECT Id, Name, Score
FROM Users
ORDER BY Id;
```

## What each statement does

| Statement                | Meaning                                    |
| :----------------------- | :----------------------------------------- |
| `CREATE DATABASE Demo`   | Creates a database catalog entry           |
| `USE Demo`               | Binds the current session to that database |
| `CREATE TABLE Users ...` | Defines schema and keys                    |
| `INSERT INTO Users ...`  | Validates and writes rows                  |
| `SELECT ...`             | Reads and returns rows                     |

## Logical table before the `SELECT`

|  Id | Name  | Score |
| --: | :---- | ----: |
|   1 | Alice |  95.5 |
|   2 | Bob   |  88.0 |

## Query example

```sql
SELECT Id, Name, Score
FROM Users
ORDER BY Id;
```

## What this query means

- project `Id`, `Name`, and `Score`
- read from `Users`
- sort rows by `Id`

## Expected result table

|  Id | Name  | Score |
| --: | :---- | ----: |
|   1 | Alice |  95.5 |
|   2 | Bob   |  88.0 |

## Result shape in code

When executed through the embedding API, query results are returned as `QueryResult` instances.

| Member     | Meaning                                                   |
| :--------- | :-------------------------------------------------------- |
| `Messages` | Execution messages such as row counts or validation notes |
| `Fields`   | The field names returned in order                         |
| `Data`     | The row payload itself                                    |
| `IsError`  | Whether execution failed                                  |

## Embedded usage example

```csharp
using DataVo.Core;
using DataVo.Core.StorageEngine.Config;

using var context = new DataVoContext(new DataVoConfig
{
    StorageMode = StorageMode.InMemory
});

context.Execute("CREATE DATABASE Demo");
context.Execute("USE Demo");
context.Execute("CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR(50), Score FLOAT)");
context.Execute("INSERT INTO Users VALUES (1, 'Alice', 95.5)");
context.Execute("INSERT INTO Users VALUES (2, 'Bob', 88.0)");

var results = context.Execute(@"
    SELECT Id, Name, Score
    FROM Users
    ORDER BY Id;
");
```

## Execution flow at a glance

1. SQL text is tokenized by the `Lexer`
2. the `Parser` builds AST nodes
3. the `Evaluator` creates executable actions
4. runtime services use storage and indexes to retrieve data
5. rows are returned in `QueryResult.Data`

## Storage modes

| Mode       | Best for                                              |
| :--------- | :---------------------------------------------------- |
| `InMemory` | tests, examples, and benchmarks                       |
| `Disk`     | persistent local data and recovery-oriented scenarios |

## Where to go next

- [Setup and Packaging](./setup-and-packaging.md)
- [SELECT and Querying](./select-and-querying.md)
- [Schema and DDL](./schema-and-ddl.md)
