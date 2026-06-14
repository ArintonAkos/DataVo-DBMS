# DML (Models) Overview

The DML models are small data carriers built from parser AST nodes. They normalize `INSERT`, `UPDATE`, and `DELETE` statements into shapes that are easy for the execution actions to consume.

## Component breakdown

| Component (File) | Role |
|------------------|------|
| `DeleteFromModel.cs` | Target table name + normalized `Where` statement (defaults to `true`). |
| `InsertIntoModel.cs` | Target table, optional column list, and one or more `VALUES` rows (`RawRows`). |
| `UpdateModel.cs` | Target table, `SET` expressions by column name, and `WHERE` expression (defaults to `true`). |

## Dependencies & interactions

- Built by `Parser.cs` / `Parser.DML` parsing routines.
- Consumed by the execution actions in `DataVo.Core/Parser/DML` (`DeleteFrom`, `InsertInto`, `Update`).
- Models do not touch storage; evaluation and constraint checks happen in the action layer.
