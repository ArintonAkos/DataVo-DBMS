# Types (Parser) Overview

The `Types` module contains small in-memory data structures used during statement evaluation (joins, grouping, projection).

## Core types

- `Row`: a dictionary of column/value pairs.
- `JoinedRow`: a dictionary of `tableName/alias -> Row` segments.
- `JoinedRowId`: a composite identifier (ordered list of RowIds) used as a join key.
- `ListedTable`: a list of `JoinedRow`.
- `HashedTable`: a dictionary of `JoinedRowId -> JoinedRow`.
- `GroupedTable`: a dictionary of `groupKey -> ListedTable`, with `ApplyAggregations(...)`.

## Notes

These types are evaluation-time only; they are not persisted by the storage engines.
