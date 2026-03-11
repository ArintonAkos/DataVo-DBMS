# IColumn

`IColumn` exposes the raw catalog/storage type of a column-like value.

## Purpose

This interface gives shared services a minimal type contract without forcing them to depend on a specific catalog implementation.

## Current consumers

- `Services/TableColumnService.cs`
- catalog model types such as `Models/Catalog/Column.cs`

## Example

```csharp
bool isNumeric = TableColumnService.IsNumeric(column);
string rawType = column.RawType();
```
