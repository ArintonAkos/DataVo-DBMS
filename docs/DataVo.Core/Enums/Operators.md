# Operators

`Operators` centralizes supported condition, comparison, arithmetic, and function operators.

## Operator groups

- condition operators: `AND`, `OR`
- logical operators: `=`, `!=`, `>`, `<`, `>=`, `<=`, `LIKE`, `IS NULL`, `IS NOT NULL`
- arithmetic operators: `+`, `-`, `*`, `/`
- scalar functions: `LEN`, `UPPER`, `LOWER`

## Important implementation detail

`Supported()` returns operators ordered by descending length. This prevents a shorter operator from matching before a longer one at the same source position.

## Example

```csharp
bool found = Operators.ContainsOperator("Age >= 18", 4, out int length);
```
