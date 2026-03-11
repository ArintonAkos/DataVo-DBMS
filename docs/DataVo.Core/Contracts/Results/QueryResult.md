# QueryResult

`QueryResult` is the standardized response envelope for all executed commands.

## Fields

- `Messages`: execution messages and diagnostics
- `Data`: row payloads for result-producing commands
- `Fields`: selected column names in output order
- `ExecutionTime`: elapsed time metadata
- `IsError`: coarse success/failure flag

## Factory helpers

- `QueryResult.Error(...)`
- `QueryResult.Success(...)`
- `QueryResult.Default()`

## Example

```csharp
QueryResult result = QueryResult.Success(
    ["Rows selected: 2"],
    [
        new Dictionary<string, dynamic> { ["Id"] = 1 },
        new Dictionary<string, dynamic> { ["Id"] = 2 }
    ],
    ["Id"]);
```
