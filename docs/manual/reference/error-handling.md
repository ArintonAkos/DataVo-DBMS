# Error Handling

The general SQL API reports failures through `QueryResult`. A result can contain rows, messages, and an `IsError` flag. Application code should check the flag before assuming data is present.

```csharp
using DataVo.Core.Contracts.Results;

List<QueryResult> results = db.Execute("SELECT * FROM MissingTable");

foreach (QueryResult result in results)
{
    if (result.IsError)
    {
        Console.Error.WriteLine(string.Join(Environment.NewLine, result.Messages));
        continue;
    }

    Console.WriteLine($"Rows returned: {result.Data.Count}");
}
```

For successful statements, `Messages` often contains human-readable execution notes such as row counts or table/database messages.

```csharp
QueryResult result = db.Execute("""
INSERT INTO Users (Id, Name)
VALUES (1, 'Ada');
""")[0];

foreach (string message in result.Messages)
{
    Console.WriteLine(message);
}
```

For SELECT queries, use `Fields` to inspect the returned column order and `Data` to read row dictionaries.

```csharp
QueryResult result = db.Execute("""
SELECT Id, Name
FROM Users
ORDER BY Id ASC;
""")[0];

Console.WriteLine(string.Join(", ", result.Fields));

foreach (Dictionary<string, object?> row in result.Data)
{
    Console.WriteLine($"{row["Id"]}: {row["Name"]}");
}
```

When using the Entity Framework bridge, unsupported query patterns can surface as typed EF integration exceptions. Keep the query expression and exception message together in logs so unsupported operators are easy to diagnose.

```csharp
try
{
    List<User> users = ctx.QueryFromDataVo<User>(query => query
        .Where(user => user.IsActive)
        .OrderBy(user => user.Id)
        .Take(10));
}
catch (Exception ex)
{
    logger.LogError(ex, "DataVo EF query pattern was not supported");
    throw;
}
```

For v0.1, do not build application logic around stable numeric error codes. Prefer checking `IsError`, logging `Messages`, and keeping the SQL statement and storage configuration with the diagnostic record.

```csharp
try
{
    List<QueryResult> results = db.Execute(sql);

    foreach (QueryResult result in results.Where(static r => r.IsError))
    {
        logger.LogError("DataVo query failed: {Messages}", string.Join("; ", result.Messages));
    }
}
catch (Exception ex)
{
    logger.LogError(ex, "DataVo integration call threw an exception");
    throw;
}
```

## Error Handling Summary

| Feature | Status | Notes |
| --- | --- | --- |
| `QueryResult.IsError` | Supported | Primary signal for general SQL execution failure. |
| `QueryResult.Messages` | Supported | Contains row counts, validation notes, and error messages. |
| `QueryResult.Fields` and `Data` | Supported | Used for successful tabular results. |
| Parser/lexer/binding diagnostics | Supported | May surface through result messages or exceptions depending on call path. |
| EF query validation exceptions | Supported | Unsupported EF bridge query patterns can be reported through typed integration errors. |
| Stable public error code catalog | Planned | v0.1 does not yet document stable numeric error codes. |
