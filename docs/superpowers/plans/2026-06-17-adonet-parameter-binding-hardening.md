# ADO.NET Parameter Binding Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace fragile global string parameter replacement with token-aware substitution that does not modify string literals or longer parameter names.

**Architecture:** Keep the public `DataVoCommand` API unchanged. Introduce a private scanner in `DataVoCommand` that substitutes parameter markers only outside quoted SQL strings and only when the marker is not part of a longer identifier.

**Tech Stack:** C#, xUnit, existing `DataVo.Data` command tests.

---

## File Structure

- Modify: `DataVo.Data/DataVoCommand.cs`
  - Replace `sql.Replace(param.ParameterName, literal)` with a token-aware substitution method.
  - Format vector values if `float[]`, `double[]`, or numeric arrays are passed.
- Modify/Create: `DataVo.Tests/ADO/AdoNetTests.cs`
  - Add tests for parameter-prefix collisions, quoted literals, escaped quotes, and vector arrays.

## Task 1: Add Collision Regression Test

**Files:**
- Modify: `DataVo.Tests/ADO/AdoNetTests.cs`

- [ ] **Step 1: Write failing test**

Add:

```csharp
[Fact]
public void CommandParameters_DoNotReplacePrefixInsideLongerParameterName()
{
    using var connection = new DataVoConnection($"StorageMode=InMemory;DataSource=ado_params_{Guid.NewGuid():N}");
    connection.Open();

    using var setup = connection.CreateCommand();
    setup.CommandText = "CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR(50));";
    setup.ExecuteNonQuery();

    using var insert = connection.CreateCommand();
    insert.CommandText = "INSERT INTO Items VALUES (@id_long, @id);";
    insert.Parameters.AddWithValue("@id", "short");
    insert.Parameters.AddWithValue("@id_long", 1);
    insert.ExecuteNonQuery();

    using var query = connection.CreateCommand();
    query.CommandText = "SELECT Name FROM Items WHERE Id = @id_long;";
    query.Parameters.AddWithValue("@id_long", 1);

    Assert.Equal("short", query.ExecuteScalar());
}
```

- [ ] **Step 2: Verify RED**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter CommandParameters_DoNotReplacePrefixInsideLongerParameterName`

Expected: FAIL with malformed SQL or wrong substituted value.

## Task 2: Add Quoted Literal Regression Test

**Files:**
- Modify: `DataVo.Tests/ADO/AdoNetTests.cs`

- [ ] **Step 1: Write failing test**

Add:

```csharp
[Fact]
public void CommandParameters_DoNotReplaceMarkersInsideQuotedStrings()
{
    using var connection = new DataVoConnection($"StorageMode=InMemory;DataSource=ado_quotes_{Guid.NewGuid():N}");
    connection.Open();

    using var setup = connection.CreateCommand();
    setup.CommandText = "CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR(50)); INSERT INTO Items VALUES (1, '@id');";
    setup.ExecuteNonQuery();

    using var query = connection.CreateCommand();
    query.CommandText = "SELECT Name FROM Items WHERE Name = '@id';";
    query.Parameters.AddWithValue("@id", 1);

    Assert.Equal("@id", query.ExecuteScalar());
}
```

- [ ] **Step 2: Verify RED**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter CommandParameters_DoNotReplaceMarkersInsideQuotedStrings`

Expected: FAIL because current replacement changes `'@id'`.

## Task 3: Implement Token-Aware Substitution

**Files:**
- Modify: `DataVo.Data/DataVoCommand.cs`

- [ ] **Step 1: Add scanner-based substitution**

Replace `SubstituteParameters` with scanner logic:

```csharp
private string SubstituteParameters(string sql)
{
    if (_parameters.AllParameters.Count == 0)
    {
        return sql;
    }

    var literals = _parameters.AllParameters
        .Where(p => !string.IsNullOrEmpty(p.ParameterName))
        .OrderByDescending(p => p.ParameterName!.Length)
        .ToDictionary(p => p.ParameterName!, p => FormatLiteral(p.Value), StringComparer.Ordinal);

    var builder = new StringBuilder(sql.Length);
    bool inString = false;

    for (int i = 0; i < sql.Length; i++)
    {
        char current = sql[i];
        if (current == '\'')
        {
            builder.Append(current);
            if (inString && i + 1 < sql.Length && sql[i + 1] == '\'')
            {
                builder.Append(sql[++i]);
                continue;
            }

            inString = !inString;
            continue;
        }

        if (!inString && current == '@')
        {
            string? matchedName = TryReadParameterName(sql, i, literals);
            if (matchedName != null)
            {
                builder.Append(literals[matchedName]);
                i += matchedName.Length - 1;
                continue;
            }
        }

        builder.Append(current);
    }

    return builder.ToString();
}
```

Add helper:

```csharp
private static string? TryReadParameterName(string sql, int start, IReadOnlyDictionary<string, string> literals)
{
    foreach (string name in literals.Keys)
    {
        if (start + name.Length > sql.Length)
        {
            continue;
        }

        if (!sql.AsSpan(start, name.Length).SequenceEqual(name.AsSpan()))
        {
            continue;
        }

        int next = start + name.Length;
        if (next < sql.Length && (char.IsLetterOrDigit(sql[next]) || sql[next] == '_'))
        {
            continue;
        }

        return name;
    }

    return null;
}
```

Add `using System.Text;`.

- [ ] **Step 2: Run focused tests**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter "CommandParameters_DoNotReplace"`

Expected: PASS.

## Task 4: Add Vector Array Literal Formatting

**Files:**
- Modify: `DataVo.Data/DataVoCommand.cs`
- Modify: `DataVo.Tests/ADO/AdoNetTests.cs`

- [ ] **Step 1: Write failing vector parameter test**

Add:

```csharp
[Fact]
public void CommandParameters_FormatFloatArrayAsVectorLiteral()
{
    using var connection = new DataVoConnection($"StorageMode=InMemory;DataSource=ado_vector_{Guid.NewGuid():N}");
    connection.Open();

    using var setup = connection.CreateCommand();
    setup.CommandText = "CREATE TABLE Items (Id INT PRIMARY KEY, Vector VECTOR(3));";
    setup.ExecuteNonQuery();

    using var insert = connection.CreateCommand();
    insert.CommandText = "INSERT INTO Items VALUES (@id, @vec);";
    insert.Parameters.AddWithValue("@id", 1);
    insert.Parameters.AddWithValue("@vec", new[] { 0.1f, 0.2f, 0.3f });
    insert.ExecuteNonQuery();

    using var query = connection.CreateCommand();
    query.CommandText = "SELECT Id FROM Items WHERE Id = @id;";
    query.Parameters.AddWithValue("@id", 1);

    Assert.Equal(1, Convert.ToInt32(query.ExecuteScalar()));
}
```

- [ ] **Step 2: Verify RED**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter CommandParameters_FormatFloatArrayAsVectorLiteral`

Expected: FAIL because arrays currently format as type names.

- [ ] **Step 3: Implement vector array formatting**

Update `FormatLiteral`:

```csharp
float[] floats => $"'{FormatVector(floats.Select(v => (double)v))}'",
double[] doubles => $"'{FormatVector(doubles)}'",
decimal[] decimals => $"'{FormatVector(decimals.Select(v => (double)v))}'",
```

Add helper:

```csharp
private static string FormatVector(IEnumerable<double> values)
{
    return "[" + string.Join(", ", values.Select(v => v.ToString("R", CultureInfo.InvariantCulture))) + "]";
}
```

- [ ] **Step 4: Verify full ADO tests**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter AdoNetTests`

Expected: PASS.

## Task 5: Full Verification

- [ ] **Step 1: Run full suite**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore`

Expected: 0 failed.

