# Performance Streaming First Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce unnecessary materialization for simple Volcano `LIMIT` queries and expose a benchmarkable path for future streaming work.

**Architecture:** Add a bounded execution helper that can stop after N rows. Use it only where semantics allow early termination: simple no-join Volcano scans with pushed-down `LIMIT` and no blocking operators ahead of `TakeOperator`.

**Tech Stack:** C#, Volcano operators, xUnit.

---

## File Structure

- Modify: `DataVo.Core/Execution/Volcano/OperatorPipelineRunner.cs`
  - Add `ExecuteToList(IQueryOperator root, int? maxRows)`.
- Modify: `DataVo.Core/Parser/DQL/Select.cs`
  - Use bounded execution when Volcano limit pushdown is known safe.
- Modify: `DataVo.Tests/Execution/VolcanoOperatorTests.cs`
  - Add runner-level bounded execution tests.
- Modify: `docs/architecture/performance-and-storage-design.md`
  - Document the first bounded execution slice.

## Task 1: Add Bounded Runner Test

**Files:**
- Modify: `DataVo.Tests/Execution/VolcanoOperatorTests.cs`

- [ ] **Step 1: Write failing test**

Add:

```csharp
[Fact]
public void OperatorPipelineRunner_StopsAfterMaxRows()
{
    var rows = Enumerable.Range(1, 100)
        .Select(i => new ExecutionRow(i, new Dictionary<string, object?> { ["Id"] = i }))
        .ToList();

    var result = OperatorPipelineRunner.ExecuteToList(new TableScanOperator(rows), maxRows: 5);

    Assert.Equal(5, result.Count);
    Assert.Equal(5, result[^1].RowId);
}
```

- [ ] **Step 2: Verify RED**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter OperatorPipelineRunner_StopsAfterMaxRows`

Expected: FAIL because overload does not exist.

## Task 2: Implement Bounded Runner

**Files:**
- Modify: `DataVo.Core/Execution/Volcano/OperatorPipelineRunner.cs`

- [ ] **Step 1: Add overload**

Implement:

```csharp
public static List<ExecutionRow> ExecuteToList(IQueryOperator root, int? maxRows)
{
    if (maxRows.HasValue && maxRows.Value < 0)
    {
        throw new ArgumentOutOfRangeException(nameof(maxRows));
    }

    var result = new List<ExecutionRow>(maxRows.GetValueOrDefault() > 0 ? maxRows.Value : 0);
    root.Open();

    try
    {
        while (!maxRows.HasValue || result.Count < maxRows.Value)
        {
            var row = root.GetNextRow();
            if (row == null)
            {
                break;
            }

            result.Add(row);
        }
    }
    finally
    {
        root.Close();
    }

    return result;
}
```

Update existing overload:

```csharp
public static List<ExecutionRow> ExecuteToList(IQueryOperator root)
{
    return ExecuteToList(root, maxRows: null);
}
```

- [ ] **Step 2: Verify GREEN**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter OperatorPipelineRunner_StopsAfterMaxRows`

Expected: PASS.

## Task 3: Use Bounded Execution in Safe SELECT Path

**Files:**
- Modify: `DataVo.Core/Parser/DQL/Select.cs`
- Modify: `DataVo.Tests/E2E/DQL/VolcanoSelectExecutionTests.cs`

- [ ] **Step 1: Add behavior regression test**

Add a test that enables Volcano and verifies a simple `LIMIT` query still returns exactly the first rows:

```csharp
[Fact]
public void VolcanoNoJoinLimit_ReturnsExpectedRowsWithBoundedRunner()
{
    using var context = CreateContext(config =>
    {
        config.EnableVolcanoExecution = true;
    });

    context.Execute("CREATE DATABASE LimitDb");
    context.Execute("USE LimitDb");
    context.Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR(50))");
    for (int i = 1; i <= 20; i++)
    {
        context.Execute($"INSERT INTO Items VALUES ({i}, 'Item {i}')");
    }

    var result = context.Execute("SELECT * FROM Items LIMIT 3").Single();

    Assert.False(result.IsError, string.Join(Environment.NewLine, result.Messages));
    Assert.Equal(3, result.Data.Count);
}
```

Adapt helper names to existing `SqlExecutionTestsBase` APIs.

- [ ] **Step 2: Run test before code change**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter VolcanoNoJoinLimit_ReturnsExpectedRowsWithBoundedRunner`

Expected: PASS or FAIL depending on existing behavior; this is a regression guard.

- [ ] **Step 3: Apply bounded execution where safe**

In `EvaluateNoJoinWithVolcano`, compute:

```csharp
int? boundedMaxRows = _volcanoLimitPushedDown
    && !_volcanoOrderPushedDown
    && !_volcanoDistinctPushedDown
    && !_volcanoGroupByPushedDown
    && !_volcanoAggregatePushedDown
        ? _model.LimitTake
        : null;
```

Use:

```csharp
List<ExecutionRow> filteredRows = OperatorPipelineRunner.ExecuteToList(root, boundedMaxRows);
```

- [ ] **Step 4: Verify focused tests**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter "OperatorPipelineRunner_StopsAfterMaxRows|VolcanoNoJoinLimit_ReturnsExpectedRowsWithBoundedRunner"`

Expected: PASS.

## Task 4: Document the Performance Slice

**Files:**
- Modify: `docs/architecture/performance-and-storage-design.md`

- [ ] **Step 1: Add bounded execution note**

Add:

```markdown
### Bounded Volcano execution

Simple no-join Volcano plans with pushed-down `LIMIT` can stop pulling rows once the requested row count is reached. Blocking operators such as sort, distinct, joins, and aggregate still require full input materialization in the current engine.
```

- [ ] **Step 2: Full verification**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore`

Expected: 0 failed.

