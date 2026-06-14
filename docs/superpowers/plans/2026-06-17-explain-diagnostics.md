# EXPLAIN Diagnostics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a first useful `EXPLAIN` command that returns planner diagnostics for `SELECT` statements without executing row reads.

**Architecture:** Introduce a parser command for `EXPLAIN <select>`. The first slice reports logical/physical plan kind, estimated cost, and planner reason using the same planner decision code as SELECT. It intentionally avoids `EXPLAIN ANALYZE` until execution metrics are designed.

**Tech Stack:** C#, parser AST/actions, xUnit.

---

## File Structure

- Modify: `DataVo.Core/Parser/Parser.cs`
  - Parse `EXPLAIN SELECT ...`.
- Create: `DataVo.Core/Parser/DQL/Explain.cs`
  - Execute planner-only diagnostics.
- Modify: `DataVo.Core/Parser/DQL/Select.Planner.cs`
  - Expose an internal diagnostics method usable by `Explain`.
- Modify: `DataVo.Tests/E2E/DQL/ExplainTests.cs`
  - Add command-level tests.
- Modify: `docs/features/select-and-querying.md`
  - Document initial `EXPLAIN` scope.

## Task 1: Add Failing EXPLAIN SELECT Test

**Files:**
- Create: `DataVo.Tests/E2E/DQL/ExplainTests.cs`

- [ ] **Step 1: Write failing test**

Create:

```csharp
namespace DataVo.Tests.E2E.DQL;

public class ExplainTests : SqlExecutionTestsBase
{
    [Fact]
    public void ExplainSelect_ReturnsPlannerDiagnosticsWithoutRows()
    {
        Execute("CREATE DATABASE ExplainDb");
        Execute("USE ExplainDb");
        Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR(50))");
        Execute("INSERT INTO Items VALUES (1, 'A')");

        var result = Execute("EXPLAIN SELECT * FROM Items WHERE Id = 1").Single();

        Assert.False(result.IsError, string.Join(Environment.NewLine, result.Messages));
        Assert.Contains("Plan", result.Fields);
        Assert.Contains("Physical", result.Fields);
        Assert.Contains("EstimatedCost", result.Fields);
        Assert.Contains("Reason", result.Fields);
        Assert.Single(result.Data);
        Assert.DoesNotContain(result.Data[0].Keys, key => key.Equals("Id", StringComparison.OrdinalIgnoreCase));
    }
}
```

- [ ] **Step 2: Verify RED**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter ExplainSelect_ReturnsPlannerDiagnosticsWithoutRows`

Expected: FAIL because `EXPLAIN` is not parsed.

## Task 2: Add Minimal Parser Support

**Files:**
- Modify: `DataVo.Core/Parser/Parser.cs`
- Create or modify AST/action wiring as needed following existing command patterns.

- [ ] **Step 1: Parse `EXPLAIN` before regular SELECT dispatch**

Add a parser branch for the keyword `EXPLAIN` that consumes it and parses the following `SELECT` statement into the existing `SelectStatement` AST.

The resulting runnable must be `new Explain(selectStatement)`.

- [ ] **Step 2: Verify parser reaches new action**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter ExplainSelect_ReturnsPlannerDiagnosticsWithoutRows`

Expected: FAIL because `Explain` action is missing or not implemented.

## Task 3: Implement Planner Diagnostics Action

**Files:**
- Create: `DataVo.Core/Parser/DQL/Explain.cs`
- Modify: `DataVo.Core/Parser/DQL/Select.Planner.cs`

- [ ] **Step 1: Expose planner diagnostics from Select**

Add an internal immutable record:

```csharp
internal sealed record SelectPlanDiagnostics(
    string LogicalPlan,
    string PhysicalPlan,
    int EstimatedCost,
    string Reason);
```

Add method on `Select`:

```csharp
internal SelectPlanDiagnostics ExplainPlanForCurrentModel()
{
    ExpressionNode? whereExpression = _model.WhereStatement.IsEvaluatable()
        ? _model.WhereStatement.GetExpression()
        : null;

    PhysicalPlanDecision plan = BuildPhysicalPlan(whereExpression);
    return new SelectPlanDiagnostics(
        plan.LogicalPlan.ToString(),
        plan.UseVolcano ? "Volcano" : "Legacy",
        plan.EstimatedCost,
        plan.Reason);
}
```

- [ ] **Step 2: Implement `Explain` action**

Create action that validates database, creates a `Select`, calls diagnostics, and returns one row:

```csharp
internal sealed class Explain(SelectStatement selectStatement) : BaseDbAction
{
    protected override DatabasePermission RequiredPermission => DatabasePermission.ReadData;

    public override void PerformAction(Guid session)
    {
        _ = GetDatabaseName(session);
        var select = new Select(selectStatement);
        select.UseEngine(Engine);
        var plan = select.ExplainPlanForCurrentModel();

        Fields = ["Plan", "Physical", "EstimatedCost", "Reason"];
        Data =
        [
            new Dictionary<string, object?>
            {
                ["Plan"] = plan.LogicalPlan,
                ["Physical"] = plan.PhysicalPlan,
                ["EstimatedCost"] = plan.EstimatedCost,
                ["Reason"] = plan.Reason
            }
        ];
        Messages.Add("Explain plan generated.");
    }
}
```

Include required `using DataVo.Core.Runtime.Security;`.

- [ ] **Step 3: Verify GREEN**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore --filter ExplainSelect_ReturnsPlannerDiagnosticsWithoutRows`

Expected: PASS.

## Task 4: Document Initial EXPLAIN Scope

**Files:**
- Modify: `docs/features/select-and-querying.md`

- [ ] **Step 1: Add docs**

Add:

```markdown
## EXPLAIN

`EXPLAIN SELECT ...` returns planner diagnostics without returning table rows. The initial output includes logical plan, physical execution family, estimated cost, and planner reason.

`EXPLAIN ANALYZE` is not part of the initial scope.
```

- [ ] **Step 2: Full verification**

Run: `dotnet test DataVo.Tests/DataVo.Tests.csproj --no-restore`

Expected: 0 failed.

