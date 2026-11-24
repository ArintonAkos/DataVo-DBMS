using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime.Diagnostics;

namespace DataVo.Core.Parser.DQL;

/// <summary>
/// <c>EXPLAIN SELECT ...</c>: reports the planner's estimate and, by running the statement once under a
/// <see cref="QueryExecutionTrace"/>, the operator that actually produced its rows, whether that was a zero-allocation
/// fast path, and why the statement fell back to the general evaluator when it did.
/// </summary>
internal class Explain(ExplainStatement ast) : BaseDbAction
{
    /// <summary>The columns of the single row EXPLAIN returns, in order.</summary>
    internal static readonly string[] Columns =
    [
        "Plan", "Physical", "EstimatedCost", "Reason",
        "Operator", "FastPath", "FallbackReason", "DeclinedPaths",
        "EstimatedRows", "RowsScanned", "RowsReturned",
        "SegmentsRead", "SegmentsSkipped", "IndexesUsed", "Details",
    ];

    public override void PerformAction(Guid session)
    {
        try
        {
            var planner = new Select(ast.Select);
            planner.UseEngine(Engine);
            SelectPlannerDiagnostics diagnostics = planner.BuildPlannerDiagnostics(session);

            // The planner's estimate alone cannot say which fast path takes the statement: dispatch decides that
            // while reading. A SELECT has no side effects, so run it once with the trace on and report what ran.
            var traced = new Select(ast.Select);
            traced.UseEngine(Engine);
            QueryExecutionTrace trace;
            using (QueryExecutionTrace.Begin(out trace))
            {
                traced.PerformAction(session);
            }

            // A statement the planner accepts but that fails when run (a subquery over a missing table) still gets
            // its plan; the failure is reported in Details instead of failing EXPLAIN.
            string? failure = traced.Failed ? string.Join(" ", traced.StatementMessages) : null;

            Fields.AddRange(Columns);
            Data.Add(new Dictionary<string, object?>
            {
                ["Plan"] = diagnostics.Plan,
                ["Physical"] = diagnostics.Physical,
                ["EstimatedCost"] = diagnostics.EstimatedCost,
                ["Reason"] = diagnostics.Reason,
                ["Operator"] = failure is null ? trace.Operator : null,
                ["FastPath"] = failure is null && trace.FastPath,
                ["FallbackReason"] = trace.FallbackReason,
                ["DeclinedPaths"] = trace.Declines.Count == 0 ? null : string.Join("; ", trace.Declines),
                ["EstimatedRows"] = trace.EstimatedRows >= 0 ? trace.EstimatedRows : null,
                ["RowsScanned"] = trace.RowsScanned >= 0 ? trace.RowsScanned : null,
                ["RowsReturned"] = failure is null ? (long)traced.ReturnedRowCount : null,
                ["SegmentsRead"] = trace.SegmentsRead,
                ["SegmentsSkipped"] = trace.SegmentsSkipped,
                ["IndexesUsed"] = trace.Indexes.Count == 0 ? null : string.Join(", ", trace.Indexes),
                ["Details"] = failure is null ? trace.Detail : $"execution failed: {failure}",
            });
            Messages.Add("Rows selected: 1");
        }
        catch (Exception ex)
        {
            AddError(ex);
        }
    }
}
