using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.DQL;

internal class Explain(ExplainStatement ast) : BaseDbAction
{
    public override void PerformAction(Guid session)
    {
        try
        {
            var select = new Select(ast.Select);
            select.UseEngine(Engine);
            SelectPlannerDiagnostics diagnostics = select.BuildPlannerDiagnostics(session);

            Fields.AddRange(["Plan", "Physical", "EstimatedCost", "Reason"]);
            Data.Add(new Dictionary<string, object?>
            {
                ["Plan"] = diagnostics.Plan,
                ["Physical"] = diagnostics.Physical,
                ["EstimatedCost"] = diagnostics.EstimatedCost,
                ["Reason"] = diagnostics.Reason
            });
            Messages.Add("Rows selected: 1");
        }
        catch (Exception ex)
        {
            AddError(ex);
        }
    }
}
