using DataVo.Core.Exceptions;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Parser.AST;
using DataVo.Core.Services;

namespace DataVo.Core.Parser.DQL;

internal partial class Select
{
    private Dictionary<string, TableDetail> MaterializeCtes(List<CteDefinitionNode> ctes, Guid session)
    {
        Dictionary<string, TableDetail> materialized = new(StringComparer.OrdinalIgnoreCase);

        foreach (CteDefinitionNode cte in ctes)
        {
            Dictionary<string, TableDetail> inherited = new(StringComparer.OrdinalIgnoreCase);

            foreach (KeyValuePair<string, TableDetail> table in _model.CteTables)
            {
                inherited[table.Key] = table.Value;
            }

            foreach (KeyValuePair<string, TableDetail> table in materialized)
            {
                inherited[table.Key] = table.Value;
            }

            var cteSelect = new Select(cte.Select);
            cteSelect.UseEngine(Engine);
            cteSelect._model.SetCteTables(inherited);

            var cteResult = cteSelect.Perform(session);
            if (cteResult.IsError)
            {
                throw new EvaluationException(cteResult.Messages.FirstOrDefault() ?? $"Failed to materialize CTE '{cte.Name.Name}'.");
            }

            List<Record> rows = [];
            long rowId = 1;
            foreach (Dictionary<string, object?> row in cteResult.Data)
            {
                Dictionary<string, object?> values = row.ToDictionary(entry => entry.Key, entry => (object?)entry.Value);
                rows.Add(new Record(rowId++, values));
            }

            materialized[cte.Name.Name] = new TableDetail(cte.Name.Name, null, [.. cteResult.Fields], rows);
        }

        return materialized;
    }
}
