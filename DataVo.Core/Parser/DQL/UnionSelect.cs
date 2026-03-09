using System.Text.Json;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Logging;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;
using DataVo.Core.Utils;

namespace DataVo.Core.Parser.DQL;

internal class UnionSelect(UnionSelectStatement ast) : BaseDbAction
{
    public override void PerformAction(Guid session)
    {
        try
        {
            QueryResult leftResult = ExecuteSelect(ast.Left, session);
            if (leftResult.IsError)
            {
                Messages = leftResult.Messages;
                Data = leftResult.Data;
                Fields = leftResult.Fields;
                return;
            }

            Fields = [.. leftResult.Fields.Select(CanonicalizeFieldName)];
            Data = NormalizeRows(leftResult, Fields);

            foreach (var branch in ast.Branches)
            {
                QueryResult branchResult = ExecuteSelect(branch.Select, session);
                if (branchResult.IsError)
                {
                    Messages = branchResult.Messages;
                    Data = branchResult.Data;
                    Fields = branchResult.Fields;
                    return;
                }

                EnsureCompatibleShape(branchResult.Fields, Fields);

                var normalizedBranchRows = NormalizeRows(branchResult, Fields);
                Data.AddRange(normalizedBranchRows);

                if (!branch.IsAll)
                {
                    Data = DistinctRows(Data, Fields);
                }
            }

            Data = ApplyOrderBy(Data, Fields, ast.OrderByExpression);
            Data = ApplyLimit(Data, ast.LimitExpression);

            Logger.Info($"Rows selected: {Data.Count}");
            Messages.Add($"Rows selected: {Data.Count}");
        }
        catch (Exception ex)
        {
            Messages.Add($"Error: {ex.Message}");
            Logger.Error(ex.ToString());
        }
    }

    private QueryResult ExecuteSelect(SelectStatement statement, Guid session)
    {
        var select = new Select(statement);
        select.UseEngine(Engine);
        return select.Perform(session);
    }

    private static void EnsureCompatibleShape(List<string> branchFields, List<string> baseFields)
    {
        if (branchFields.Count != baseFields.Count)
        {
            throw new Exception("UNION queries must project the same number of columns.");
        }
    }

    private static List<Dictionary<string, dynamic>> NormalizeRows(QueryResult result, List<string> baseFields)
    {
        var normalized = new List<Dictionary<string, dynamic>>(result.Data.Count);

        foreach (var row in result.Data)
        {
            Dictionary<string, dynamic> mapped = [];

            for (int i = 0; i < baseFields.Count; i++)
            {
                string targetField = baseFields[i];
                string sourceField = result.Fields[i];
                mapped[targetField] = row.TryGetValue(sourceField, out var value) ? value! : null!;
            }

            normalized.Add(mapped);
        }

        return normalized;
    }

    private static string CanonicalizeFieldName(string field)
    {
        int aliasIndex = field.IndexOf(" AS ", StringComparison.OrdinalIgnoreCase);
        if (aliasIndex >= 0)
        {
            return field[(aliasIndex + 4)..].Trim();
        }

        return field;
    }

    private static List<Dictionary<string, dynamic>> ApplyOrderBy(List<Dictionary<string, dynamic>> rows, List<string> fields, OrderByNode? orderBy)
    {
        if (orderBy == null || orderBy.Columns.Count == 0)
        {
            return rows;
        }

        IOrderedEnumerable<Dictionary<string, dynamic>>? ordered = null;

        foreach (var orderCol in orderBy.Columns)
        {
            string fieldName = CanonicalizeFieldName(orderCol.Column.Name);
            Func<Dictionary<string, dynamic>, object?> keySelector = row => ResolveFieldValue(row, fieldName);

            ordered = ordered == null
                ? (orderCol.IsAscending
                    ? rows.OrderBy(keySelector, DynamicObjectComparer.Instance)
                    : rows.OrderByDescending(keySelector, DynamicObjectComparer.Instance))
                : (orderCol.IsAscending
                    ? ordered.ThenBy(keySelector, DynamicObjectComparer.Instance)
                    : ordered.ThenByDescending(keySelector, DynamicObjectComparer.Instance));
        }

        return ordered?.ToList() ?? rows;
    }

    private static List<Dictionary<string, dynamic>> ApplyLimit(List<Dictionary<string, dynamic>> rows, LimitNode? limit)
    {
        if (limit == null)
        {
            return rows;
        }

        IEnumerable<Dictionary<string, dynamic>> query = rows;

        if (limit.SkipTarget > 0)
        {
            query = query.Skip(limit.SkipTarget);
        }

        return query.Take(limit.TakeTarget).ToList();
    }

    private static object? ResolveFieldValue(Dictionary<string, dynamic> row, string fieldName)
    {
        if (row.TryGetValue(fieldName, out var value))
        {
            return value;
        }

        if (fieldName.Contains('.'))
        {
            string unqualified = fieldName.Split('.', 2)[1];
            if (row.TryGetValue(unqualified, out value))
            {
                return value;
            }
        }

        throw new Exception($"Compound ORDER BY column '{fieldName}' is not present in the UNION result.");
    }

    private static List<Dictionary<string, dynamic>> DistinctRows(List<Dictionary<string, dynamic>> rows, List<string> fields)
    {
        HashSet<string> seen = [];
        List<Dictionary<string, dynamic>> distinctRows = [];

        foreach (var row in rows)
        {
            string signature = BuildRowSignature(row, fields);
            if (seen.Add(signature))
            {
                distinctRows.Add(row);
            }
        }

        return distinctRows;
    }

    private static string BuildRowSignature(Dictionary<string, dynamic> row, List<string> fields)
    {
        object?[] values = new object?[fields.Count];

        for (int i = 0; i < fields.Count; i++)
        {
            values[i] = row.TryGetValue(fields[i], out var value) ? value : null;
        }

        return JsonSerializer.Serialize(values);
    }
}