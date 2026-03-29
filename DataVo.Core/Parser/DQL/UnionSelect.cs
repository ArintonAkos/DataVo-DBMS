using System.Globalization;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Exceptions;
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

                EnsureCompatibleShape(branchResult, Fields, Data);

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

    private static void EnsureCompatibleShape(QueryResult branchResult, List<string> baseFields, List<Dictionary<string, object?>> baseRows)
    {
        List<string> branchFields = branchResult.Fields;

        if (branchFields.Count != baseFields.Count)
        {
            throw new BindingException("UNION queries must project the same number of columns.");
        }

        for (int i = 0; i < baseFields.Count; i++)
        {
            string baseKind = InferColumnKind(baseRows, baseFields[i]);
            string branchKind = InferColumnKind(branchResult.Data, branchFields[i]);

            if (baseKind == ColumnKinds.Unknown || branchKind == ColumnKinds.Unknown)
            {
                continue;
            }

            if (!string.Equals(baseKind, branchKind, StringComparison.Ordinal))
            {
                throw new BindingException($"UNION column {i + 1} has incompatible types: {baseKind} vs {branchKind}.");
            }
        }
    }

    private static string InferColumnKind(List<Dictionary<string, object?>> rows, string fieldName)
    {
        foreach (var row in rows)
        {
            if (!row.TryGetValue(fieldName, out var value) || value == null)
            {
                continue;
            }

            return ClassifyValue(value);
        }

        return ColumnKinds.Unknown;
    }

    private static string ClassifyValue(object value)
    {
        Type type = value.GetType();

        if (type == typeof(string))
        {
            return ColumnKinds.String;
        }

        if (type == typeof(bool))
        {
            return ColumnKinds.Boolean;
        }

        if (type == typeof(DateOnly) || type == typeof(DateTime))
        {
            return ColumnKinds.Date;
        }

        if (IsNumericType(type))
        {
            return ColumnKinds.Numeric;
        }

        return type.Name;
    }

    private static bool IsNumericType(Type type)
    {
        return type == typeof(byte)
            || type == typeof(sbyte)
            || type == typeof(short)
            || type == typeof(ushort)
            || type == typeof(int)
            || type == typeof(uint)
            || type == typeof(long)
            || type == typeof(ulong)
            || type == typeof(float)
            || type == typeof(double)
            || type == typeof(decimal);
    }

    private static class ColumnKinds
    {
        public const string Unknown = "unknown";
        public const string Numeric = "numeric";
        public const string String = "string";
        public const string Boolean = "boolean";
        public const string Date = "date";
    }

    private static List<Dictionary<string, object?>> NormalizeRows(QueryResult result, List<string> baseFields)
    {
        var normalized = new List<Dictionary<string, object?>>(result.Data.Count);

        foreach (var row in result.Data)
        {
            Dictionary<string, object?> mapped = [];

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

    private static List<Dictionary<string, object?>> ApplyOrderBy(List<Dictionary<string, object?>> rows, List<string> fields, OrderByNode? orderBy)
    {
        if (orderBy == null || orderBy.Columns.Count == 0)
        {
            return rows;
        }

        IOrderedEnumerable<Dictionary<string, object?>>? ordered = null;

        foreach (var orderCol in orderBy.Columns)
        {
            string fieldName = CanonicalizeFieldName(orderCol.Column.Name);
            Func<Dictionary<string, object?>, object?> keySelector = row => ResolveFieldValue(row, fieldName);

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

    private static List<Dictionary<string, object?>> ApplyLimit(List<Dictionary<string, object?>> rows, LimitNode? limit)
    {
        if (limit == null)
        {
            return rows;
        }

        IEnumerable<Dictionary<string, object?>> query = rows;

        if (limit.SkipTarget > 0)
        {
            query = query.Skip(limit.SkipTarget);
        }

        return query.Take(limit.TakeTarget).ToList();
    }

    private static object? ResolveFieldValue(Dictionary<string, object?> row, string fieldName)
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

        throw new EvaluationException($"Compound ORDER BY column '{fieldName}' is not present in the UNION result.");
    }

    private static List<Dictionary<string, object?>> DistinctRows(List<Dictionary<string, object?>> rows, List<string> fields)
    {
        HashSet<string> seen = [];
        List<Dictionary<string, object?>> distinctRows = [];

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

    private static string BuildRowSignature(Dictionary<string, object?> row, List<string> fields)
    {
        string[] values = new string[fields.Count];

        for (int i = 0; i < fields.Count; i++)
        {
            object? value = row.TryGetValue(fields[i], out var candidate) ? candidate : null;
            values[i] = BuildValueSignature(value);
        }

        return string.Join("|", values);
    }

    private static string BuildValueSignature(object? value)
    {
        if (value == null)
        {
            return "<null>";
        }

        return value switch
        {
            string s => $"System.String:{s}",
            char c => $"System.Char:{c}",
            bool b => $"System.Boolean:{(b ? "1" : "0")}",
            byte v => $"System.Byte:{v}",
            sbyte v => $"System.SByte:{v}",
            short v => $"System.Int16:{v}",
            ushort v => $"System.UInt16:{v}",
            int v => $"System.Int32:{v}",
            uint v => $"System.UInt32:{v}",
            long v => $"System.Int64:{v}",
            ulong v => $"System.UInt64:{v}",
            float v => $"System.Single:{v.ToString(CultureInfo.InvariantCulture)}",
            double v => $"System.Double:{v.ToString(CultureInfo.InvariantCulture)}",
            decimal v => $"System.Decimal:{v.ToString(CultureInfo.InvariantCulture)}",
            DateTime v => $"System.DateTime:{v.ToUniversalTime():O}",
            DateTimeOffset v => $"System.DateTimeOffset:{v.ToUniversalTime():O}",
            Guid v => $"System.Guid:{v}",
            byte[] bytes => $"System.Byte[]:{Convert.ToBase64String(bytes)}",
            _ => $"{value.GetType().FullName}:{Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty}"
        };
    }
}