using System.Globalization;
using DataVo.Core.BTree;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Exceptions;

namespace DataVo.Core.CompiledQueries;

public static class DataVoCompiledQuery
{
    public static TResult? SelectSingle<TResult>(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        IReadOnlyList<DataVoCompiledQueryParameter> parameters,
        Func<Dictionary<string, object?>, TResult> mapper)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(parameters);
        ArgumentNullException.ThrowIfNull(mapper);

        if (plan.Kind != DataVoCompiledQueryKind.SelectSingle)
        {
            throw new InvalidOperationException($"Plan kind '{plan.Kind}' cannot be executed as SelectSingle.");
        }

        IReadOnlyList<TResult> rows = ExecuteSelect(context, plan, parameters, mapper);
        return rows.Count == 0 ? default : rows[0];
    }

    public static IReadOnlyList<TResult> SelectMany<TResult>(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        IReadOnlyList<DataVoCompiledQueryParameter> parameters,
        Func<Dictionary<string, object?>, TResult> mapper)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(parameters);
        ArgumentNullException.ThrowIfNull(mapper);

        if (plan.Kind != DataVoCompiledQueryKind.SelectMany && plan.Kind != DataVoCompiledQueryKind.SelectSingle)
        {
            throw new InvalidOperationException($"Plan kind '{plan.Kind}' cannot be executed as SelectMany.");
        }

        return ExecuteSelect(context, plan, parameters, mapper);
    }

    public static IReadOnlyList<long> Insert(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        IReadOnlyList<DataVoCompiledQueryParameter> parameters)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(parameters);

        if (plan.Kind != DataVoCompiledQueryKind.Insert)
        {
            throw new InvalidOperationException($"Plan kind '{plan.Kind}' cannot be executed as Insert.");
        }

        Dictionary<string, object?> parameterDictionary = ToParameterDictionary(parameters);
        var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < plan.InsertColumns.Count; i++)
        {
            row[plan.InsertColumns[i]] = RequiredParameter(parameterDictionary, plan.InsertParameterNames[i]);
        }

        return context.BulkInsert(plan.TableName, [row]);
    }

    public static int Update(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        IReadOnlyList<DataVoCompiledQueryParameter> parameters)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(parameters);

        if (plan.Kind != DataVoCompiledQueryKind.Update)
        {
            throw new InvalidOperationException($"Plan kind '{plan.Kind}' cannot be executed as Update.");
        }

        Dictionary<string, object?> parameterDictionary = ToParameterDictionary(parameters);
        string setClause = string.Join(
            ", ",
            plan.Assignments.Select(pair => $"{pair.Key} = {FormatSqlLiteral(RequiredParameter(parameterDictionary, pair.Value))}"));
        string whereValue = FormatSqlLiteral(RequiredParameter(parameterDictionary, plan.WhereParameterName!));

        QueryResult result = context.Execute(
            $"UPDATE {plan.TableName} SET {setClause} WHERE {plan.WhereColumn} = {whereValue}")
            .Single();

        foreach (string message in result.Messages)
        {
            const string prefix = "Rows affected:";
            if (message.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
                && int.TryParse(message[prefix.Length..].Trim(), out int affected))
            {
                return affected;
            }
        }

        return 0;
    }

    private static IReadOnlyList<TResult> ExecuteSelect<TResult>(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        IReadOnlyList<DataVoCompiledQueryParameter> parameters,
        Func<Dictionary<string, object?>, TResult> mapper)
    {
        string databaseName = ResolveCurrentDatabase(context);
        Dictionary<string, object?> parameterDictionary = ToParameterDictionary(parameters);
        object? expected = RequiredParameter(parameterDictionary, plan.WhereParameterName!);
        string expectedKey = BuildComparisonKey(plan.WhereColumn!, expected);

        List<Dictionary<string, object?>> rows = TryReadMatchingRows(context, plan, databaseName, expectedKey);

        return rows
            .Select(ProjectRowIfNeeded)
            .Select(mapper)
            .ToArray();

        Dictionary<string, object?> ProjectRowIfNeeded(Dictionary<string, object?> row)
        {
            if (plan.ProjectedColumns.Count == 0)
            {
                return row;
            }

            var projected = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
            foreach (string column in plan.ProjectedColumns)
            {
                projected[column] = row.TryGetValue(column, out object? value) ? value : null;
            }

            return projected;
        }
    }

    private static List<Dictionary<string, object?>> TryReadMatchingRows(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        string databaseName,
        string expectedKey)
    {
        string primaryKeyIndexName = $"_PK_{plan.TableName}";

        try
        {
            List<long> ids =
            [
                .. context.Engine.IndexManager.FilterUsingIndex(expectedKey, primaryKeyIndexName, plan.TableName, databaseName)
            ];

            Dictionary<long, Dictionary<string, object?>> indexedRows =
                context.Engine.StorageContext.GetTableContents(ids, plan.TableName, databaseName);

            List<Dictionary<string, object?>> matches = ids
                .Where(indexedRows.ContainsKey)
                .Select(id => indexedRows[id])
                .ToList();

            if (matches.Count > 0)
            {
                return matches;
            }
        }
        catch (IndexException ex) when (IsMissingPrimaryKeyIndex(ex, primaryKeyIndexName, plan.TableName))
        {
        }

        Dictionary<long, Dictionary<string, object?>> scanned =
            context.Engine.StorageContext.GetTableContents(plan.TableName, databaseName);

        return scanned.Values
            .Where(row => row.ContainsKey(plan.WhereColumn!)
                && string.Equals(
                    IndexKeyEncoder.BuildKeyString(row, [plan.WhereColumn!]),
                    expectedKey,
                    StringComparison.Ordinal))
            .ToList();
    }

    private static bool IsMissingPrimaryKeyIndex(IndexException exception, string indexName, string tableName)
    {
        return exception.Message.Contains($"Index {indexName} on table {tableName} does not exist", StringComparison.OrdinalIgnoreCase);
    }

    private static Dictionary<string, object?> ToParameterDictionary(IReadOnlyList<DataVoCompiledQueryParameter> parameters)
    {
        var result = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        foreach (DataVoCompiledQueryParameter parameter in parameters)
        {
            if (string.IsNullOrWhiteSpace(parameter.Name))
            {
                throw new ArgumentException("Compiled query parameter names cannot be null or whitespace.", nameof(parameters));
            }

            if (result.ContainsKey(parameter.Name))
            {
                throw new ArgumentException($"Duplicate compiled query parameter '{parameter.Name}'.", nameof(parameters));
            }

            result[parameter.Name] = parameter.Value;
        }

        return result;
    }

    private static object? RequiredParameter(Dictionary<string, object?> parameters, string parameterName)
    {
        if (!parameters.TryGetValue(parameterName, out object? value))
        {
            throw new ArgumentException($"Missing compiled query parameter '{parameterName}'.", nameof(parameters));
        }

        return value;
    }

    private static string ResolveCurrentDatabase(DataVoContext context)
    {
        string? databaseName = context.Engine.Sessions.Get(context.SessionId);
        if (string.IsNullOrWhiteSpace(databaseName))
        {
            throw new InvalidOperationException("No database selected for the current session. Execute USE <database> first.");
        }

        return databaseName;
    }

    private static string FormatSqlLiteral(object? value)
    {
        return value switch
        {
            null => "NULL",
            string text => $"'{text.Replace("'", "''", StringComparison.Ordinal)}'",
            char character => $"'{character.ToString().Replace("'", "''", StringComparison.Ordinal)}'",
            bool flag => flag ? "true" : "false",
            DateOnly date => $"'{date:yyyy-MM-dd}'",
            DateTime dateTime => $"'{dateTime:yyyy-MM-dd HH:mm:ss.fffffff}'",
            DateTimeOffset dateTimeOffset => $"'{dateTimeOffset:yyyy-MM-dd HH:mm:ss.fffffff zzz}'",
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture) ?? "NULL",
            _ => $"'{value.ToString()?.Replace("'", "''", StringComparison.Ordinal)}'"
        };
    }

    private static string BuildComparisonKey(string columnName, object? value)
    {
        return IndexKeyEncoder.BuildKeyString(
            new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase)
            {
                [columnName] = value
            },
            [columnName]);
    }
}
