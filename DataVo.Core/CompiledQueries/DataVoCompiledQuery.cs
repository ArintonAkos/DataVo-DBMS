using System.Globalization;
using DataVo.Core.BTree;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.MVCC;
using DataVo.Core.StorageEngine;
using DataVo.Core.Utils;

namespace DataVo.Core.CompiledQueries;

/// <summary>
/// Executes source-generated compiled query plans against a <see cref="DataVoContext"/>.
/// </summary>
public static class DataVoCompiledQuery
{
    /// <summary>
    /// Executes a select plan and returns the first mapped row, or <c>default</c> when no row matches.
    /// </summary>
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

    /// <summary>
    /// Executes a select plan and returns every mapped row that matches the plan predicate.
    /// </summary>
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

    /// <summary>
    /// Executes an insert plan for a single row and returns inserted row identifiers.
    /// </summary>
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

    /// <summary>
    /// Executes an update plan and returns the affected row count.
    /// </summary>
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

        if (context.Engine.TransactionManager.HasActiveTransaction(context.SessionId))
        {
            throw new NotSupportedException("Compiled update plans do not support active transactions.");
        }

        string databaseName = ResolveCurrentDatabase(context);
        Dictionary<string, object?> parameterDictionary = ToParameterDictionary(parameters);
        object? expected = RequiredParameter(parameterDictionary, plan.WhereParameterName!);
        string expectedKey = BuildComparisonKey(plan.WhereColumn!, expected);
        List<KeyValuePair<long, Dictionary<string, object?>>> candidates = TryReadMatchingRowEntries(context, plan, databaseName, expectedKey);
        if (candidates.Count == 0)
        {
            return 0;
        }

        List<long> candidateRowIds = candidates.Select(static pair => pair.Key).ToList();
        List<long> rowWriteLocks = context.Engine.LockManager.AcquireRowWriteLocks(databaseName, plan.TableName, candidateRowIds);

        try
        {
            List<long> revalidatedRowIds = RevalidateMatchingRowIdsAfterLock(context, plan, databaseName, expectedKey, candidateRowIds, rowWriteLocks);
            if (revalidatedRowIds.Count == 0)
            {
                return 0;
            }

            Dictionary<long, Dictionary<string, object?>> existingRows =
                context.Engine.StorageContext.GetTableContents(revalidatedRowIds, plan.TableName, databaseName);
            List<long> orderedRowIds = revalidatedRowIds
                .Where(existingRows.ContainsKey)
                .ToList();

            if (orderedRowIds.Count == 0)
            {
                return 0;
            }

            foreach (long rowId in orderedRowIds)
            {
                MvccCoordinator.ValidateCanModifyRow(context.Engine, databaseName, plan.TableName, rowId, null, "UPDATE");
            }

            Dictionary<string, Column> columnsByName = GetTableColumnsByName(context, plan.TableName, databaseName);
            List<Dictionary<string, object?>> newRows = orderedRowIds
                .Select(rowId => ApplyAssignments(existingRows[rowId], plan, parameterDictionary, columnsByName))
                .ToList();

            ValidateUpdatedRows(context, plan.TableName, databaseName, orderedRowIds, existingRows, newRows);

            long statementTxId = MvccCoordinator.ResolveStatementTransactionId(context.Engine, null);
            ReplaceRows(context, plan.TableName, databaseName, orderedRowIds, newRows, statementTxId);
            return newRows.Count;
        }
        finally
        {
            context.Engine.LockManager.ReleaseRowWriteLocks(databaseName, plan.TableName, rowWriteLocks);
        }
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

        List<Dictionary<string, object?>> rows = TryReadMatchingRowEntries(context, plan, databaseName, expectedKey)
            .Select(static pair => pair.Value)
            .ToList();

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

    private static List<KeyValuePair<long, Dictionary<string, object?>>> TryReadMatchingRowEntries(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        string databaseName,
        string expectedKey)
    {
        List<string> primaryKeys = context.Engine.Catalog.GetTablePrimaryKeys(plan.TableName, databaseName);
        bool isPrimaryKeyPredicate = primaryKeys.Contains(plan.WhereColumn!, StringComparer.OrdinalIgnoreCase);

        if (isPrimaryKeyPredicate)
        {
            string primaryKeyIndexName = $"_PK_{plan.TableName}";

            try
            {
                List<long> ids =
                [
                    .. context.Engine.IndexManager.FilterUsingIndex(expectedKey, primaryKeyIndexName, plan.TableName, databaseName)
                ];

                Dictionary<long, StoredRow> indexedRows =
                    context.Engine.StorageContext.GetTypedTableContents(ids, plan.TableName, databaseName);

                List<KeyValuePair<long, Dictionary<string, object?>>> matches = ids
                    .Where(indexedRows.ContainsKey)
                    .Select(id => new KeyValuePair<long, Dictionary<string, object?>>(
                        id,
                        MaterializeStoredRow(indexedRows[id])))
                    .ToList();

                if (matches.Count > 0)
                {
                    return matches;
                }
            }
            catch (IndexException ex) when (IsMissingPrimaryKeyIndex(ex, primaryKeyIndexName, plan.TableName))
            {
            }
        }

        Dictionary<long, StoredRow> scanned =
            context.Engine.StorageContext.GetTypedTableContents(plan.TableName, databaseName);

        return scanned
            .Select(pair => new KeyValuePair<long, Dictionary<string, object?>>(
                pair.Key,
                MaterializeStoredRow(pair.Value)))
            .Where(pair => pair.Value.ContainsKey(plan.WhereColumn!)
                && string.Equals(
                    IndexKeyEncoder.BuildKeyString(pair.Value, [plan.WhereColumn!]),
                    expectedKey,
                    StringComparison.Ordinal))
            .ToList();
    }

    private static Dictionary<string, object?> MaterializeStoredRow(StoredRow row)
    {
        StoredRowDictionaryView view = row.AsDictionary();
        var result = new Dictionary<string, object?>(view.Count, StringComparer.OrdinalIgnoreCase);
        foreach ((string key, object? value) in view)
        {
            result[key] = value;
        }

        return result;
    }

    private static List<long> RevalidateMatchingRowIdsAfterLock(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        string databaseName,
        string expectedKey,
        IReadOnlyList<long> originalCandidateRowIds,
        IReadOnlyList<long> lockCoveredRowIds)
    {
        if (originalCandidateRowIds.Count == 0)
        {
            return [];
        }

        HashSet<long> originalCandidates = [.. originalCandidateRowIds];
        HashSet<long> lockCovered = [.. lockCoveredRowIds];

        return TryReadMatchingRowEntries(context, plan, databaseName, expectedKey)
            .Select(static pair => pair.Key)
            .Where(originalCandidates.Contains)
            .Where(rowId => rowId <= 0 || lockCovered.Count == 0 || lockCovered.Contains(rowId))
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

    private static object? RequiredParameter(IReadOnlyDictionary<string, object?> parameters, string parameterName)
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

    private static Dictionary<string, Column> GetTableColumnsByName(DataVoContext context, string tableName, string databaseName)
    {
        return context.Engine.Catalog
            .GetTableColumns(tableName, databaseName)
            .ToDictionary(
                column => column.Name,
                column => new Column
                {
                    Name = column.Name,
                    Type = column.Type,
                    Length = column.Length,
                    DefaultValue = column.DefaultValue
                },
                StringComparer.OrdinalIgnoreCase);
    }

    private static Dictionary<string, object?> ApplyAssignments(
        IReadOnlyDictionary<string, object?> oldRow,
        DataVoCompiledQueryPlan plan,
        IReadOnlyDictionary<string, object?> parameters,
        IReadOnlyDictionary<string, Column> columnsByName)
    {
        var newRow = new Dictionary<string, object?>(oldRow, StringComparer.OrdinalIgnoreCase);

        foreach ((string columnName, string parameterName) in plan.Assignments)
        {
            if (!columnsByName.TryGetValue(columnName, out Column? column))
            {
                throw new BindingException($"Column {columnName} doesn't exist in table {plan.TableName}!");
            }

            object? rawValue = RequiredParameter(parameters, parameterName);
            newRow[columnName] = NormalizeAssignedValue(column, rawValue);
        }

        return newRow;
    }

    private static object? NormalizeAssignedValue(Column columnMetadata, object? rawValue)
    {
        if (rawValue == null || rawValue is DBNull)
        {
            return null;
        }

        var column = new Column
        {
            Name = columnMetadata.Name,
            Type = columnMetadata.Type,
            Length = columnMetadata.Length,
            DefaultValue = columnMetadata.DefaultValue
        };

        string formattedValue = FormatColumnValue(rawValue, column.Type);
        column.Value = formattedValue;

        object? parsedValue = column.ParsedValue;
        if (!IsParsedValueCompatible(column, parsedValue))
        {
            throw new EvaluationException($"Type of argument doesn't match with column type for {column.Name}!");
        }

        return NormalizeParsedValue(column, parsedValue);
    }

    private static string FormatColumnValue(object rawValue, string columnType)
    {
        if (rawValue is string text)
        {
            return text;
        }

        if (columnType.Equals("VECTOR", StringComparison.OrdinalIgnoreCase)
            && VectorParser.TryCoerceToVector(rawValue, out float[] vector))
        {
            return VectorParser.SerializeVector(vector);
        }

        if (rawValue is DateOnly dateOnly)
        {
            return dateOnly.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }

        if (rawValue is DateTime dateTime && columnType.Equals("DATE", StringComparison.OrdinalIgnoreCase))
        {
            return DateOnly.FromDateTime(dateTime).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        }

        if (rawValue is IFormattable formattable)
        {
            return formattable.ToString(null, CultureInfo.InvariantCulture);
        }

        return Convert.ToString(rawValue, CultureInfo.InvariantCulture) ?? "null";
    }

    private static bool IsParsedValueCompatible(Column column, object? parsedValue)
    {
        if (parsedValue == null)
        {
            return true;
        }

        return column.Type.ToUpperInvariant() switch
        {
            "INT" => parsedValue is int,
            "FLOAT" => parsedValue is double,
            "BIT" => parsedValue is bool,
            "DATE" => parsedValue is DateOnly,
            "VECTOR" => parsedValue is float[] vector && (column.Length <= 0 || vector.Length == column.Length),
            _ => true
        };
    }

    private static object? NormalizeParsedValue(Column column, object? parsedValue)
    {
        if (parsedValue is float[] vector)
        {
            return vector.ToArray();
        }

        if (column.Type.Equals("DATE", StringComparison.OrdinalIgnoreCase) && parsedValue is DateTime dateTime)
        {
            return DateOnly.FromDateTime(dateTime);
        }

        return parsedValue;
    }

    private static void ValidateUpdatedRows(
        DataVoContext context,
        string tableName,
        string databaseName,
        IReadOnlyList<long> rowIds,
        IReadOnlyDictionary<long, Dictionary<string, object?>> existingRows,
        IReadOnlyList<Dictionary<string, object?>> newRows)
    {
        List<string> primaryKeys = context.Engine.Catalog.GetTablePrimaryKeys(tableName, databaseName);
        List<string> uniqueKeys = context.Engine.Catalog.GetTableUniqueKeys(tableName, databaseName);
        List<ForeignKey> foreignKeys = context.Engine.Catalog.GetTableForeignKeys(tableName, databaseName);
        List<(string ChildTable, string ChildColumn, string ParentColumn, string OnDeleteAction)> childForeignKeys =
            context.Engine.Catalog.GetChildForeignKeys(tableName, databaseName);

        Dictionary<string, HashSet<string>> batchUniqueValues = InitializeBatchUniqueTracker(primaryKeys, uniqueKeys);

        for (int i = 0; i < rowIds.Count; i++)
        {
            long rowId = rowIds[i];
            Dictionary<string, object?> oldRow = existingRows[rowId];
            Dictionary<string, object?> newRow = newRows[i];
            int rowNumber = i + 1;

            ValidatePrimaryAndUniqueConstraints(context, tableName, databaseName, newRow, oldRow, primaryKeys, uniqueKeys, batchUniqueValues, rowNumber);
            ValidateForeignKeyConstraints(context, databaseName, newRow, oldRow, foreignKeys, rowNumber);
            ValidateChildForeignKeyConstraints(context, tableName, databaseName, newRow, oldRow, childForeignKeys);
        }
    }

    private static Dictionary<string, HashSet<string>> InitializeBatchUniqueTracker(
        IReadOnlyList<string> primaryKeys,
        IReadOnlyList<string> uniqueKeys)
    {
        Dictionary<string, HashSet<string>> batchUniqueValues = new(StringComparer.OrdinalIgnoreCase);
        foreach (string columnName in primaryKeys.Concat(uniqueKeys))
        {
            batchUniqueValues[columnName] = new HashSet<string>(StringComparer.Ordinal);
        }

        return batchUniqueValues;
    }

    private static void ValidatePrimaryAndUniqueConstraints(
        DataVoContext context,
        string tableName,
        string databaseName,
        Dictionary<string, object?> newRow,
        IReadOnlyDictionary<string, object?> oldRow,
        IReadOnlyList<string> primaryKeys,
        IReadOnlyList<string> uniqueKeys,
        IReadOnlyDictionary<string, HashSet<string>> batchUniqueValues,
        int rowNumber)
    {
        foreach (string columnName in primaryKeys.Concat(uniqueKeys))
        {
            if (!newRow.TryGetValue(columnName, out object? value))
            {
                continue;
            }

            if (value == null && primaryKeys.Contains(columnName, StringComparer.OrdinalIgnoreCase))
            {
                throw new EvaluationException($"Constraint violation: Primary key column {columnName} cannot be null in row {rowNumber}.");
            }

            if (value == null)
            {
                continue;
            }

            string valueText = value.ToString()!;
            string oldValueText = oldRow.TryGetValue(columnName, out object? oldValue)
                ? oldValue?.ToString() ?? "null_val"
                : "null_val";

            if (valueText == oldValueText)
            {
                continue;
            }

            if (!batchUniqueValues[columnName].Add(valueText))
            {
                throw new EvaluationException($"Update conflict: Duplicate value '{valueText}' generated within the same batch for unique column {columnName}.");
            }

            string indexName = primaryKeys.Contains(columnName, StringComparer.OrdinalIgnoreCase)
                ? $"_PK_{tableName}"
                : $"_UK_{columnName}";

            bool duplicateExists;
            try
            {
                duplicateExists = context.Engine.IndexManager.IndexContainsKey(valueText, indexName, tableName, databaseName);
            }
            catch
            {
                duplicateExists = context.Engine.StorageContext.GetTableContents(tableName, databaseName)
                    .Any(entry => entry.Value.TryGetValue(columnName, out object? existing)
                                  && existing != null
                                  && string.Equals(existing.ToString(), valueText, StringComparison.Ordinal));
            }

            if (duplicateExists)
            {
                throw new EvaluationException($"Constraint violation: Duplicate value '{valueText}' for unique column {columnName} in row {rowNumber}.");
            }
        }
    }

    private static void ValidateForeignKeyConstraints(
        DataVoContext context,
        string databaseName,
        Dictionary<string, object?> newRow,
        IReadOnlyDictionary<string, object?> oldRow,
        IReadOnlyList<ForeignKey> foreignKeys,
        int rowNumber)
    {
        foreach (ForeignKey foreignKey in foreignKeys)
        {
            if (!newRow.TryGetValue(foreignKey.AttributeName, out object? foreignKeyValue))
            {
                continue;
            }

            string newValueText = foreignKeyValue?.ToString() ?? "null";
            string oldValueText = oldRow.TryGetValue(foreignKey.AttributeName, out object? oldValue)
                ? oldValue?.ToString() ?? "null"
                : "null";

            if (newValueText == oldValueText || newValueText == "null")
            {
                continue;
            }

            if (!CheckForeignKeyConstraint(context, foreignKey, newValueText, databaseName))
            {
                throw new EvaluationException($"Foreign key violation: Value '{newValueText}' does not reference an existing parent row for foreign key column {foreignKey.AttributeName} in row {rowNumber}.");
            }
        }
    }

    private static void ValidateChildForeignKeyConstraints(
        DataVoContext context,
        string tableName,
        string databaseName,
        Dictionary<string, object?> newRow,
        IReadOnlyDictionary<string, object?> oldRow,
        IReadOnlyList<(string ChildTable, string ChildColumn, string ParentColumn, string OnDeleteAction)> childForeignKeys)
    {
        foreach ((string childTable, string childColumn, string parentColumn, _) in childForeignKeys)
        {
            string oldParentValue = oldRow.TryGetValue(parentColumn, out object? oldValue)
                ? oldValue?.ToString() ?? "null"
                : "null";
            string newParentValue = newRow.TryGetValue(parentColumn, out object? newValue)
                ? newValue?.ToString() ?? "null"
                : "null";

            if (oldParentValue == newParentValue || oldParentValue == "null")
            {
                continue;
            }

            string childIndexName = $"_FK_{childTable}_{childColumn}";
            List<long> childRowIds;

            try
            {
                childRowIds = context.Engine.IndexManager.FilterUsingIndex(oldParentValue, childIndexName, childTable, databaseName).ToList();
            }
            catch
            {
                childRowIds = FindChildRowsByTableScan(context, childTable, childColumn, oldParentValue, databaseName);
            }

            childRowIds = childRowIds
                .Where(id => id != 0 && context.Engine.StorageContext.TableContainsRow(id, childTable, databaseName))
                .ToList();

            if (childRowIds.Count > 0)
            {
                throw new EvaluationException($"Foreign key violation: Cannot update {parentColumn} ('{oldParentValue}' -> '{newParentValue}') in {tableName} because {childRowIds.Count} row(s) in {childTable} depend on it.");
            }
        }
    }

    private static bool CheckForeignKeyConstraint(
        DataVoContext context,
        ForeignKey foreignKey,
        string columnValue,
        string databaseName)
    {
        foreach (Reference reference in foreignKey.References)
        {
            if (!ReferenceExists(context, reference.ReferenceTableName, reference.ReferenceAttributeName, columnValue, databaseName))
            {
                return false;
            }
        }

        return true;
    }

    private static bool ReferenceExists(
        DataVoContext context,
        string tableName,
        string columnName,
        string expectedValue,
        string databaseName)
    {
        try
        {
            if (context.Engine.IndexManager.IndexContainsKey(expectedValue, $"_PK_{tableName}", tableName, databaseName))
            {
                return true;
            }
        }
        catch
        {
        }

        return context.Engine.StorageContext.GetTableContents(tableName, databaseName)
            .Values
            .Any(row => row.TryGetValue(columnName, out object? value)
                        && value != null
                        && string.Equals(value.ToString(), expectedValue, StringComparison.Ordinal));
    }

    private static List<long> FindChildRowsByTableScan(
        DataVoContext context,
        string childTable,
        string childColumn,
        string parentValue,
        string databaseName)
    {
        return context.Engine.StorageContext.GetTableContents(childTable, databaseName)
            .Where(pair => pair.Value.TryGetValue(childColumn, out object? value)
                           && value != null
                           && string.Equals(value.ToString(), parentValue, StringComparison.Ordinal))
            .Select(static pair => pair.Key)
            .ToList();
    }

    private static void ReplaceRows(
        DataVoContext context,
        string tableName,
        string databaseName,
        IReadOnlyList<long> oldRowIds,
        IReadOnlyList<Dictionary<string, object?>> newRows,
        long statementTxId)
    {
        List<IndexFile> indexFiles = context.Engine.Catalog.GetTableIndexes(tableName, databaseName);

        context.Engine.StorageContext.DeleteFromTable(oldRowIds.ToList(), tableName, databaseName);
        foreach (IndexFile index in indexFiles)
        {
            string indexName = index.IndexFileName ?? string.Empty;
            if (string.IsNullOrWhiteSpace(indexName))
            {
                continue;
            }

            string indexKind = index.IndexKind ?? string.Empty;
            if (context.Engine.IndexManager.SupportsVectorIndexType(indexKind))
            {
                context.Engine.IndexManager.DeleteFromVectorIndex(oldRowIds.ToList(), indexName, tableName, databaseName, indexKind);
            }
            else
            {
                context.Engine.IndexManager.DeleteFromIndex(oldRowIds.ToList(), indexName, tableName, databaseName);
            }
        }

        for (int i = 0; i < newRows.Count; i++)
        {
            Dictionary<string, object?> newRow = newRows[i];
            long oldRowId = oldRowIds[i];
            long assignedRowId = context.Engine.StorageContext.InsertOneIntoTable(newRow, tableName, databaseName);
            MvccCoordinator.RegisterUpdateVersion(context.Engine, databaseName, tableName, oldRowId, assignedRowId, statementTxId);

            foreach (IndexFile index in indexFiles)
            {
                if (index.AttributeNames.Any(attributeName => !newRow.TryGetValue(attributeName, out object? attributeValue) || attributeValue == null))
                {
                    continue;
                }

                string indexName = index.IndexFileName ?? string.Empty;
                if (string.IsNullOrWhiteSpace(indexName))
                {
                    continue;
                }

                string indexKind = index.IndexKind ?? string.Empty;
                if (context.Engine.IndexManager.SupportsVectorIndexType(indexKind))
                {
                    if (index.AttributeNames.Count != 1)
                    {
                        throw new BindingException($"Vector index '{indexName}' (type '{indexKind}') must reference exactly one VECTOR column.");
                    }

                    string vectorColumn = index.AttributeNames[0];
                    if (!VectorParser.TryCoerceToVector(newRow[vectorColumn], out float[] vector))
                    {
                        throw new EvaluationException($"Cannot coerce value of '{vectorColumn}' into VECTOR for index '{indexName}'.");
                    }

                    context.Engine.IndexManager.InsertIntoVectorIndex(vector, assignedRowId, indexName, tableName, databaseName, indexKind);
                    continue;
                }

                string indexValue = IndexKeyEncoder.BuildKeyString(newRow, index.AttributeNames);
                context.Engine.IndexManager.InsertIntoIndex(indexValue, assignedRowId, indexName, tableName, databaseName);
            }
        }
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
