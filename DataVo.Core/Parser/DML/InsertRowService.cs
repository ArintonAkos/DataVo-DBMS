using System.Globalization;
using DataVo.Core.BTree;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.MVCC;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.Transactions;
using DataVo.Core.Utils;
using PolyIndexManager = DataVo.Core.Indexing.IndexManager;

namespace DataVo.Core.Parser.DML;

internal sealed class InsertRowService(
    DataVoEngine engine,
    StorageContext context,
    EngineCatalog catalog,
    PolyIndexManager indexes)
{
    public InsertRowsResult InsertRows(
        string databaseName,
        string tableName,
        IReadOnlyList<IReadOnlyDictionary<string, object?>> rows,
        TransactionContext? txContext,
        long statementTxId,
        ChangeRecorder? recorder = null)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("Table name is required.", nameof(tableName));
        }

        if (rows.Count == 0)
        {
            return new InsertRowsResult([], 0, []);
        }

        List<string> primaryKeys = catalog.GetTablePrimaryKeys(tableName, databaseName);
        List<string> uniqueKeys = catalog.GetTableUniqueKeys(tableName, databaseName);
        List<ForeignKey> foreignKeys = catalog.GetTableForeignKeys(tableName, databaseName);
        List<IndexFile> indexFiles = catalog.GetTableIndexes(tableName, databaseName);
        List<Column> tableColumns = catalog.GetTableColumns(tableName, databaseName);

        HashSet<string> columnNames = tableColumns
            .Select(column => column.Name)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var foreignKeysByAttribute = foreignKeys
            .GroupBy(foreignKey => foreignKey.AttributeName, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => group.First(), StringComparer.OrdinalIgnoreCase);

        var acceptedRows = new List<Dictionary<string, object?>>(rows.Count);
        var messages = new List<string>();
        var acceptedPrimaryKeys = new HashSet<string>(StringComparer.Ordinal);
        var acceptedUniqueValues = uniqueKeys.ToDictionary(
            key => key,
            _ => new HashSet<string>(StringComparer.Ordinal),
            StringComparer.OrdinalIgnoreCase);

        for (int i = 0; i < rows.Count; i++)
        {
            int rowNumber = i + 1;
            Dictionary<string, object?> inputRow = MaterializeRow(rows[i]);
            EnsureKnownColumns(inputRow, columnNames, tableName);

            if (TryNormalizeRow(
                tableName,
                databaseName,
                rowNumber,
                inputRow,
                tableColumns,
                primaryKeys,
                uniqueKeys,
                foreignKeysByAttribute,
                acceptedPrimaryKeys,
                acceptedUniqueValues,
                messages,
                out Dictionary<string, object?> normalized))
            {
                acceptedRows.Add(normalized);
            }
        }

        if (acceptedRows.Count == 0)
        {
            return new InsertRowsResult([], 0, messages);
        }

        if (txContext != null)
        {
            foreach (Dictionary<string, object?> row in acceptedRows)
            {
                txContext.BufferInsert(
                    tableName,
                    row.ToDictionary(pair => pair.Key, pair => pair.Value, row.Comparer));
            }

            return new InsertRowsResult([], acceptedRows.Count, messages);
        }

        List<long> rowIds = context.InsertIntoTable(acceptedRows, tableName, databaseName);

        for (int i = 0; i < acceptedRows.Count; i++)
        {
            long rowId = rowIds[i];
            MvccCoordinator.RegisterInsertVersion(engine, databaseName, tableName, rowId, statementTxId);
            InsertIndexes(tableName, databaseName, acceptedRows[i], rowId, indexFiles);
            recorder?.RecordInsert(tableName, rowId, acceptedRows[i]);
        }

        return new InsertRowsResult(rowIds, acceptedRows.Count, messages);
    }

    public long InsertTypedRow(
        string databaseName,
        string tableName,
        ReactiveRowSchema columns,
        ReadOnlySpan<CellValue> row,
        long statementTxId,
        ChangeRecorder? recorder = null)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("Table name is required.", nameof(tableName));
        }

        if (columns.ColumnCount != row.Length)
        {
            throw new ArgumentException(
                $"Typed row schema has {columns.ColumnCount} columns but row has {row.Length} cells.",
                nameof(row));
        }

        List<string> primaryKeys = catalog.GetTablePrimaryKeys(tableName, databaseName);
        List<string> uniqueKeys = catalog.GetTableUniqueKeys(tableName, databaseName);
        List<ForeignKey> foreignKeys = catalog.GetTableForeignKeys(tableName, databaseName);
        List<IndexFile> indexFiles = catalog.GetTableIndexes(tableName, databaseName);
        List<Column> tableColumns = catalog.GetTableColumns(tableName, databaseName);

        EnsureTypedSchemaMatchesCatalog(columns, tableColumns, tableName);

        HashSet<string> columnNames = tableColumns
            .Select(column => column.Name)
            .ToHashSet(StringComparer.OrdinalIgnoreCase);

        var foreignKeysByAttribute = foreignKeys
            .GroupBy(foreignKey => foreignKey.AttributeName, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(group => group.Key, group => group.First(), StringComparer.OrdinalIgnoreCase);

        var inputRow = new Dictionary<string, object?>(row.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < row.Length; i++)
        {
            ValidateTypedCell(tableColumns[i], row[i]);
            inputRow[tableColumns[i].Name] = row[i].ToObject();
        }

        EnsureKnownColumns(inputRow, columnNames, tableName);

        var messages = new List<string>();
        var acceptedPrimaryKeys = new HashSet<string>(StringComparer.Ordinal);
        var acceptedUniqueValues = uniqueKeys.ToDictionary(
            key => key,
            _ => new HashSet<string>(StringComparer.Ordinal),
            StringComparer.OrdinalIgnoreCase);

        if (!TryNormalizeRow(
            tableName,
            databaseName,
            rowNumber: 1,
            inputRow,
            tableColumns,
            primaryKeys,
            uniqueKeys,
            foreignKeysByAttribute,
            acceptedPrimaryKeys,
            acceptedUniqueValues,
            messages,
            out Dictionary<string, object?> normalized))
        {
            string message = messages.Count == 0
                ? $"Typed insert into {tableName} was rejected."
                : string.Join(" ", messages);
            throw new InvalidOperationException(message);
        }

        List<long> rowIds = context.InsertIntoTable([normalized], tableName, databaseName);
        long rowId = rowIds[0];
        MvccCoordinator.RegisterInsertVersion(engine, databaseName, tableName, rowId, statementTxId);
        InsertIndexes(tableName, databaseName, normalized, rowId, indexFiles);

        if (recorder is not null)
        {
            var typedAfter = new TypedRow(columns, row);
            recorder.RecordTypedInsert(tableName, rowId, normalized, typedAfter);
        }

        return rowId;
    }

    private static Dictionary<string, object?> MaterializeRow(IReadOnlyDictionary<string, object?> row)
    {
        return row is Dictionary<string, object?> dict && dict.Comparer.Equals(StringComparer.OrdinalIgnoreCase)
            ? new Dictionary<string, object?>(dict, StringComparer.OrdinalIgnoreCase)
            : new Dictionary<string, object?>(row, StringComparer.OrdinalIgnoreCase);
    }

    private static void EnsureTypedSchemaMatchesCatalog(
        ReactiveRowSchema columns,
        IReadOnlyList<Column> tableColumns,
        string tableName)
    {
        if (columns.ColumnCount != tableColumns.Count)
        {
            throw new NotSupportedException(
                $"Typed inserts for table {tableName} require the full catalog row in catalog order; use the dictionary BulkInsert path for partial rows.");
        }

        for (int i = 0; i < tableColumns.Count; i++)
        {
            if (!columns.ColumnAt(i).Equals(tableColumns[i].Name, StringComparison.OrdinalIgnoreCase))
            {
                throw new NotSupportedException(
                    $"Typed inserts for table {tableName} require catalog order. Expected column {tableColumns[i].Name} at ordinal {i}; use the dictionary BulkInsert path.");
            }
        }
    }

    private static void ValidateTypedCell(Column column, CellValue cell)
    {
        CellType expected = column.Type.ToUpperInvariant() switch
        {
            "INT" => CellType.Int32,
            "VARCHAR" => CellType.String,
            "BIT" => CellType.Boolean,
            "FLOAT" or "DATE" or "VECTOR" => throw new NotSupportedException(
                $"Typed inserts do not support column {column.Name} of type {column.Type}; use the dictionary BulkInsert path."),
            _ => throw new NotSupportedException(
                $"Typed inserts do not support column {column.Name} of type {column.Type}; use the dictionary BulkInsert path.")
        };

        if (cell.IsNull)
        {
            return;
        }

        if (cell.Type != expected)
        {
            throw new NotSupportedException(
                $"Typed insert cell for column {column.Name} must be {expected} for {column.Type}; use the dictionary BulkInsert path.");
        }

        if (expected == CellType.String
            && column.Length > 0
            && cell.AsString() is string text
            && text.Length > column.Length)
        {
            throw new NotSupportedException(
                $"Typed insert cell for column {column.Name} exceeds VARCHAR({column.Length}); use the dictionary BulkInsert path.");
        }
    }

    private static void EnsureKnownColumns(
        IReadOnlyDictionary<string, object?> inputRow,
        HashSet<string> columnNames,
        string tableName)
    {
        foreach (string columnName in inputRow.Keys)
        {
            if (!columnNames.Contains(columnName))
            {
                throw new BindingException($"Column {columnName} doesn't exist in table {tableName}!");
            }
        }
    }

    private bool TryNormalizeRow(
        string tableName,
        string databaseName,
        int rowNumber,
        IReadOnlyDictionary<string, object?> inputRow,
        IReadOnlyList<Column> tableColumns,
        IReadOnlyList<string> primaryKeys,
        IReadOnlyList<string> uniqueKeys,
        IReadOnlyDictionary<string, ForeignKey> foreignKeysByAttribute,
        HashSet<string> acceptedPrimaryKeys,
        IReadOnlyDictionary<string, HashSet<string>> acceptedUniqueValues,
        List<string> messages,
        out Dictionary<string, object?> normalized)
    {
        normalized = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        foreach (Column tableColumn in tableColumns)
        {
            object? rawValue = inputRow.TryGetValue(tableColumn.Name, out object? suppliedValue)
                ? suppliedValue
                : tableColumn.DefaultValue;

            if (!TryNormalizeValue(tableColumn, rawValue, out object? normalizedValue))
            {
                messages.Add($"Type of argument doesn't match with column type in row {rowNumber}!");
                return false;
            }

            normalized[tableColumn.Name] = normalizedValue;
        }

        if (!ValidatePrimaryKeys(tableName, databaseName, rowNumber, normalized, primaryKeys, acceptedPrimaryKeys, messages))
        {
            return false;
        }

        if (!ValidateUniqueKeys(tableName, databaseName, rowNumber, normalized, uniqueKeys, acceptedUniqueValues, messages))
        {
            return false;
        }

        if (!ValidateForeignKeys(databaseName, rowNumber, normalized, foreignKeysByAttribute, messages))
        {
            return false;
        }

        return true;
    }

    private static bool TryNormalizeValue(Column column, object? rawValue, out object? normalizedValue)
    {
        normalizedValue = null;

        if (rawValue == null || rawValue is DBNull)
        {
            return true;
        }

        string formattedValue = FormatColumnValue(rawValue, column.Type);
        column.Value = formattedValue;

        object? parsedValue = column.ParsedValue;
        if (!IsParsedValueCompatible(column, parsedValue))
        {
            return false;
        }

        normalizedValue = NormalizeParsedValue(column, parsedValue);
        return true;
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

    private bool ValidatePrimaryKeys(
        string tableName,
        string databaseName,
        int rowNumber,
        Dictionary<string, object?> row,
        IReadOnlyList<string> primaryKeys,
        HashSet<string> acceptedPrimaryKeys,
        List<string> messages)
    {
        if (primaryKeys.Count == 0)
        {
            return true;
        }

        if (primaryKeys.Any(primaryKey => row[primaryKey] == null))
        {
            messages.Add($"Primary key cannot be null in row {rowNumber}!");
            return false;
        }

        string primaryKeyValue = IndexKeyEncoder.BuildKeyString(row, primaryKeys);
        if (acceptedPrimaryKeys.Contains(primaryKeyValue) || PrimaryKeyExists(tableName, databaseName, row, primaryKeys))
        {
            messages.Add($"Primary key violation in row {rowNumber}!");
            return false;
        }

        acceptedPrimaryKeys.Add(primaryKeyValue);
        return true;
    }

    private bool ValidateUniqueKeys(
        string tableName,
        string databaseName,
        int rowNumber,
        Dictionary<string, object?> row,
        IReadOnlyList<string> uniqueKeys,
        IReadOnlyDictionary<string, HashSet<string>> acceptedUniqueValues,
        List<string> messages)
    {
        foreach (string uniqueKey in uniqueKeys)
        {
            if (row[uniqueKey] == null)
            {
                continue;
            }

            string uniqueValue = IndexKeyEncoder.BuildKeyString(row, [uniqueKey]);
            HashSet<string> acceptedValues = acceptedUniqueValues[uniqueKey];

            if (acceptedValues.Contains(uniqueValue) || UniqueValueExists(tableName, databaseName, uniqueKey, row))
            {
                messages.Add($"Unique key violation in row {rowNumber}!");
                return false;
            }

            acceptedValues.Add(uniqueValue);
        }

        return true;
    }

    private bool ValidateForeignKeys(
        string databaseName,
        int rowNumber,
        Dictionary<string, object?> row,
        IReadOnlyDictionary<string, ForeignKey> foreignKeysByAttribute,
        List<string> messages)
    {
        foreach ((string columnName, ForeignKey foreignKey) in foreignKeysByAttribute)
        {
            string candidate = IndexKeyEncoder.BuildKeyString(row, [columnName]);
            if (!ReferenceExists(foreignKey, candidate, databaseName))
            {
                messages.Add($"Foreign key violation in row {rowNumber}!");
                return false;
            }
        }

        return true;
    }

    private bool PrimaryKeyExists(
        string tableName,
        string databaseName,
        Dictionary<string, object?> row,
        IReadOnlyList<string> primaryKeys)
    {
        string primaryKeyValue = IndexKeyEncoder.BuildKeyString(row, primaryKeys);

        try
        {
            if (indexes.IndexContainsKey(primaryKeyValue, $"_PK_{tableName}", tableName, databaseName))
            {
                return true;
            }
        }
        catch (IndexException)
        {
        }

        Dictionary<long, Dictionary<string, object?>> existingRows = context.GetTableContents(tableName, databaseName);
        return existingRows.Values.Any(existingRow =>
            string.Equals(IndexKeyEncoder.BuildKeyString(existingRow, primaryKeys), primaryKeyValue, StringComparison.Ordinal));
    }

    private bool UniqueValueExists(
        string tableName,
        string databaseName,
        string columnName,
        Dictionary<string, object?> row)
    {
        string uniqueValue = IndexKeyEncoder.BuildKeyString(row, [columnName]);

        try
        {
            if (indexes.IndexContainsKey(uniqueValue, $"_UK_{columnName}", tableName, databaseName))
            {
                return true;
            }
        }
        catch (IndexException)
        {
        }

        Dictionary<long, Dictionary<string, object?>> existingRows = context.GetTableContents(tableName, databaseName);
        return existingRows.Values.Any(existingRow =>
            string.Equals(IndexKeyEncoder.BuildKeyString(existingRow, [columnName]), uniqueValue, StringComparison.Ordinal));
    }

    private bool ReferenceExists(ForeignKey foreignKey, string expectedValue, string databaseName)
    {
        foreach (Reference reference in foreignKey.References)
        {
            try
            {
                if (indexes.IndexContainsKey(expectedValue, $"_PK_{reference.ReferenceTableName}", reference.ReferenceTableName, databaseName))
                {
                    continue;
                }
            }
            catch (IndexException)
            {
            }

            Dictionary<long, Dictionary<string, object?>> existingRows = context.GetTableContents(reference.ReferenceTableName, databaseName);
            if (existingRows.Values.Any(existingRow =>
                string.Equals(IndexKeyEncoder.BuildKeyString(existingRow, [reference.ReferenceAttributeName]), expectedValue, StringComparison.Ordinal)))
            {
                continue;
            }

            return false;
        }

        return true;
    }

    private void InsertIndexes(
        string tableName,
        string databaseName,
        Dictionary<string, object?> row,
        long rowId,
        IReadOnlyList<IndexFile> indexFiles)
    {
        foreach (IndexFile index in indexFiles)
        {
            if (index.AttributeNames.Any(attribute => row[attribute] == null))
            {
                continue;
            }

            string indexName = index.IndexFileName ?? string.Empty;
            if (string.IsNullOrWhiteSpace(indexName))
            {
                continue;
            }

            string indexKind = index.IndexKind ?? string.Empty;
            if (indexes.SupportsVectorIndexType(indexKind))
            {
                if (index.AttributeNames.Count != 1)
                {
                    throw new BindingException($"Vector index '{indexName}' (type '{indexKind}') must reference exactly one VECTOR column.");
                }

                string vectorColumn = index.AttributeNames[0];
                if (!VectorParser.TryCoerceToVector(row[vectorColumn], out float[] vector))
                {
                    throw new EvaluationException($"Cannot coerce value of '{vectorColumn}' into VECTOR for index '{indexName}'.");
                }

                indexes.InsertIntoVectorIndex(vector, rowId, indexName, tableName, databaseName, indexKind);
                continue;
            }

            string indexValue = IndexKeyEncoder.BuildKeyString(row, index.AttributeNames);
            indexes.InsertIntoIndex(indexValue, rowId, indexName, tableName, databaseName);
        }
    }
}

internal sealed record InsertRowsResult(
    IReadOnlyList<long> RowIds,
    int AcceptedRowCount,
    IReadOnlyList<string> Messages);
