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

        // Cached per (table, schema version): computed once instead of per batch (also benefits any
        // repeated single-row inserts hitting the same warm cache).
        TableValidationMetadata metadata = catalog.GetTableValidationMetadata(tableName, databaseName);
        IReadOnlyList<string> primaryKeys = metadata.PrimaryKeys;
        IReadOnlyList<string> uniqueKeys = metadata.UniqueKeys;
        IReadOnlyList<IndexFile> indexFiles = metadata.Indexes;
        IReadOnlyList<Column> tableColumns = metadata.Columns;
        HashSet<string> columnNames = metadata.ColumnNames;
        IReadOnlyDictionary<string, ForeignKey> foreignKeysByAttribute = metadata.ForeignKeysByAttribute;

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

        // Cached per (table, schema version): no per-row catalog re-reads or LINQ structure rebuilds.
        TableValidationMetadata metadata = catalog.GetTableValidationMetadata(tableName, databaseName);
        IReadOnlyList<Column> tableColumns = metadata.Columns;
        IReadOnlyList<string> primaryKeys = metadata.PrimaryKeys;
        IReadOnlyList<string> uniqueKeys = metadata.UniqueKeys;
        IReadOnlyList<IndexFile> indexFiles = metadata.Indexes;
        IReadOnlyDictionary<string, ForeignKey> foreignKeysByAttribute = metadata.ForeignKeysByAttribute;

        EnsureTypedSchemaMatchesCatalog(columns, tableColumns, tableName);
        CellValue[] normalizedCells = NormalizeTypedCells(tableColumns, row);

        // Constraint-free tables (no PK/UK/FK) skip the validation scaffolding entirely: no messages
        // List, no acceptedPrimaryKeys HashSet, no acceptedUniqueValues dict (Slice 5 P3, lever 6).
        bool hasConstraints = primaryKeys.Count > 0 || uniqueKeys.Count > 0 || foreignKeysByAttribute.Count > 0;
        if (hasConstraints)
        {
            var messages = new List<string>();
            var acceptedPrimaryKeys = new HashSet<string>(StringComparer.Ordinal);
            var acceptedUniqueValues = uniqueKeys.ToDictionary(
                key => key,
                _ => new HashSet<string>(StringComparer.Ordinal),
                StringComparer.OrdinalIgnoreCase);

            if (!ValidateTypedPrimaryKeys(
                tableName,
                databaseName,
                rowNumber: 1,
                normalizedCells,
                columns,
                primaryKeys,
                acceptedPrimaryKeys,
                messages)
                || !ValidateTypedUniqueKeys(
                    tableName,
                    databaseName,
                    rowNumber: 1,
                    normalizedCells,
                    columns,
                    uniqueKeys,
                    acceptedUniqueValues,
                    messages)
                || !ValidateTypedForeignKeys(
                    databaseName,
                    rowNumber: 1,
                    normalizedCells,
                    columns,
                    foreignKeysByAttribute,
                    messages))
            {
                string message = messages.Count == 0
                    ? $"Typed insert into {tableName} was rejected."
                    : string.Join(" ", messages);
                throw new InvalidOperationException(message);
            }
        }

        // normalizedCells is freshly allocated by NormalizeTypedCells and is not mutated after this point;
        // later uses in this method must remain read-only after handing ownership to StoredRow.
        var storedRow = StoredRow.FromOwnedCells(columns, normalizedCells);
        long rowId = context.InsertTypedRow(storedRow, tableColumns, tableName, databaseName);
        MvccCoordinator.RegisterInsertVersion(engine, databaseName, tableName, rowId, statementTxId);
        InsertTypedIndexes(tableName, databaseName, columns, normalizedCells, rowId, indexFiles);

        if (recorder is not null)
        {
            // Share the StoredRow's owned, immutable cells with the captured after-image (no clone, no
            // eager dict); the owned dict is materialized lazily only if an owned subscriber reads it
            // (Slice 5 P3, lever 4). normalizedCells is never mutated after this point.
            recorder.RecordTypedInsert(tableName, rowId, TypedRow.FromOwnedCells(columns, normalizedCells));
        }

        return rowId;
    }

    public IReadOnlyList<long> InsertTypedRows(
        string databaseName,
        string tableName,
        ReactiveRowSchema columns,
        IReadOnlyList<CellValue[]> rows,
        long statementTxId,
        ChangeRecorder? recorder = null)
    {
        if (string.IsNullOrWhiteSpace(tableName))
        {
            throw new ArgumentException("Table name is required.", nameof(tableName));
        }

        if (rows.Count == 0)
        {
            return [];
        }

        TableValidationMetadata metadata = catalog.GetTableValidationMetadata(tableName, databaseName);
        IReadOnlyList<Column> tableColumns = metadata.Columns;
        IReadOnlyList<string> primaryKeys = metadata.PrimaryKeys;
        IReadOnlyList<string> uniqueKeys = metadata.UniqueKeys;
        IReadOnlyList<IndexFile> indexFiles = metadata.Indexes;
        IReadOnlyDictionary<string, ForeignKey> foreignKeysByAttribute = metadata.ForeignKeysByAttribute;

        EnsureTypedSchemaMatchesCatalog(columns, tableColumns, tableName);

        bool hasIntegerPrimaryKey = TryResolveIntegerPrimaryKeyOrdinal(primaryKeys, columns, out int integerPrimaryKeyOrdinal);
        bool hasConstraints = primaryKeys.Count > 0 || uniqueKeys.Count > 0 || foreignKeysByAttribute.Count > 0;
        List<string>? messages = hasConstraints ? [] : null;
        HashSet<string>? acceptedPrimaryKeys = hasConstraints && !hasIntegerPrimaryKey ? new HashSet<string>(StringComparer.Ordinal) : null;
        HashSet<long>? acceptedIntegerPrimaryKeys = hasIntegerPrimaryKey ? [] : null;
        IReadOnlyDictionary<string, HashSet<string>>? acceptedUniqueValues = hasConstraints
            ? uniqueKeys.ToDictionary(
                key => key,
                _ => new HashSet<string>(StringComparer.Ordinal),
                StringComparer.OrdinalIgnoreCase)
            : null;
        bool skipExistingPrimaryKeyLookup = primaryKeys.Count > 0 && context.IsTableEmpty(tableName, databaseName);

        string[]? primaryKeyValues = primaryKeys.Count > 0 && !hasIntegerPrimaryKey ? new string[rows.Count] : null;
        long[]? integerPrimaryKeyValues = hasIntegerPrimaryKey ? new long[rows.Count] : null;
        for (int i = 0; i < rows.Count; i++)
        {
            CellValue[] row = rows[i];
            if (columns.ColumnCount != row.Length)
            {
                throw new ArgumentException(
                    $"Typed row schema has {columns.ColumnCount} columns but row {i + 1} has {row.Length} cells.",
                    nameof(rows));
            }

            ValidateTypedCells(tableColumns, row);

            if (!hasConstraints)
            {
                continue;
            }

            int rowNumber = i + 1;
            string? primaryKeyValue = null;
            bool primaryKeyValid = hasIntegerPrimaryKey
                ? ValidateTypedIntegerPrimaryKey(
                    tableName,
                    databaseName,
                    rowNumber,
                    row,
                    integerPrimaryKeyOrdinal,
                    acceptedIntegerPrimaryKeys!,
                    messages!,
                    skipExistingPrimaryKeyLookup,
                    out integerPrimaryKeyValues![i])
                : ValidateTypedPrimaryKeys(
                    tableName,
                    databaseName,
                    rowNumber,
                    row,
                    columns,
                    primaryKeys,
                    acceptedPrimaryKeys!,
                    messages!,
                    skipExistingPrimaryKeyLookup,
                    out primaryKeyValue);

            if (!primaryKeyValid
                || !ValidateTypedUniqueKeys(
                    tableName,
                    databaseName,
                    rowNumber,
                    row,
                    columns,
                    uniqueKeys,
                    acceptedUniqueValues!,
                    messages!)
                || !ValidateTypedForeignKeys(
                    databaseName,
                    rowNumber,
                    row,
                    columns,
                    foreignKeysByAttribute,
                    messages!))
            {
                string message = messages!.Count == 0
                    ? $"Typed insert into {tableName} was rejected."
                    : string.Join(" ", messages);
                throw new InvalidOperationException(message);
            }

            if (primaryKeyValues is not null)
            {
                primaryKeyValues[i] = primaryKeyValue!;
            }
        }

        var storedRows = new List<StoredRow>(rows.Count);
        for (int i = 0; i < rows.Count; i++)
        {
            storedRows.Add(StoredRow.FromOwnedCells(columns, rows[i]));
        }

        IReadOnlyList<long> rowIds = context.InsertTypedRows(storedRows, tableName, databaseName);
        MvccCoordinator.RegisterInsertVersions(engine, databaseName, tableName, rowIds, statementTxId);
        InsertTypedIndexesBatch(
            tableName,
            databaseName,
            columns,
            rows,
            rowIds,
            indexFiles,
            primaryKeys,
            primaryKeyValues,
            integerPrimaryKeyValues);

        if (recorder is not null)
        {
            for (int i = 0; i < rowIds.Count; i++)
            {
                recorder.RecordTypedInsert(tableName, rowIds[i], TypedRow.FromOwnedCells(columns, rows[i]));
            }
        }

        return rowIds;
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

    private static CellValue[] NormalizeTypedCells(IReadOnlyList<Column> tableColumns, ReadOnlySpan<CellValue> row)
    {
        var normalized = new CellValue[row.Length];
        for (int i = 0; i < row.Length; i++)
        {
            ValidateTypedCell(tableColumns[i], row[i]);
            normalized[i] = row[i];
        }

        return normalized;
    }

    private static void ValidateTypedCells(IReadOnlyList<Column> tableColumns, ReadOnlySpan<CellValue> row)
    {
        for (int i = 0; i < row.Length; i++)
        {
            ValidateTypedCell(tableColumns[i], row[i]);
        }
    }

    private static bool TryResolveIntegerPrimaryKeyOrdinal(
        IReadOnlyList<string> primaryKeys,
        ReactiveRowSchema schema,
        out int ordinal)
    {
        ordinal = -1;
        return primaryKeys.Count == 1 && schema.TryGetOrdinal(primaryKeys[0], out ordinal);
    }

    private static void ValidateTypedCell(Column column, CellValue cell)
    {
        CellType expected = ExpectedCellType(column);

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

        if (expected == CellType.Vector
            && column.Length > 0
            && cell.VectorLength != column.Length)
        {
            throw new NotSupportedException(
                $"Typed insert cell for column {column.Name} must have VECTOR({column.Length}) dimensions; use the dictionary BulkInsert path.");
        }
    }

    private static CellType ExpectedCellType(Column column)
    {
        string type = column.Type;
        if (type.Equals("INT", StringComparison.OrdinalIgnoreCase))
        {
            return CellType.Int32;
        }

        if (type.Equals("FLOAT", StringComparison.OrdinalIgnoreCase))
        {
            return CellType.Double;
        }

        if (type.Equals("BIT", StringComparison.OrdinalIgnoreCase))
        {
            return CellType.Boolean;
        }

        if (type.Equals("DATE", StringComparison.OrdinalIgnoreCase))
        {
            return CellType.Date;
        }

        if (type.Equals("VARCHAR", StringComparison.OrdinalIgnoreCase))
        {
            return CellType.String;
        }

        if (type.Equals("VECTOR", StringComparison.OrdinalIgnoreCase))
        {
            return CellType.Vector;
        }

        throw new NotSupportedException(
            $"Typed inserts do not support column {column.Name} of type {column.Type}; use the dictionary BulkInsert path.");
    }

    private bool ValidateTypedPrimaryKeys(
        string tableName,
        string databaseName,
        int rowNumber,
        ReadOnlySpan<CellValue> cells,
        ReactiveRowSchema schema,
        IReadOnlyList<string> primaryKeys,
        HashSet<string> acceptedPrimaryKeys,
        List<string> messages)
    {
        return ValidateTypedPrimaryKeys(
            tableName,
            databaseName,
            rowNumber,
            cells,
            schema,
            primaryKeys,
            acceptedPrimaryKeys,
            messages,
            out _);
    }

    private bool ValidateTypedPrimaryKeys(
        string tableName,
        string databaseName,
        int rowNumber,
        ReadOnlySpan<CellValue> cells,
        ReactiveRowSchema schema,
        IReadOnlyList<string> primaryKeys,
        HashSet<string> acceptedPrimaryKeys,
        List<string> messages,
        out string? primaryKeyValue)
    {
        return ValidateTypedPrimaryKeys(
            tableName,
            databaseName,
            rowNumber,
            cells,
            schema,
            primaryKeys,
            acceptedPrimaryKeys,
            messages,
            skipExistingPrimaryKeyLookup: false,
            out primaryKeyValue);
    }

    private bool ValidateTypedPrimaryKeys(
        string tableName,
        string databaseName,
        int rowNumber,
        ReadOnlySpan<CellValue> cells,
        ReactiveRowSchema schema,
        IReadOnlyList<string> primaryKeys,
        HashSet<string> acceptedPrimaryKeys,
        List<string> messages,
        bool skipExistingPrimaryKeyLookup,
        out string? primaryKeyValue)
    {
        primaryKeyValue = null;
        if (primaryKeys.Count == 0)
        {
            return true;
        }

        foreach (string primaryKey in primaryKeys)
        {
            if (!GetTypedCell(cells, schema, primaryKey).IsNull)
            {
                continue;
            }

            messages.Add($"Primary key cannot be null in row {rowNumber}!");
            return false;
        }

        primaryKeyValue = IndexKeyEncoder.BuildKeyString(schema, cells, primaryKeys);
        if (acceptedPrimaryKeys.Contains(primaryKeyValue)
            || (!skipExistingPrimaryKeyLookup && PrimaryKeyExists(tableName, databaseName, primaryKeyValue, primaryKeys)))
        {
            messages.Add($"Primary key violation in row {rowNumber}!");
            return false;
        }

        acceptedPrimaryKeys.Add(primaryKeyValue);
        return true;
    }

    private bool ValidateTypedIntegerPrimaryKey(
        string tableName,
        string databaseName,
        int rowNumber,
        ReadOnlySpan<CellValue> cells,
        int primaryKeyOrdinal,
        HashSet<long> acceptedPrimaryKeys,
        List<string> messages,
        bool skipExistingPrimaryKeyLookup,
        out long primaryKeyValue)
    {
        CellValue cell = cells[primaryKeyOrdinal];
        primaryKeyValue = 0;

        if (cell.IsNull)
        {
            messages.Add($"Primary key cannot be null in row {rowNumber}!");
            return false;
        }

        primaryKeyValue = cell.Type switch
        {
            CellType.Int32 => cell.AsInt32(),
            CellType.Int64 => cell.AsInt64(),
            _ => throw new NotSupportedException(
                $"Typed integer primary key for row {rowNumber} must be INT or BIGINT; use the dictionary BulkInsert path.")
        };

        if (acceptedPrimaryKeys.Contains(primaryKeyValue)
            || (!skipExistingPrimaryKeyLookup && IntegerPrimaryKeyExists(tableName, databaseName, primaryKeyValue)))
        {
            messages.Add($"Primary key violation in row {rowNumber}!");
            return false;
        }

        acceptedPrimaryKeys.Add(primaryKeyValue);
        return true;
    }

    private bool ValidateTypedUniqueKeys(
        string tableName,
        string databaseName,
        int rowNumber,
        ReadOnlySpan<CellValue> cells,
        ReactiveRowSchema schema,
        IReadOnlyList<string> uniqueKeys,
        IReadOnlyDictionary<string, HashSet<string>> acceptedUniqueValues,
        List<string> messages)
    {
        foreach (string uniqueKey in uniqueKeys)
        {
            if (GetTypedCell(cells, schema, uniqueKey).IsNull)
            {
                continue;
            }

            string uniqueValue = IndexKeyEncoder.BuildKeyString(schema, cells, [uniqueKey]);
            HashSet<string> acceptedValues = acceptedUniqueValues[uniqueKey];

            if (acceptedValues.Contains(uniqueValue)
                || UniqueValueExists(tableName, databaseName, uniqueKey, uniqueValue))
            {
                messages.Add($"Unique key violation in row {rowNumber}!");
                return false;
            }

            acceptedValues.Add(uniqueValue);
        }

        return true;
    }

    private bool ValidateTypedForeignKeys(
        string databaseName,
        int rowNumber,
        ReadOnlySpan<CellValue> cells,
        ReactiveRowSchema schema,
        IReadOnlyDictionary<string, ForeignKey> foreignKeysByAttribute,
        List<string> messages)
    {
        foreach ((string columnName, ForeignKey foreignKey) in foreignKeysByAttribute)
        {
            string candidate = IndexKeyEncoder.BuildKeyString(schema, cells, [columnName]);
            if (!ReferenceExists(foreignKey, candidate, databaseName))
            {
                messages.Add($"Foreign key violation in row {rowNumber}!");
                return false;
            }
        }

        return true;
    }

    private static CellValue GetTypedCell(ReadOnlySpan<CellValue> cells, ReactiveRowSchema schema, string columnName)
    {
        if (!schema.TryGetOrdinal(columnName, out int ordinal))
        {
            throw new BindingException($"Column {columnName} doesn't exist in typed row schema!");
        }

        return cells[ordinal];
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
        return PrimaryKeyExists(tableName, databaseName, primaryKeyValue, primaryKeys);
    }

    private bool PrimaryKeyExists(
        string tableName,
        string databaseName,
        string primaryKeyValue,
        IReadOnlyList<string> primaryKeys)
    {
        // The _PK_ index is created with the table and maintained on every insert (it also backs keyed
        // SELECTs), so it is authoritative: trust both a hit and a miss. Only when the index is genuinely
        // unavailable (IndexException) do we fall back to a full-table scan. Trusting the miss keeps keyed
        // bulk inserts at O(log n) per row instead of materializing+scanning the whole table each time.
        try
        {
            return indexes.IndexContainsKey(primaryKeyValue, $"_PK_{tableName}", tableName, databaseName);
        }
        catch (IndexException)
        {
        }

        Dictionary<long, Dictionary<string, object?>> existingRows = context.GetTableContents(tableName, databaseName);
        return existingRows.Values.Any(existingRow =>
            string.Equals(IndexKeyEncoder.BuildKeyString(existingRow, primaryKeys), primaryKeyValue, StringComparison.Ordinal));
    }

    private bool IntegerPrimaryKeyExists(string tableName, string databaseName, long primaryKeyValue)
    {
        string indexName = $"_PK_{tableName}";
        if (indexes.TryLookupIntegerPrimaryKey(primaryKeyValue, indexName, tableName, databaseName, out _))
        {
            return true;
        }

        try
        {
            return indexes.IndexContainsKey($"[{primaryKeyValue}]", indexName, tableName, databaseName);
        }
        catch (IndexException)
        {
            return false;
        }
    }

    private bool UniqueValueExists(
        string tableName,
        string databaseName,
        string columnName,
        Dictionary<string, object?> row)
    {
        string uniqueValue = IndexKeyEncoder.BuildKeyString(row, [columnName]);
        return UniqueValueExists(tableName, databaseName, columnName, uniqueValue);
    }

    private bool UniqueValueExists(
        string tableName,
        string databaseName,
        string columnName,
        string uniqueValue)
    {
        // The _UK_ index is created with the table and maintained on every insert, so it is authoritative:
        // trust hit and miss alike; only scan when the index is genuinely unavailable (IndexException).
        try
        {
            return indexes.IndexContainsKey(uniqueValue, $"_UK_{columnName}", tableName, databaseName);
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
                existingRow.TryGetValue(reference.ReferenceAttributeName, out object? value)
                && value != null
                && string.Equals(value.ToString(), expectedValue, StringComparison.Ordinal)))
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

    private void InsertTypedIndexes(
        string tableName,
        string databaseName,
        ReactiveRowSchema schema,
        ReadOnlySpan<CellValue> cells,
        long rowId,
        IReadOnlyList<IndexFile> indexFiles)
    {
        foreach (IndexFile index in indexFiles)
        {
            bool hasNullKeyPart = false;
            foreach (string attribute in index.AttributeNames)
            {
                if (GetTypedCell(cells, schema, attribute).IsNull)
                {
                    hasNullKeyPart = true;
                    break;
                }
            }

            if (hasNullKeyPart)
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
                CellValue vectorCell = GetTypedCell(cells, schema, vectorColumn);
                if (vectorCell.Type != CellType.Vector)
                {
                    throw new EvaluationException($"Cannot coerce value of '{vectorColumn}' into VECTOR for index '{indexName}'.");
                }

                indexes.InsertIntoVectorIndex(vectorCell.AsVectorReadOnlySpan(), rowId, indexName, tableName, databaseName, indexKind);
                continue;
            }

            if (IsPrimaryKeyIndex(tableName, indexName)
                && TryGetSingleIntegerKey(schema, cells, index.AttributeNames, out long integerKey))
            {
                indexes.InsertIntegerPrimaryKeys([(integerKey, rowId)], indexName, tableName, databaseName);
                continue;
            }

            if (TryGetSingleIntegerKey(schema, cells, index.AttributeNames, out long integerIndexKey))
            {
                indexes.InsertIntegerIndexEntries([(integerIndexKey, rowId)], indexName, tableName, databaseName);
                continue;
            }

            string indexValue = IndexKeyEncoder.BuildKeyString(schema, cells, index.AttributeNames);
            indexes.InsertIntoIndex(indexValue, rowId, indexName, tableName, databaseName);
        }
    }

    private void InsertTypedIndexesBatch(
        string tableName,
        string databaseName,
        ReactiveRowSchema schema,
        IReadOnlyList<CellValue[]> rows,
        IReadOnlyList<long> rowIds,
        IReadOnlyList<IndexFile> indexFiles,
        IReadOnlyList<string> primaryKeys,
        string[]? primaryKeyValues,
        long[]? integerPrimaryKeyValues)
    {
        foreach (IndexFile index in indexFiles)
        {
            string indexName = index.IndexFileName ?? string.Empty;
            if (string.IsNullOrWhiteSpace(indexName))
            {
                continue;
            }

            string indexKind = index.IndexKind ?? string.Empty;
            if (indexes.SupportsVectorIndexType(indexKind))
            {
                InsertTypedVectorIndexBatch(tableName, databaseName, schema, rows, rowIds, index, indexName, indexKind);
                continue;
            }

            var entries = new List<(string Value, long RowId)>(rowIds.Count);
            bool canReusePrimaryKey = primaryKeyValues is not null
                && IsPrimaryKeyIndex(tableName, indexName)
                && AttributeNamesEqual(index.AttributeNames, primaryKeys);
            bool canUseIntegerPrimaryKey = integerPrimaryKeyValues is not null
                && IsPrimaryKeyIndex(tableName, indexName)
                && AttributeNamesEqual(index.AttributeNames, primaryKeys);

            if (canUseIntegerPrimaryKey)
            {
                var integerEntries = new List<(long Key, long RowId)>(rowIds.Count);
                for (int i = 0; i < rowIds.Count; i++)
                {
                    integerEntries.Add((integerPrimaryKeyValues![i], rowIds[i]));
                }

                indexes.InsertIntegerPrimaryKeys(integerEntries, indexName, tableName, databaseName);
                continue;
            }

            if (TryBuildIntegerIndexEntries(schema, rows, rowIds, index.AttributeNames, out List<(long Key, long RowId)>? integerIndexEntries)
                && integerIndexEntries is not null)
            {
                indexes.InsertIntegerIndexEntries(integerIndexEntries, indexName, tableName, databaseName);
                continue;
            }

            for (int i = 0; i < rows.Count; i++)
            {
                ReadOnlySpan<CellValue> cells = rows[i];
                if (!canReusePrimaryKey && HasNullKeyPart(cells, schema, index.AttributeNames))
                {
                    continue;
                }

                string indexValue = canReusePrimaryKey
                    ? primaryKeyValues![i]
                    : IndexKeyEncoder.BuildKeyString(schema, cells, index.AttributeNames);
                entries.Add((indexValue, rowIds[i]));
            }

            indexes.InsertManyIntoIndex(entries, indexName, tableName, databaseName);
        }
    }

    private static bool TryBuildIntegerIndexEntries(
        ReactiveRowSchema schema,
        IReadOnlyList<CellValue[]> rows,
        IReadOnlyList<long> rowIds,
        IReadOnlyList<string> attributes,
        out List<(long Key, long RowId)>? entries)
    {
        entries = null;
        if (attributes.Count != 1 || !schema.TryGetOrdinal(attributes[0], out int ordinal))
        {
            return false;
        }

        var result = new List<(long Key, long RowId)>(rowIds.Count);
        for (int i = 0; i < rows.Count; i++)
        {
            CellValue cell = rows[i][ordinal];
            if (cell.IsNull)
            {
                continue;
            }

            if (cell.Type == CellType.Int32)
            {
                result.Add((cell.AsInt32(), rowIds[i]));
                continue;
            }

            if (cell.Type == CellType.Int64)
            {
                result.Add((cell.AsInt64(), rowIds[i]));
                continue;
            }

            return false;
        }

        entries = result;
        return true;
    }

    private void InsertTypedVectorIndexBatch(
        string tableName,
        string databaseName,
        ReactiveRowSchema schema,
        IReadOnlyList<CellValue[]> rows,
        IReadOnlyList<long> rowIds,
        IndexFile index,
        string indexName,
        string indexKind)
    {
        if (index.AttributeNames.Count != 1)
        {
            throw new BindingException($"Vector index '{indexName}' (type '{indexKind}') must reference exactly one VECTOR column.");
        }

        string vectorColumn = index.AttributeNames[0];
        for (int i = 0; i < rows.Count; i++)
        {
            CellValue vectorCell = GetTypedCell(rows[i], schema, vectorColumn);
            if (vectorCell.IsNull)
            {
                continue;
            }

            if (vectorCell.Type != CellType.Vector)
            {
                throw new EvaluationException($"Cannot coerce value of '{vectorColumn}' into VECTOR for index '{indexName}'.");
            }

            indexes.InsertIntoVectorIndex(vectorCell.AsVectorReadOnlySpan(), rowIds[i], indexName, tableName, databaseName, indexKind);
        }
    }

    private static bool HasNullKeyPart(
        ReadOnlySpan<CellValue> cells,
        ReactiveRowSchema schema,
        IReadOnlyList<string> attributes)
    {
        foreach (string attribute in attributes)
        {
            if (GetTypedCell(cells, schema, attribute).IsNull)
            {
                return true;
            }
        }

        return false;
    }

    private static bool TryGetSingleIntegerKey(
        ReactiveRowSchema schema,
        ReadOnlySpan<CellValue> cells,
        IReadOnlyList<string> attributes,
        out long key)
    {
        key = 0;
        if (attributes.Count != 1 || !schema.TryGetOrdinal(attributes[0], out int ordinal))
        {
            return false;
        }

        CellValue cell = cells[ordinal];
        if (cell.IsNull)
        {
            return false;
        }

        if (cell.Type == CellType.Int32)
        {
            key = cell.AsInt32();
            return true;
        }

        if (cell.Type == CellType.Int64)
        {
            key = cell.AsInt64();
            return true;
        }

        return false;
    }

    private static bool IsPrimaryKeyIndex(string tableName, string indexName) =>
        indexName.Equals($"_PK_{tableName}", StringComparison.OrdinalIgnoreCase);

    private static bool AttributeNamesEqual(IReadOnlyList<string> left, IReadOnlyList<string> right)
    {
        if (left.Count != right.Count)
        {
            return false;
        }

        for (int i = 0; i < left.Count; i++)
        {
            if (!left[i].Equals(right[i], StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        return true;
    }
}

internal sealed record InsertRowsResult(
    IReadOnlyList<long> RowIds,
    int AcceptedRowCount,
    IReadOnlyList<string> Messages);
