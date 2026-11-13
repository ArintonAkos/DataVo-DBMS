using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Statements.Mechanism;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;

namespace DataVo.Core.Parser.DQL;

/// <summary>
/// Streams a plain scan of one table: rows are read in small pages and handed out one at a time, so the
/// result is never held in memory. Reuses the typed single-table path's predicates and row decoding.
/// </summary>
internal partial class Select
{
    private const int StreamPageRows = 1024;

    private sealed class StreamPage(TypedScanState scan, DataVoRowSchema schema, int[] outputOrdinals)
    {
        public TypedScanState Scan { get; } = scan;
        public DataVoRowSchema Schema { get; } = schema;
        public int[] OutputOrdinals { get; } = outputOrdinals;
        public List<DataVoRow> Rows { get; } = new(StreamPageRows);
    }

    /// <summary>Returns a lazy row stream, or null when the query must be evaluated as a whole first.</summary>
    internal IEnumerable<DataVoRow>? TryOpenStream(Guid session)
    {
        if (IsNeverStreamable(session))
        {
            return null;
        }

        try
        {
            using IDisposable principal = Engine.SessionSecurity.PushAmbientPrincipalForSession(session);
            Engine.SessionSecurity.Authorize(session, RequiredPermission, Engine.Config);
            return BuildStream(ValidateDatabase(session));
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            return null; // the general path reports the same failure in its usual form
        }
    }

    private bool IsNeverStreamable(Guid session) =>
        ast.FromTable is null
        || ast.Ctes.Count > 0
        || ast.Joins.Count > 0
        || ast.IsDistinct
        || ast.GroupByExpression is not null
        || ast.HavingExpression is not null
        || ast.OrderByExpression is { Columns.Count: > 0 }
        || Transactions.HasActiveTransaction(session);

    private IEnumerable<DataVoRow>? BuildStream(string databaseName)
    {
        string tableName = _model.FromTable.TableName;
        IReadOnlyList<Column> columns = Catalog.GetTableColumns(tableName, databaseName);
        var predicates = new List<TypedPredicate>();
        ExpressionNode? where = _model.WhereStatement.IsEvaluatable() ? _model.WhereStatement.GetExpression() : null;
        if (columns.Count == 0
            || _model.GetAggregateColumns().Count > 0
            || _model.GetComputedExpressionColumns().Count > 0
            || _model.GetWindowFunctionColumns().Count > 0
            || (where is not null && !TryCollectTypedPredicates(where, tableName, columns, predicates))
            || HasIndexableEquality(predicates, columns, tableName, databaseName))
        {
            return null;
        }

        if (StreamOutputOf(columns) is not { } output)
        {
            return null;
        }

        return ReadStream(tableName, databaseName, columns, predicates, output.Fields, output.Ordinals);
    }

    /// <summary>An equality on an indexed column reads few rows through the index, which beats scanning.</summary>
    private bool HasIndexableEquality(List<TypedPredicate> predicates, IReadOnlyList<Column> columns, string tableName, string databaseName)
    {
        List<string> primaryKeys = Catalog.GetTablePrimaryKeys(tableName, databaseName);
        Dictionary<string, string> indexed = _model.FromTable.IndexedColumns ?? [];
        return predicates.Exists(predicate =>
            predicate.Operator == Enums.Operators.EQUALS
            && (indexed.ContainsKey(columns[predicate.Ordinal].Name)
                || primaryKeys.Contains(columns[predicate.Ordinal].Name, StringComparer.OrdinalIgnoreCase)));
    }

    private (List<string> Fields, int[] Ordinals)? StreamOutputOf(IReadOnlyList<Column> columns)
    {
        if (_model.TableColumnsInUse is null)
        {
            return ([.. columns.Select(column => column.Name)], [.. Enumerable.Range(0, columns.Count)]);
        }

        if (DirectOutputOf(columns) is not { } direct)
        {
            return null;
        }

        List<string> fields = _typedOutputFields!;
        var ordinals = new int[fields.Count];
        foreach ((string field, int ordinal) in direct)
        {
            ordinals[fields.IndexOf(field)] = ordinal;
        }

        return (fields, ordinals);
    }

    private IEnumerable<DataVoRow> ReadStream(
        string tableName,
        string databaseName,
        IReadOnlyList<Column> columns,
        List<TypedPredicate> predicates,
        List<string> fields,
        int[] outputOrdinals)
    {
        ClusteredKeyLayout? clustered = Context.ResolveClusteredKey(databaseName, tableName);
        bool clusteredIsGuid = clustered is not null
            && string.Equals(columns[clustered.Ordinal].Type, "GUID", StringComparison.OrdinalIgnoreCase);
        (RowKey? lower, RowKey? upper) = clustered is not null && !clusteredIsGuid
            ? KeyRangeOf(predicates, clustered.Ordinal)
            : (null, null);

        var filterMask = new bool[columns.Count];
        predicates.ForEach(predicate => filterMask[predicate.Ordinal] = true);
        var restMask = new bool[columns.Count];
        foreach (int ordinal in outputOrdinals)
        {
            restMask[ordinal] = !filterMask[ordinal];
        }

        StreamPage NewPage() => new(
            new TypedScanState
            {
                Columns = columns,
                Predicates = predicates,
                FilterMask = filterMask,
                RestMask = restMask,
                Cells = new CellValue[columns.Count],
                ClusteredOrdinal = clustered?.Ordinal ?? -1,
                ClusteredIsGuid = clusteredIsGuid,
                TableName = tableName,
            },
            new DataVoRowSchema(fields),
            outputOrdinals);

        return EnumerateStream(NewPage, tableName, databaseName, lower, upper, _model.LimitSkip ?? 0, _model.LimitTake ?? int.MaxValue);
    }

    private IEnumerable<DataVoRow> EnumerateStream(Func<StreamPage> newPage, string tableName, string databaseName, RowKey? lower, RowKey? upper, int skip, int take)
    {
        // Each enumeration has its own decode buffers, so one stream can be enumerated more than once.
        StreamPage page = newPage();
        var cursor = new StorageContext.RowPageCursor();
        int schemaVersion = Catalog.GetTableSchemaVersion(tableName, databaseName);
        int remaining = take;
        while (remaining > 0 && !cursor.Done && !IsEmptyRange(lower, upper))
        {
            ReadNextPage(page, cursor, databaseName, lower, upper, schemaVersion);
            foreach (DataVoRow row in page.Rows)
            {
                if (skip > 0)
                {
                    skip--;
                    continue;
                }

                yield return row;
                if (--remaining == 0)
                {
                    yield break;
                }
            }
        }
    }

    private static bool IsEmptyRange(RowKey? lower, RowKey? upper) => lower is { } l && upper is { } u && l >= u;

    private void ReadNextPage(StreamPage page, StorageContext.RowPageCursor cursor, string databaseName, RowKey? lower, RowKey? upper, int schemaVersion)
    {
        using SnapshotLockScope readScope = Engine.EnterRuntimeReadScope();
        using IDisposable current = DataVoEngine.PushCurrent(Engine);
        if (Catalog.GetTableSchemaVersion(page.Scan.TableName, databaseName) != schemaVersion)
        {
            throw new InvalidOperationException($"Table {page.Scan.TableName} changed shape while it was being streamed.");
        }

        page.Rows.Clear();
        if (!Context.ScanRowPage(page.Scan.TableName, databaseName, cursor, lower, upper, StreamPageRows, ref page, VisitStreamRow))
        {
            throw new InvalidOperationException($"Table {page.Scan.TableName} cannot be streamed at this point.");
        }
    }

    private static bool VisitStreamRow(ref StreamPage page, RowKey rowId, ReadOnlySpan<byte> rowBytes, ReadOnlySpan<CellValue> typedCells)
    {
        TypedScanState state = page.Scan;
        CellValue[] cells = state.Cells;
        Load(state, rowId, rowBytes, typedCells, state.FilterMask);
        foreach (TypedPredicate predicate in state.Predicates)
        {
            if (!Matches(predicate, cells[predicate.Ordinal], state.Columns[predicate.Ordinal]))
            {
                return true;
            }
        }

        Load(state, rowId, rowBytes, typedCells, state.RestMask);
        var values = new object?[page.OutputOrdinals.Length];
        for (int i = 0; i < values.Length; i++)
        {
            int ordinal = page.OutputOrdinals[i];
            values[i] = StorageContext.ConvertCellForLegacyDictionary(state.Columns[ordinal], cells[ordinal]);
        }

        page.Rows.Add(new DataVoRow(page.Schema, values));
        return true;
    }
}
