using System.Runtime.CompilerServices;
using DataVo.Core.BTree;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.MVCC;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.Runtime.Security;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Serialization;
using DataVo.Core.Transactions;

namespace DataVo.Core.CompiledQueries;

/// <summary>
/// Prepared typed point lookup for a <see cref="DataVoCompiledQueryPlan"/>. Projection metadata and the indexed
/// access path are resolved once; warm indexed hits decode raw row bytes straight into a reused typed buffer.
/// </summary>
public sealed class DataVoPreparedSelectSingle<T>
{
    private readonly DataVoContext _context;
    private readonly DataVoCompiledQueryPlan _plan;
    private readonly string _databaseName;
    private readonly string? _indexName;
    private readonly PreparedProjection _projection;
    private readonly CompiledRowMapper<T> _mapper;

    internal DataVoPreparedSelectSingle(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        string databaseName,
        string? indexName,
        PreparedProjection projection,
        CompiledRowMapper<T> mapper)
    {
        _context = context;
        _plan = plan;
        _databaseName = databaseName;
        _indexName = indexName;
        _projection = projection;
        _mapper = mapper;
    }

    /// <summary>Executes the prepared lookup for an INT predicate value.</summary>
    public T? Execute(int value) => ExecuteIntegerKey(value);

    /// <summary>Executes the prepared lookup for a BIGINT predicate value.</summary>
    public T? Execute(long value) => ExecuteIntegerKey(value);

    /// <summary>Executes the prepared lookup for a FLOAT predicate value.</summary>
    public T? Execute(double value) => ExecuteKey(DataVoCompiledQuery.BuildScalarComparisonKey(value));

    /// <summary>Executes the prepared lookup for a text predicate value.</summary>
    public T? Execute(string? value) => ExecuteKey(DataVoCompiledQuery.BuildScalarComparisonKey(value));

    /// <summary>Executes the prepared lookup for a GUID predicate value.</summary>
    public T? Execute(Guid value) => ExecuteGuidKey(value);

    /// <summary>Executes the prepared lookup for a general predicate value.</summary>
    public T? Execute(object? value) => ExecuteParameterValue(value);

    internal T? ExecuteParameterValue(object? value)
    {
        return value switch
        {
            int intValue => Execute(intValue),
            long longValue => Execute(longValue),
            double doubleValue => Execute(doubleValue),
            string stringValue => Execute(stringValue),
            Guid guidValue => Execute(guidValue),
            null => Execute((string?)null),
            _ => ExecuteKey(DataVoCompiledQuery.BuildScalarComparisonKey(value))
        };
    }

    /// <summary>How often a point read follows its key to a row that a concurrent update moved before giving up.</summary>
    private const int MaxMovedRowRetries = 16;

    private T? ExecuteIntegerKey(long value)
    {
        _context.Engine.AuthorizeDirectAccess(_context.SessionId, DatabasePermission.ReadData);
        if (HasActiveTransactionOrSnapshot(out TransactionContext? transactionContext))
        {
            return ExecuteTransactionalKey(DataVoCompiledQuery.BuildScalarComparisonKey(value), transactionContext);
        }

        if (_indexName is not null
            && _context.Engine.IndexManager.TryLookupIntegerPrimaryKey(
                value,
                _indexName,
                _plan.TableName,
                _databaseName,
                out RowKey rowId))
        {
            // Read without a lock: an update that moves the row re-points the key before it deletes the old row,
            // so a row found gone is looked up again rather than reported missing.
            for (int attempt = 0; ; attempt++)
            {
                if (TryProjectRow(rowId, out T? result))
                {
                    return result;
                }

                if (attempt == MaxMovedRowRetries
                    || !_context.Engine.IndexManager.TryLookupIntegerPrimaryKey(value, _indexName, _plan.TableName, _databaseName, out RowKey current)
                    || current == rowId)
                {
                    return default;
                }

                rowId = current;
            }
        }

        if (value is >= int.MinValue and <= int.MaxValue)
        {
            Dictionary<RowKey, StoredRow> rows = _context.Engine.StorageContext.GetTypedTableContents(_plan.TableName, _databaseName);
            if (CompiledIntegerScan.TryScanSingle(rows, _plan.WhereColumn!, (int)value, _mapper, out T? scanResult))
            {
                return scanResult;
            }
        }

        return ExecuteKey(DataVoCompiledQuery.BuildScalarComparisonKey(value));
    }

    private T? ExecuteGuidKey(Guid value)
    {
        _context.Engine.AuthorizeDirectAccess(_context.SessionId, DatabasePermission.ReadData);
        if (HasActiveTransactionOrSnapshot(out TransactionContext? transactionContext))
        {
            return ExecuteTransactionalKey(DataVoCompiledQuery.BuildScalarComparisonKey(value), transactionContext);
        }

        if (_indexName is not null
            && _context.Engine.IndexManager.TryLookupGuidPrimaryKey(
                value,
                _indexName,
                _plan.TableName,
                _databaseName,
                out RowKey rowId))
        {
            // See ExecuteIntegerKey: a row found gone may have moved under the key.
            for (int attempt = 0; ; attempt++)
            {
                if (TryProjectRow(rowId, out T? result))
                {
                    return result;
                }

                if (attempt == MaxMovedRowRetries
                    || !_context.Engine.IndexManager.TryLookupGuidPrimaryKey(value, _indexName, _plan.TableName, _databaseName, out RowKey current)
                    || current == rowId)
                {
                    return default;
                }

                rowId = current;
            }
        }

        return ExecuteKey(DataVoCompiledQuery.BuildScalarComparisonKey(value));
    }

    private T? ExecuteKey(string expectedKey)
    {
        _context.Engine.AuthorizeDirectAccess(_context.SessionId, DatabasePermission.ReadData);
        if (HasActiveTransactionOrSnapshot(out TransactionContext? transactionContext))
        {
            return ExecuteTransactionalKey(expectedKey, transactionContext);
        }

        if (_indexName is not null)
        {
            try
            {
                IReadOnlyList<RowKey> rowIds = _context.Engine.IndexManager.FilterUsingIndex(
                    expectedKey,
                    _indexName,
                    _plan.TableName,
                    _databaseName);

                for (int i = 0; i < rowIds.Count; i++)
                {
                    if (TryProjectRow(rowIds[i], out T? result))
                    {
                        return result;
                    }
                }
            }
            catch (IndexException)
            {
            }
        }

        return ExecuteScanFallback(expectedKey);
    }

    private bool HasActiveTransactionOrSnapshot(out TransactionContext? transactionContext)
    {
        TransactionManager txManager = _context.Engine.TransactionManager;
        transactionContext = txManager.HasAnyActiveTransaction()
            ? txManager.GetContext(_context.SessionId) ?? MvccExecutionScope.CurrentTransaction
            : MvccExecutionScope.CurrentTransaction;
        return transactionContext is not null || MvccExecutionScope.CurrentSnapshot is not null;
    }

    private T? ExecuteTransactionalKey(string expectedKey, TransactionContext? transactionContext)
    {
        using IDisposable engineScope = DataVoEngine.PushCurrent(_context.Engine);
        using IDisposable? txScope = transactionContext is not null
            && !ReferenceEquals(MvccExecutionScope.CurrentTransaction, transactionContext)
            ? MvccExecutionScope.PushTransaction(transactionContext)
            : null;

        if (_indexName is not null)
        {
            try
            {
                List<RowKey> rowIds = MvccCoordinator.FilterUsingIndex(
                    _context.Engine,
                    _databaseName,
                    _plan.TableName,
                    _indexName,
                    _plan.WhereColumn!,
                    expectedKey);

                if (rowIds.Count == 0)
                {
                    return default;
                }

                Dictionary<RowKey, StoredRow> rows =
                    _context.Engine.StorageContext.GetTypedTableContents(rowIds, _plan.TableName, _databaseName);
                for (int i = 0; i < rowIds.Count; i++)
                {
                    if (rows.TryGetValue(rowIds[i], out StoredRow? row))
                    {
                        return _projection.MapStoredRow(row, _mapper);
                    }
                }

                return default;
            }
            catch (IndexException)
            {
            }
        }

        return ExecuteScanFallback(expectedKey);
    }

    private bool TryProjectRow(RowKey rowId, out T? result) =>
        _projection.TryProjectRow(_context.Engine.StorageContext, _plan.TableName, _databaseName, rowId, _mapper, out result);

    private T? ExecuteScanFallback(string expectedKey)
    {
        Dictionary<RowKey, StoredRow> rows = _context.Engine.StorageContext.GetTypedTableContents(_plan.TableName, _databaseName);
        string[] whereColumns = [_plan.WhereColumn!];
        foreach ((_, StoredRow row) in rows)
        {
            StoredRowView view = row.AsView();
            if (!view.Schema.TryGetOrdinal(_plan.WhereColumn!, out _))
            {
                continue;
            }

            if (!string.Equals(
                    IndexKeyEncoder.BuildKeyString(view.Schema, view.Cells, whereColumns),
                    expectedKey,
                    StringComparison.Ordinal))
            {
                continue;
            }

            return _mapper(new CompiledRowReader(view));
        }

        return default;
    }
}

internal sealed class PreparedProjection
{
    public PreparedProjection(
        IReadOnlyList<Column> columns,
        bool[] isProjected,
        ReactiveRowSchema projectedSchema,
        CellValue[] buffer)
    {
        Columns = columns;
        IsProjected = isProjected;
        ProjectedSchema = projectedSchema;
        Buffer = buffer;
    }

    public IReadOnlyList<Column> Columns { get; }

    public bool[] IsProjected { get; }

    public ReactiveRowSchema ProjectedSchema { get; }

    public CellValue[] Buffer { get; }

    /// <summary>
    /// Reads and projects a visible row by identifier into <see cref="Buffer"/>, invoking <paramref name="mapper"/>
    /// on the projected view without materializing an intermediate dictionary.
    /// </summary>
    /// <typeparam name="T">The mapped result type.</typeparam>
    /// <param name="storage">The storage context used to verify visibility and read the row.</param>
    /// <param name="tableName">The target table name.</param>
    /// <param name="databaseName">The owning database name.</param>
    /// <param name="rowId">The row key to project.</param>
    /// <param name="mapper">The delegate that materializes <typeparamref name="T"/> from the projected reader.</param>
    /// <param name="result">When this method returns <see langword="true"/>, the mapped row value.</param>
    /// <returns><see langword="true"/> if the row is visible and projected; otherwise <see langword="false"/>.</returns>
    public bool TryProjectRow<T>(
        StorageContext storage,
        string tableName,
        string databaseName,
        RowKey rowId,
        CompiledRowMapper<T> mapper,
        out T? result)
    {
        result = default;
        if (!storage.IsRowVisible(tableName, databaseName, rowId))
        {
            return false;
        }

        byte[]? bytes = storage.TryReadRowBytes(tableName, databaseName, rowId);
        if (bytes is not null)
        {
            lock (Buffer)
            {
                RowSerializer.DecodeProjectedCells(bytes, Columns, IsProjected, Buffer);
                result = mapper(new CompiledRowReader(new StoredRowView(ProjectedSchema, Buffer)));
            }

            return true;
        }

        lock (Buffer)
        {
            if (TryFillFromTypedRow(storage, tableName, databaseName, rowId))
            {
                result = mapper(new CompiledRowReader(new StoredRowView(ProjectedSchema, Buffer)));
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Reads one typed row from storage and copies its projected columns into <see cref="Buffer"/>, without
    /// copying the row out first. The caller holds the lock on <see cref="Buffer"/>.
    /// </summary>
    /// <returns><see langword="false"/> when the key names no typed row.</returns>
    public bool TryFillFromTypedRow(StorageContext storage, string tableName, string databaseName, RowKey rowId)
    {
        PreparedProjection projection = this;
        return storage.TryVisitTypedCells(
            tableName,
            databaseName,
            rowId,
            ref projection,
            static (ref PreparedProjection target, long _, ReadOnlySpan<CellValue> cells) =>
            {
                target.Fill(cells);
                return true;
            });
    }

    /// <summary>Copies the projected columns of a row's cells into <see cref="Buffer"/>; a column the row lacks is NULL.</summary>
    public void Fill(ReadOnlySpan<CellValue> cells)
    {
        int projectedOrdinal = 0;
        for (int ordinal = 0; ordinal < Columns.Count; ordinal++)
        {
            if (IsProjected[ordinal])
            {
                Buffer[projectedOrdinal++] = ordinal < cells.Length ? cells[ordinal] : CellValue.Null;
            }
        }
    }

    /// <summary>
    /// Maps a materialized <see cref="StoredRow"/> through <see cref="ProjectedSchema"/> and <see cref="Buffer"/>
    /// so both column-name and projected-ordinal readers receive the expected layout.
    /// </summary>
    public T MapStoredRow<T>(StoredRow row, CompiledRowMapper<T> mapper)
    {
        StoredRowView view = row.AsView();
        if (Buffer.Length == 0)
        {
            return mapper(new CompiledRowReader(view));
        }

        lock (Buffer)
        {
            for (int i = 0; i < Buffer.Length; i++)
            {
                string columnName = ProjectedSchema.ColumnAt(i);
                Buffer[i] = view.Schema.TryGetOrdinal(columnName, out int ordinal)
                    ? view[ordinal]
                    : CellValue.Null;
            }

            return mapper(new CompiledRowReader(new StoredRowView(ProjectedSchema, Buffer)));
        }
    }
}

internal readonly struct PreparedSelectSingleCacheKey : IEquatable<PreparedSelectSingleCacheKey>
{
    private readonly DataVoCompiledQueryPlan _plan;
    private readonly Delegate _mapper;

    public PreparedSelectSingleCacheKey(DataVoCompiledQueryPlan plan, Delegate mapper)
    {
        _plan = plan;
        _mapper = mapper;
    }

    public bool Equals(PreparedSelectSingleCacheKey other) =>
        ReferenceEquals(_plan, other._plan)
        && Equals(_mapper.Method, other._mapper.Method)
        && ReferenceEquals(_mapper.Target, other._mapper.Target);

    public override bool Equals(object? obj) =>
        obj is PreparedSelectSingleCacheKey other && Equals(other);

    public override int GetHashCode() =>
        HashCode.Combine(
            RuntimeHelpers.GetHashCode(_plan),
            _mapper.Method,
            _mapper.Target is null ? 0 : RuntimeHelpers.GetHashCode(_mapper.Target));
}

internal readonly struct PreparedSelectManyCacheKey : IEquatable<PreparedSelectManyCacheKey>
{
    private readonly DataVoCompiledQueryPlan _plan;
    private readonly Delegate _mapper;

    public PreparedSelectManyCacheKey(DataVoCompiledQueryPlan plan, Delegate mapper)
    {
        _plan = plan;
        _mapper = mapper;
    }

    public bool Equals(PreparedSelectManyCacheKey other) =>
        ReferenceEquals(_plan, other._plan)
        && Equals(_mapper.Method, other._mapper.Method)
        && ReferenceEquals(_mapper.Target, other._mapper.Target);

    public override bool Equals(object? obj) =>
        obj is PreparedSelectManyCacheKey other && Equals(other);

    public override int GetHashCode() =>
        HashCode.Combine(
            RuntimeHelpers.GetHashCode(_plan),
            _mapper.Method,
            _mapper.Target is null ? 0 : RuntimeHelpers.GetHashCode(_mapper.Target));
}
