using System.Runtime.CompilerServices;
using DataVo.Core.BTree;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Serialization;

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

    private T? ExecuteIntegerKey(long value)
    {
        if (_indexName is not null
            && _context.Engine.IndexManager.TryLookupIntegerPrimaryKey(
                value,
                _indexName,
                _plan.TableName,
                _databaseName,
                out long rowId))
        {
            return TryProjectRow(rowId, out T? result) ? result : default;
        }

        return ExecuteKey(DataVoCompiledQuery.BuildScalarComparisonKey(value));
    }

    private T? ExecuteGuidKey(Guid value)
    {
        if (_indexName is not null
            && _context.Engine.IndexManager.TryLookupGuidPrimaryKey(
                value,
                _indexName,
                _plan.TableName,
                _databaseName,
                out long rowId))
        {
            return TryProjectRow(rowId, out T? result) ? result : default;
        }

        return ExecuteKey(DataVoCompiledQuery.BuildScalarComparisonKey(value));
    }

    private T? ExecuteKey(string expectedKey)
    {
        if (_indexName is not null)
        {
            try
            {
                IReadOnlyList<long> rowIds = _context.Engine.IndexManager.FilterUsingIndex(
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

    private bool TryProjectRow(long rowId, out T? result)
    {
        result = default;
        if (!_context.Engine.StorageContext.IsRowVisible(_plan.TableName, _databaseName, rowId))
        {
            return false;
        }

        if (_context.Engine.StorageContext.TryReadStoredRow(_plan.TableName, _databaseName, rowId, out StoredRow? storedRow)
            && storedRow is not null)
        {
            result = _mapper(new CompiledRowReader(storedRow.AsView()));
            return true;
        }

        byte[]? bytes = _context.Engine.StorageContext.TryReadRowBytes(_plan.TableName, _databaseName, rowId);
        if (bytes is null)
        {
            return false;
        }

        lock (_projection.Buffer)
        {
            RowSerializer.DecodeProjectedCells(bytes, _projection.Columns, _projection.IsProjected, _projection.Buffer);
            result = _mapper(new CompiledRowReader(new StoredRowView(_projection.ProjectedSchema, _projection.Buffer)));
        }

        return true;
    }

    private T? ExecuteScanFallback(string expectedKey)
    {
        Dictionary<long, StoredRow> rows = _context.Engine.StorageContext.GetTypedTableContents(_plan.TableName, _databaseName);
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
        ReferenceEquals(_plan, other._plan) && ReferenceEquals(_mapper, other._mapper);

    public override bool Equals(object? obj) =>
        obj is PreparedSelectSingleCacheKey other && Equals(other);

    public override int GetHashCode() =>
        HashCode.Combine(RuntimeHelpers.GetHashCode(_plan), RuntimeHelpers.GetHashCode(_mapper));
}
