using DataVo.Core.BTree;
using DataVo.Core.Exceptions;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Serialization;

namespace DataVo.Core.CompiledQueries;

/// <summary>
/// Prepared typed multi-row lookup for a <see cref="DataVoCompiledQueryPlan"/>.
/// </summary>
public sealed class DataVoPreparedSelectMany<T>
{
    private readonly DataVoContext _context;
    private readonly DataVoCompiledQueryPlan _plan;
    private readonly string _databaseName;
    private readonly string? _indexName;
    private readonly PreparedProjection _projection;
    private readonly CompiledRowMapper<T> _mapper;

    internal DataVoPreparedSelectMany(
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
    public IReadOnlyList<T> Execute(int value) => ExecuteIntegerKey(value);

    /// <summary>Executes the prepared lookup for a BIGINT predicate value.</summary>
    public IReadOnlyList<T> Execute(long value) => ExecuteIntegerKey(value);

    /// <summary>Executes the prepared lookup for a FLOAT predicate value.</summary>
    public IReadOnlyList<T> Execute(double value) => ExecuteKey(DataVoCompiledQuery.BuildScalarComparisonKey(value));

    /// <summary>Executes the prepared lookup for a text predicate value.</summary>
    public IReadOnlyList<T> Execute(string? value) => ExecuteKey(DataVoCompiledQuery.BuildScalarComparisonKey(value));

    /// <summary>Executes the prepared lookup for a general predicate value.</summary>
    public IReadOnlyList<T> Execute(object? value) => ExecuteKey(DataVoCompiledQuery.BuildScalarComparisonKey(value));

    private IReadOnlyList<T> ExecuteIntegerKey(long value)
    {
        if (_indexName is not null
            && _context.Engine.IndexManager.TryLookupIntegerIndex(
                value,
                _indexName,
                _plan.TableName,
                _databaseName,
                out IReadOnlyList<long> rowIds))
        {
            var results = new List<T>(rowIds.Count);
            for (int i = 0; i < rowIds.Count; i++)
            {
                if (TryProjectRow(rowIds[i], out T? result))
                {
                    results.Add(result!);
                }
            }

            return results;
        }

        return ExecuteKey(DataVoCompiledQuery.BuildScalarComparisonKey(value));
    }

    private IReadOnlyList<T> ExecuteKey(string expectedKey)
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

                var results = new List<T>(rowIds.Count);
                for (int i = 0; i < rowIds.Count; i++)
                {
                    if (TryProjectRow(rowIds[i], out T? result))
                    {
                        results.Add(result!);
                    }
                }

                return results;
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

    private IReadOnlyList<T> ExecuteScanFallback(string expectedKey)
    {
        Dictionary<long, StoredRow> rows = _context.Engine.StorageContext.GetTypedTableContents(_plan.TableName, _databaseName);
        string[] whereColumns = [_plan.WhereColumn!];
        var results = new List<T>();
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

            results.Add(_mapper(new CompiledRowReader(view)));
        }

        return results;
    }
}
