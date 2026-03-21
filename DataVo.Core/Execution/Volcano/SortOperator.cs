using DataVo.Core.Utils;

namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Unary operator that materializes and sorts its source rows.
/// </summary>
public sealed class SortOperator : IQueryOperator
{
    /// <summary>
    /// Defines one sort key and its direction.
    /// </summary>
    public sealed class SortKeySpec
    {
        public SortKeySpec(Func<ExecutionRow, object?> keySelector, bool ascending)
        {
            KeySelector = keySelector;
            Ascending = ascending;
        }

        public Func<ExecutionRow, object?> KeySelector { get; }
        public bool Ascending { get; }
    }

    private readonly IQueryOperator _source;
    private readonly IReadOnlyList<SortKeySpec> _sortKeys;

    private List<ExecutionRow> _sortedRows = [];
    private int _index;

    /// <summary>
    /// Initializes a sort operator over a source stream.
    /// </summary>
    public SortOperator(IQueryOperator source, Func<ExecutionRow, object?> keySelector, bool ascending)
        : this(source, [new SortKeySpec(keySelector, ascending)])
    {
    }

    /// <summary>
    /// Initializes a sort operator over a source stream with multiple sort keys.
    /// </summary>
    public SortOperator(IQueryOperator source, IReadOnlyList<SortKeySpec> sortKeys)
    {
        _source = source;
        _sortKeys = sortKeys;
    }

    /// <inheritdoc />
    public void Open()
    {
        _source.Open();

        try
        {
            _sortedRows = [];
            while (true)
            {
                var row = _source.GetNextRow();
                if (row == null)
                {
                    break;
                }

                _sortedRows.Add(row);
            }
        }
        finally
        {
            _source.Close();
        }

        if (_sortKeys.Count > 0)
        {
            IOrderedEnumerable<ExecutionRow>? ordered = null;

            foreach (SortKeySpec key in _sortKeys)
            {
                if (ordered == null)
                {
                    ordered = key.Ascending
                        ? _sortedRows.OrderBy(key.KeySelector, DynamicObjectComparer.Instance)
                        : _sortedRows.OrderByDescending(key.KeySelector, DynamicObjectComparer.Instance);
                }
                else
                {
                    ordered = key.Ascending
                        ? ordered.ThenBy(key.KeySelector, DynamicObjectComparer.Instance)
                        : ordered.ThenByDescending(key.KeySelector, DynamicObjectComparer.Instance);
                }
            }

            _sortedRows = ordered?.ToList() ?? _sortedRows;
        }

        _index = 0;
    }

    /// <inheritdoc />
    public ExecutionRow? GetNextRow()
    {
        if (_index >= _sortedRows.Count)
        {
            return null;
        }

        var row = _sortedRows[_index];
        _index++;
        return row;
    }

    /// <inheritdoc />
    public void Close()
    {
        _sortedRows = [];
        _index = 0;
    }
}
