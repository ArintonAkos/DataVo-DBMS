using System.Globalization;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Incrementally maintained <c>GROUP BY</c> state with <c>COUNT</c>/<c>SUM</c>/<c>AVG</c>/<c>MIN</c>/<c>MAX</c>,
/// shared by the single-table and the join aggregate operators.
/// </summary>
/// <remarks>
/// Input rows are flat <see cref="CellValue"/> spans in a layout chosen by the owning operator; every column the
/// query names is resolved to an ordinal of that layout once, at compile time. State is keyed by the rendered
/// group key, so it is independent of row identity. Invertible aggregates (<c>COUNT</c>/<c>SUM</c>/<c>AVG</c>)
/// add and subtract a row's contribution; <c>MIN</c>/<c>MAX</c> keep a per-group value multiset so the next
/// extreme surfaces after the current one is removed. Groups touched since the last
/// <see cref="FlushInto"/> are classified there: a group not yet delivered is <c>Added</c>, a delivered one is
/// <c>Updated</c>, and a group whose row count reached zero is <c>Removed</c> (with NULL aggregate cells).
/// </remarks>
internal sealed partial class ReactiveGroupAggregator
{
    private enum AggregateFunction
    {
        Count,
        Sum,
        Avg,
        Min,
        Max,
    }

    /// <summary>One aggregate output column; <see cref="Ordinal"/> is -1 for <c>COUNT(*)</c>.</summary>
    private sealed record AggregateSpec(string OutputName, AggregateFunction Function, int Ordinal);

    private sealed class GroupState(CellValue[] keyValues, int aggregateCount)
    {
        public long Count;
        public readonly CellValue[] KeyValues = keyValues;
        public readonly long[] NonNullCount = new long[aggregateCount];
        public readonly decimal[] Sum = new decimal[aggregateCount];
        public readonly bool[] SumIsFractional = new bool[aggregateCount];
        public readonly ReactiveExtremumMultiset?[] Extremes = new ReactiveExtremumMultiset?[aggregateCount];
    }

    private readonly int[] _groupOrdinals;
    private readonly AggregateSpec[] _aggregates;

    // For each group output column, its index within _groupOrdinals (-1 when the column is not grouped on).
    private readonly int[] _groupOutputIndex;
    private readonly Dictionary<string, GroupState> _groups = new(StringComparer.Ordinal);

    // Group keys currently delivered, so a re-touched group is an update and an emptied one is retracted once.
    private readonly HashSet<string> _emittedGroups = new(StringComparer.Ordinal);

    // Groups touched since the last flush; a removed group keeps its key values here until it is classified.
    private readonly Dictionary<string, CellValue[]> _touched = new(StringComparer.Ordinal);
    private readonly CellValue[] _rowScratch;

    // Reused buffer for rendering a group's composite key, so steady-state lookups allocate no string; a real
    // string is materialized only when a brand-new group is created.
    private char[] _keyBuffer = new char[64];

    /// <summary>
    /// Compiles the grouping and aggregate columns of <paramref name="select"/> against an input layout.
    /// </summary>
    /// <param name="select">The aggregate SELECT.</param>
    /// <param name="resolveOrdinal">Maps a column reference (optional qualifier, column name) to its input ordinal; throws <see cref="NotSupportedException"/> for an unknown column.</param>
    /// <exception cref="NotSupportedException">An aggregate function or argument outside the supported subset.</exception>
    public ReactiveGroupAggregator(SelectStatement select, Func<string?, string, int> resolveOrdinal)
    {
        var groupOrdinals = new List<int>();
        if (select.GroupByExpression is not null)
        {
            foreach (IdentifierNode column in select.GroupByExpression.Columns)
            {
                (string? qualifier, string name) = SplitQualified(column.Name);
                groupOrdinals.Add(resolveOrdinal(qualifier, name));
            }
        }

        _groupOrdinals = [.. groupOrdinals];

        var aggregates = new List<AggregateSpec>();
        var groupOutputs = new List<(string Name, int Ordinal)>();
        foreach (SelectColumnNode column in select.Columns)
        {
            if (column.Expression is AggregateExpressionNode aggregate)
            {
                aggregates.Add(BuildAggregateSpec(column, aggregate, resolveOrdinal));
                continue;
            }

            (string? qualifier, string name) = ColumnReference(column);
            groupOutputs.Add((column.Alias ?? name, resolveOrdinal(qualifier, name)));
        }

        _aggregates = [.. aggregates];
        _groupOutputIndex = groupOutputs.Select(output => Array.IndexOf(_groupOrdinals, output.Ordinal)).ToArray();

        // Output schema = group output columns (declared order) followed by aggregate outputs.
        string[] outputNames = [.. groupOutputs.Select(output => output.Name), .. _aggregates.Select(spec => spec.OutputName)];
        OutputSchema = new ReactiveRowSchema(outputNames);
        _rowScratch = new CellValue[outputNames.Length];
    }

    /// <summary>Gets the schema of the rows this aggregator emits.</summary>
    public ReactiveRowSchema OutputSchema { get; }

    /// <summary>Adds one input row's contribution to its group, creating the group on first sight.</summary>
    /// <param name="row">The input row, in the compiled layout.</param>
    public void Add(ReadOnlySpan<CellValue> row) => Apply(row, +1);

    /// <summary>Removes one input row's contribution from its group, dropping the group when it empties.</summary>
    /// <param name="row">The input row, in the compiled layout.</param>
    public void Remove(ReadOnlySpan<CellValue> row) => Apply(row, -1);

    /// <summary>Forgets the touched groups without emitting them (after seeding the baseline).</summary>
    public void DiscardTouched() => _touched.Clear();

    /// <summary>
    /// Writes the groups touched since the last flush into <paramref name="builder"/> as added, updated or
    /// removed rows, then forgets them.
    /// </summary>
    /// <param name="builder">The builder receiving the rows.</param>
    public void FlushInto(QueryChangeBuilder builder)
    {
        foreach (KeyValuePair<string, CellValue[]> touched in _touched)
        {
            string key = touched.Key;
            if (_groups.TryGetValue(key, out GroupState? state))
            {
                WriteGroupRow(state);
                if (_emittedGroups.Add(key))
                {
                    builder.AddAddedRow(_rowScratch);
                }
                else
                {
                    builder.AddUpdatedRow(_rowScratch);
                }
            }
            else if (_emittedGroups.Remove(key))
            {
                WriteRemovedRow(touched.Value);
                builder.AddRemovedRow(_rowScratch);
            }
        }

        _touched.Clear();
    }

    private void Apply(ReadOnlySpan<CellValue> row, int weight)
    {
        ReadOnlySpan<char> keySpan = BuildGroupKey(row);
        if (!TryGetGroup(keySpan, out string? key, out GroupState? state))
        {
            if (weight < 0)
            {
                // A retraction for a group that no longer exists still needs classifying. Off the steady state.
                key = new string(keySpan);
                _touched[key] = CaptureGroupValues(row);
                return;
            }

            key = new string(keySpan);
            state = new GroupState(CaptureGroupValues(row), _aggregates.Length);
            _groups[key] = state;
            IndexGroup(key, state);
        }

        state!.Count += weight;
        for (int i = 0; i < _aggregates.Length; i++)
        {
            AggregateSpec spec = _aggregates[i];
            if (spec.Ordinal < 0 || row[spec.Ordinal].IsNull)
            {
                continue;
            }

            CellValue value = row[spec.Ordinal];
            state.NonNullCount[i] += weight;
            switch (spec.Function)
            {
                case AggregateFunction.Sum:
                case AggregateFunction.Avg:
                    state.Sum[i] += weight * ToDecimal(value);
                    if (weight > 0 && value.Type is not (CellType.Int32 or CellType.Int64))
                    {
                        state.SumIsFractional[i] = true;
                    }

                    break;

                case AggregateFunction.Min:
                case AggregateFunction.Max:
                    ReactiveExtremumMultiset extremes = state.Extremes[i] ??= new ReactiveExtremumMultiset();
                    if (weight > 0)
                    {
                        extremes.Add(value);
                    }
                    else
                    {
                        extremes.Remove(value);
                    }

                    break;
            }
        }

        _touched[key!] = state.KeyValues;
        if (state.Count <= 0)
        {
            _groups.Remove(key!);
            UnindexGroup(key!);
        }
    }

    /// <summary>Writes a present group's row (group cells, then aggregate results) into the row scratch.</summary>
    private void WriteGroupRow(GroupState state)
    {
        WriteGroupCells(state.KeyValues);
        int offset = _groupOutputIndex.Length;
        for (int i = 0; i < _aggregates.Length; i++)
        {
            _rowScratch[offset + i] = ComputeAggregateCell(i, state);
        }
    }

    /// <summary>Writes a removed group's row into the row scratch: its group cells with NULL aggregate cells.</summary>
    private void WriteRemovedRow(CellValue[] keyValues)
    {
        WriteGroupCells(keyValues);
        _rowScratch.AsSpan(_groupOutputIndex.Length).Fill(CellValue.Null);
    }

    private void WriteGroupCells(CellValue[] keyValues)
    {
        for (int i = 0; i < _groupOutputIndex.Length; i++)
        {
            int index = _groupOutputIndex[i];
            _rowScratch[i] = index >= 0 ? keyValues[index] : CellValue.Null;
        }
    }

    /// <summary>
    /// Computes one aggregate's result: COUNT and integral SUM as <c>long</c>, fractional SUM and AVG as
    /// <c>decimal</c>, an empty SUM/AVG/MIN/MAX as NULL.
    /// </summary>
    private CellValue ComputeAggregateCell(int index, GroupState state)
    {
        AggregateSpec spec = _aggregates[index];
        long nonNull = state.NonNullCount[index];
        switch (spec.Function)
        {
            case AggregateFunction.Count:
                return CellValue.From(spec.Ordinal < 0 ? state.Count : nonNull);

            case AggregateFunction.Sum:
                if (nonNull == 0)
                {
                    return CellValue.Null;
                }

                decimal sum = state.Sum[index];
                return state.SumIsFractional[index] ? CellValue.From(sum) : CellValue.From(Convert.ToInt64(sum));

            case AggregateFunction.Avg:
                return nonNull == 0 ? CellValue.Null : CellValue.From(state.Sum[index] / nonNull);

            case AggregateFunction.Min:
                return state.Extremes[index]?.Min ?? CellValue.Null;

            case AggregateFunction.Max:
                return state.Extremes[index]?.Max ?? CellValue.Null;

            default:
                return CellValue.Null;
        }
    }

    /// <summary>
    /// Renders a row's composite group key into the reused key buffer: components joined by U+001F, a NULL
    /// component as "\0NULL" and a value as "v:" plus its invariant text. Steady state allocates nothing.
    /// </summary>
    private ReadOnlySpan<char> BuildGroupKey(ReadOnlySpan<CellValue> row)
    {
        int pos = 0;
        for (int i = 0; i < _groupOrdinals.Length; i++)
        {
            if (i > 0)
            {
                AppendChars(ref pos, "\u001F");
            }

            CellValue value = row[_groupOrdinals[i]];
            if (value.IsNull)
            {
                AppendChars(ref pos, "\0NULL");
            }
            else
            {
                AppendChars(ref pos, "v:");
                AppendValue(ref pos, value);
            }
        }

        return _keyBuffer.AsSpan(0, pos);
    }

    private void AppendChars(ref int pos, ReadOnlySpan<char> text)
    {
        EnsureKeyCapacity(pos + text.Length);
        text.CopyTo(_keyBuffer.AsSpan(pos));
        pos += text.Length;
    }

    private void AppendValue(ref int pos, CellValue value)
    {
        switch (value.Type)
        {
            case CellType.Boolean:
                AppendChars(ref pos, value.AsBoolean() ? "True" : "False");
                return;

            case CellType.Int32:
                AppendFormattable(ref pos, value.AsInt32());
                return;

            case CellType.Int64:
                AppendFormattable(ref pos, value.AsInt64());
                return;

            case CellType.Double:
                AppendFormattable(ref pos, value.AsDouble());
                return;

            case CellType.Decimal:
                AppendFormattable(ref pos, value.AsDecimal());
                return;

            case CellType.String:
                AppendChars(ref pos, value.AsString() ?? string.Empty);
                return;

            case CellType.Date:
                AppendDate(ref pos, value.AsDate());
                return;

            case CellType.DateTime:
                AppendFormattable(ref pos, value.AsUtcTicks());
                return;

            default:
                AppendChars(ref pos, Convert.ToString(value.ToObject(), CultureInfo.InvariantCulture) ?? string.Empty);
                return;
        }
    }

    private void AppendDate(ref int pos, DataVoDate value)
    {
        DateTime dateTime = value.ToDateTime();
        int written;
        while (!dateTime.TryFormat(_keyBuffer.AsSpan(pos), out written, "yyyy-MM-dd", CultureInfo.InvariantCulture))
        {
            EnsureKeyCapacity(Math.Max(_keyBuffer.Length * 2, pos + 64));
        }

        pos += written;
    }

    private void EnsureKeyCapacity(int min)
    {
        if (_keyBuffer.Length < min)
        {
            Array.Resize(ref _keyBuffer, Math.Max(_keyBuffer.Length * 2, min));
        }
    }

    private CellValue[] CaptureGroupValues(ReadOnlySpan<CellValue> row)
    {
        var values = new CellValue[_groupOrdinals.Length];
        for (int i = 0; i < values.Length; i++)
        {
            values[i] = row[_groupOrdinals[i]];
        }

        return values;
    }

    private static AggregateSpec BuildAggregateSpec(
        SelectColumnNode column,
        AggregateExpressionNode aggregate,
        Func<string?, string, int> resolveOrdinal)
    {
        AggregateFunction function = aggregate.FunctionName.ToUpperInvariant() switch
        {
            "COUNT" => AggregateFunction.Count,
            "SUM" => AggregateFunction.Sum,
            "AVG" => AggregateFunction.Avg,
            "MIN" => AggregateFunction.Min,
            "MAX" => AggregateFunction.Max,
            _ => throw new NotSupportedException(
                $"Reactive aggregates do not support the function '{aggregate.FunctionName}'."),
        };

        int ordinal = -1;
        if (!aggregate.IsStar && aggregate.Argument is not null)
        {
            (string? qualifier, string name) = aggregate.Argument switch
            {
                ColumnRefNode columnRef => (NullIfEmpty(columnRef.TableOrAlias), columnRef.Column),
                ResolvedColumnRefNode resolved => (NullIfEmpty(resolved.TableName), resolved.Column),
                _ => throw new NotSupportedException(
                    "Reactive aggregates support only a bare column argument (for example SUM(Score))."),
            };
            ordinal = resolveOrdinal(qualifier, name);
        }

        if (function is not AggregateFunction.Count && ordinal < 0)
        {
            throw new NotSupportedException(
                $"Reactive aggregate '{aggregate.FunctionName}' requires a column argument.");
        }

        return new AggregateSpec(column.Alias ?? column.RawExpression.Trim(), function, ordinal);
    }

    /// <summary>Resolves a non-aggregate select column to its (qualifier, column name).</summary>
    private static (string? Qualifier, string Column) ColumnReference(SelectColumnNode column) => column.Expression switch
    {
        ColumnRefNode columnRef => (NullIfEmpty(columnRef.TableOrAlias), columnRef.Column),
        ResolvedColumnRefNode resolved => (NullIfEmpty(resolved.TableName), resolved.Column),
        _ => SplitQualified(column.RawExpression.Trim()),
    };

    /// <summary>Splits <c>qualifier.column</c> into its parts; an unqualified name has a <c>null</c> qualifier.</summary>
    internal static (string? Qualifier, string Column) SplitQualified(string name)
    {
        int dot = name.LastIndexOf('.');
        return dot >= 0 ? (name[..dot], name[(dot + 1)..]) : (null, name);
    }

    private static string? NullIfEmpty(string? value) => string.IsNullOrWhiteSpace(value) ? null : value;

    private static decimal ToDecimal(CellValue value) => value.Type switch
    {
        CellType.Int32 => value.AsInt32(),
        CellType.Int64 => value.AsInt64(),
        CellType.Double => Convert.ToDecimal(value.AsDouble()),
        CellType.Decimal => value.AsDecimal(),
        CellType.String => Convert.ToDecimal(value.AsString(), CultureInfo.InvariantCulture),
        _ => Convert.ToDecimal(value.ToObject(), CultureInfo.InvariantCulture),
    };
}
