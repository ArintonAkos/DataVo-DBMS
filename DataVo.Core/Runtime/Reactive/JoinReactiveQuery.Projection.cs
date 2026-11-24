using DataVo.Core.Parser.AST;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// Projection and WHERE-context construction for <see cref="JoinReactiveQuery"/>. The joined output
/// row mirrors the batch join's qualified column naming (for example <c>R.Id</c>), and additionally
/// carries the stable per-side primary-key identities (<c>__rid</c>/<c>__sid</c>) so callers can key a
/// live view by output identity across out-of-place updates.
/// </summary>
internal sealed partial class JoinReactiveQuery
{
    private enum Side
    {
        Left,
        Right,
    }

    private sealed record ProjectedColumn(string OutputName, Side? Side, string Column, bool IsStar);

    /// <summary>The hidden identity columns added to every joined output row.</summary>
    private const string LeftIdentityColumn = "__rid";
    private const string RightIdentityColumn = "__sid";

    private readonly List<ProjectedColumn> _projection = [];

    private void BuildProjection()
    {
        foreach (SelectColumnNode column in _shape.Columns)
        {
            if (IsStarColumn(column))
            {
                _projection.Add(new ProjectedColumn(OutputName: "*", Side: null, Column: "*", IsStar: true));
                continue;
            }

            (Side? side, string columnName, string outputName) = ResolveColumn(column);
            _projection.Add(new ProjectedColumn(outputName, side, columnName, IsStar: false));
        }
    }

    /// <summary>Where one output column's value comes from.</summary>
    private enum SourceKind : byte
    {
        Left,
        Right,
        Unqualified,
        LeftIdentity,
        RightIdentity,
    }

    /// <summary>The source of one output column: a side ordinal (or -1), or a hidden identity column.</summary>
    private readonly record struct OutputSource(SourceKind Kind, int LeftOrdinal, int RightOrdinal);

    /// <summary>
    /// Builds the output column names and their sources: each explicit projection column, or — for
    /// <c>SELECT *</c> — both tables' catalog columns qualified as <c>Table.Column</c>, followed by the hidden
    /// identity columns every joined row carries.
    /// </summary>
    private (string[] Names, OutputSource[] Sources) BuildOutputs()
    {
        var names = new List<string>();
        var sources = new List<OutputSource>();
        foreach (ProjectedColumn column in _projection)
        {
            if (column.IsStar)
            {
                AddStarOutputs(_shape.LeftTable, _left.Layout, SourceKind.Left, names, sources);
                AddStarOutputs(_shape.RightTable, _right.Layout, SourceKind.Right, names, sources);
                continue;
            }

            int leftOrdinal = column.Side is null or Side.Left ? _left.Layout.OrdinalOf(column.Column) : -1;
            int rightOrdinal = column.Side is null or Side.Right ? _right.Layout.OrdinalOf(column.Column) : -1;
            SourceKind kind = column.Side switch
            {
                Side.Left => SourceKind.Left,
                Side.Right => SourceKind.Right,
                _ => SourceKind.Unqualified,
            };

            names.Add(column.OutputName);
            sources.Add(new OutputSource(kind, leftOrdinal, rightOrdinal));
        }

        names.Add(LeftIdentityColumn);
        sources.Add(new OutputSource(SourceKind.LeftIdentity, -1, -1));
        names.Add(RightIdentityColumn);
        sources.Add(new OutputSource(SourceKind.RightIdentity, -1, -1));
        return (names.ToArray(), sources.ToArray());
    }

    private static void AddStarOutputs(
        string table,
        ReactiveRowLayout layout,
        SourceKind kind,
        List<string> names,
        List<OutputSource> sources)
    {
        for (int i = 0; i < layout.Count; i++)
        {
            names.Add(table + "." + layout.Columns[i]);
            sources.Add(kind == SourceKind.Left ? new OutputSource(kind, i, -1) : new OutputSource(kind, -1, i));
        }
    }

    /// <summary>
    /// Projects a pair of side rows into <paramref name="destination"/> in output-schema order. Either side
    /// may be absent for a null-padded outer-join row; its columns then read as <c>NULL</c>.
    /// </summary>
    private void WriteRow(EmittedPair pair, CellValue[] destination)
    {
        for (int i = 0; i < _outputSources.Length; i++)
        {
            destination[i] = ResolveCell(_outputSources[i], pair);
        }
    }

    private static CellValue ResolveCell(OutputSource source, EmittedPair pair)
    {
        return source.Kind switch
        {
            SourceKind.Left => ReadCell(pair.Left, source.LeftOrdinal),
            SourceKind.Right => ReadCell(pair.Right, source.RightOrdinal),
            // Unqualified column: resolve from whichever side defines it (left first, then right).
            SourceKind.Unqualified => pair.Left is not null && source.LeftOrdinal >= 0
                ? pair.Left.Cells[source.LeftOrdinal]
                : ReadCell(pair.Right, source.RightOrdinal),
            SourceKind.LeftIdentity => pair.Left is null ? CellValue.Null : CellValue.From(pair.Left.IdentityText),
            _ => pair.Right is null ? CellValue.Null : CellValue.From(pair.Right.IdentityText),
        };
    }

    private static CellValue ReadCell(SideRow? row, int ordinal) =>
        row is not null && ordinal >= 0 ? row.Cells[ordinal] : CellValue.Null;

    /// <summary>
    /// Evaluates the post-join WHERE over a pair of side rows (either may be absent for a null-padded row),
    /// reading the rows' typed cells through a reused context; <see langword="true"/> when there is no WHERE.
    /// </summary>
    private bool PassesWhere(EmittedPair pair)
    {
        if (!_hasWhere)
        {
            return true;
        }

        return _whereContext.Matches(_where, pair.Left?.Cells, pair.Right?.Cells);
    }

    private (Side? Side, string Column, string OutputName) ResolveColumn(SelectColumnNode column)
    {
        string? qualifier = null;
        string columnName;

        switch (column.Expression)
        {
            case ColumnRefNode columnRef:
                qualifier = columnRef.TableOrAlias;
                columnName = columnRef.Column;
                break;

            case ResolvedColumnRefNode resolved:
                qualifier = resolved.TableName;
                columnName = resolved.Column;
                break;

            default:
                (qualifier, columnName) = SplitQualified(column.RawExpression.Trim());
                break;
        }

        Side? side = ResolveSide(qualifier);
        string outputName = column.Alias
            ?? (qualifier is not null ? qualifier + "." + columnName : columnName);

        return (side, columnName, outputName);
    }

    private Side? ResolveSide(string? qualifier)
    {
        if (string.IsNullOrEmpty(qualifier))
        {
            return null;
        }

        if (qualifier.Equals(_shape.LeftTable, StringComparison.OrdinalIgnoreCase)
            || qualifier.Equals(_shape.LeftAlias, StringComparison.OrdinalIgnoreCase))
        {
            return Side.Left;
        }

        if (qualifier.Equals(_shape.RightTable, StringComparison.OrdinalIgnoreCase)
            || qualifier.Equals(_shape.RightAlias, StringComparison.OrdinalIgnoreCase))
        {
            return Side.Right;
        }

        throw new NotSupportedException(
            $"Reactive join projection references unknown table qualifier '{qualifier}'.");
    }

    private static (string? Qualifier, string Column) SplitQualified(string raw)
    {
        // Tokens may arrive space-separated (for example "R . Id"); strip whitespace around the dot.
        string normalized = raw.Replace(" ", string.Empty);
        int dot = normalized.LastIndexOf('.');
        return dot >= 0
            ? (normalized[..dot], normalized[(dot + 1)..])
            : (null, normalized);
    }

    private static bool IsStarColumn(SelectColumnNode column)
    {
        string raw = column.RawExpression.Trim();
        return raw == "*"
            || (column.Expression is null && raw.EndsWith("*", StringComparison.Ordinal));
    }

    /// <summary>
    /// The typed evaluation context for a post-join WHERE. It exposes every column of both sides under its
    /// bare, table-qualified and alias-qualified names, like the batch join's row context; when both sides
    /// define the same bare name the right side wins, so qualify the column to disambiguate.
    /// </summary>
    private sealed class JoinWhereContext
    {
        private readonly ReactiveRowSchema _leftOnly;
        private readonly ReactiveRowSchema _rightOnly;
        private readonly ReactiveRowSchema _both;
        private readonly CellValue[] _scratch;
        private readonly int _leftWidth;
        private readonly int _rightWidth;

        /// <summary>Creates the context for the given side layouts, table names and aliases.</summary>
        public JoinWhereContext(JoinShape shape, ReactiveRowLayout left, ReactiveRowLayout right)
        {
            List<string> leftNames = ContextNames(left, shape.LeftTable, shape.LeftAlias);
            List<string> rightNames = ContextNames(right, shape.RightTable, shape.RightAlias);
            _leftOnly = new ReactiveRowSchema(leftNames);
            _rightOnly = new ReactiveRowSchema(rightNames);
            _both = new ReactiveRowSchema(leftNames.Concat(rightNames).ToArray());
            _leftWidth = leftNames.Count;
            _rightWidth = rightNames.Count;
            _scratch = new CellValue[_leftWidth + _rightWidth];
        }

        /// <summary>Evaluates <paramref name="where"/> over the given side cells (either may be absent).</summary>
        public bool Matches(ReactivePredicate where, CellValue[]? left, CellValue[]? right)
        {
            Span<CellValue> scratch = _scratch;
            if (left is not null && right is not null)
            {
                Fill(left, scratch[.._leftWidth]);
                Fill(right, scratch[_leftWidth..]);
                return where.Matches(new RowRef(_both, scratch));
            }

            if (left is not null)
            {
                Fill(left, scratch[.._leftWidth]);
                return where.Matches(new RowRef(_leftOnly, scratch[.._leftWidth]));
            }

            Fill(right!, scratch[.._rightWidth]);
            return where.Matches(new RowRef(_rightOnly, scratch[.._rightWidth]));
        }

        // Each stored cell appears three times: under its bare, table-qualified and alias-qualified name.
        private static void Fill(CellValue[] cells, Span<CellValue> destination)
        {
            for (int i = 0; i < cells.Length; i++)
            {
                destination[3 * i] = cells[i];
                destination[(3 * i) + 1] = cells[i];
                destination[(3 * i) + 2] = cells[i];
            }
        }

        private static List<string> ContextNames(ReactiveRowLayout layout, string table, string? alias)
        {
            var names = new List<string>(layout.Count * 3);
            foreach (string column in layout.Columns)
            {
                names.Add(column);
                names.Add(table + "." + column);
                names.Add(alias + "." + column);
            }

            return names;
        }
    }
}
