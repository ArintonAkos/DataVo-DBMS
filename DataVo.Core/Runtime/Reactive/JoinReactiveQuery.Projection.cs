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

    /// <summary>
    /// Builds the projected output row for a (leftRow, rightRow) pair, applying the WHERE predicate;
    /// returns <c>null</c> when the WHERE rejects the row. Either side may be <c>null</c> for a
    /// null-padded outer-join row.
    /// </summary>
    private IReadOnlyDictionary<string, object?>? ProjectJoined(
        IReadOnlyDictionary<string, object?>? leftRow,
        IReadOnlyDictionary<string, object?>? rightRow)
    {
        Dictionary<string, object?> context = BuildContext(leftRow, rightRow);

        if (!_where.Matches(context))
        {
            return null;
        }

        var output = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        foreach (ProjectedColumn column in _projection)
        {
            if (column.IsStar)
            {
                ProjectStar(output, leftRow, rightRow);
                continue;
            }

            output[column.OutputName] = ResolveValue(column, leftRow, rightRow);
        }

        output[LeftIdentityColumn] = leftRow is null ? null : Identity(_leftPrimaryKeys, leftRow);
        output[RightIdentityColumn] = rightRow is null ? null : Identity(_rightPrimaryKeys, rightRow);

        return output;
    }

    private object? ResolveValue(
        ProjectedColumn column,
        IReadOnlyDictionary<string, object?>? leftRow,
        IReadOnlyDictionary<string, object?>? rightRow)
    {
        return column.Side switch
        {
            Side.Left => leftRow is not null && leftRow.TryGetValue(column.Column, out object? lv) ? lv : null,
            Side.Right => rightRow is not null && rightRow.TryGetValue(column.Column, out object? rv) ? rv : null,
            // Unqualified column: resolve from whichever side defines it (left first, then right).
            _ => ResolveUnqualified(column.Column, leftRow, rightRow),
        };
    }

    private static object? ResolveUnqualified(
        string columnName,
        IReadOnlyDictionary<string, object?>? leftRow,
        IReadOnlyDictionary<string, object?>? rightRow)
    {
        if (leftRow is not null && leftRow.TryGetValue(columnName, out object? lv))
        {
            return lv;
        }

        if (rightRow is not null && rightRow.TryGetValue(columnName, out object? rv))
        {
            return rv;
        }

        return null;
    }

    private void ProjectStar(
        Dictionary<string, object?> output,
        IReadOnlyDictionary<string, object?>? leftRow,
        IReadOnlyDictionary<string, object?>? rightRow)
    {
        if (leftRow is not null)
        {
            foreach ((string key, object? value) in leftRow)
            {
                output[_shape.LeftTable + "." + key] = value;
            }
        }

        if (rightRow is not null)
        {
            foreach ((string key, object? value) in rightRow)
            {
                output[_shape.RightTable + "." + key] = value;
            }
        }
    }

    /// <summary>
    /// Builds the predicate-evaluation context with both qualified (<c>R.Id</c>) and bare (<c>Id</c>)
    /// keys so <see cref="ReactivePredicate"/> (which resolves by bare column name) can evaluate a
    /// post-join WHERE. When both sides expose the same bare column the right side wins; qualify the
    /// column in the WHERE to disambiguate.
    /// </summary>
    private Dictionary<string, object?> BuildContext(
        IReadOnlyDictionary<string, object?>? leftRow,
        IReadOnlyDictionary<string, object?>? rightRow)
    {
        var context = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);

        if (leftRow is not null)
        {
            foreach ((string key, object? value) in leftRow)
            {
                context[key] = value;
                context[_shape.LeftTable + "." + key] = value;
                context[_shape.LeftAlias + "." + key] = value;
            }
        }

        if (rightRow is not null)
        {
            foreach ((string key, object? value) in rightRow)
            {
                context[key] = value;
                context[_shape.RightTable + "." + key] = value;
                context[_shape.RightAlias + "." + key] = value;
            }
        }

        return context;
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
}
