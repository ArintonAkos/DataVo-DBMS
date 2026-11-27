using DataVo.Core.Parser.AST;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A compiled single-table projection: the output schema and, per output column, the ordinal of the source
/// column in the table's catalog layout.
/// </summary>
/// <param name="Schema">The output row schema.</param>
/// <param name="Ordinals">The catalog-layout ordinal feeding each output column.</param>
internal sealed record ReactiveProjection(ReactiveRowSchema Schema, int[] Ordinals)
{
    /// <summary>Copies the projected cells of <paramref name="image"/> into <paramref name="destination"/>.</summary>
    /// <param name="image">A row in the table's catalog layout.</param>
    /// <param name="destination">The output cells; its length must equal the schema's column count.</param>
    public void Project(ReadOnlySpan<CellValue> image, Span<CellValue> destination)
    {
        for (int i = 0; i < Ordinals.Length; i++)
        {
            destination[i] = image[Ordinals[i]];
        }
    }
}

/// <summary>
/// Shared helpers that bind single-table reactive operators to their table's catalog layout: the layout
/// itself, column resolution, and the SELECT-list projection.
/// </summary>
internal static class ReactiveTableLayouts
{
    /// <summary>Builds the catalog-column layout of <paramref name="table"/>.</summary>
    /// <param name="engine">The owning engine.</param>
    /// <param name="table">The table name.</param>
    /// <param name="databaseName">The database that owns the table.</param>
    /// <returns>The layout, in catalog column order.</returns>
    public static ReactiveRowLayout ForTable(DataVoEngine engine, string table, string databaseName) =>
        new(engine.Catalog.GetTableColumns(table, databaseName).Select(column => column.Name).ToList());

    /// <summary>
    /// Resolves a column reference against a single table's layout.
    /// </summary>
    /// <param name="layout">The table's layout.</param>
    /// <param name="table">The table name.</param>
    /// <param name="alias">The table's alias (or its name).</param>
    /// <param name="qualifier">The reference's qualifier, or <c>null</c>.</param>
    /// <param name="column">The column name.</param>
    /// <returns>The column's ordinal.</returns>
    /// <exception cref="NotSupportedException">The qualifier names another table, or the column does not exist.</exception>
    public static int RequireOrdinal(ReactiveRowLayout layout, string table, string alias, string? qualifier, string column)
    {
        if (qualifier is not null
            && !qualifier.Equals(table, StringComparison.OrdinalIgnoreCase)
            && !qualifier.Equals(alias, StringComparison.OrdinalIgnoreCase))
        {
            throw new NotSupportedException($"Reactive subscriptions cannot resolve '{qualifier}.{column}': '{qualifier}' is not the queried table.");
        }

        int ordinal = layout.OrdinalOf(column);
        return ordinal >= 0
            ? ordinal
            : throw new NotSupportedException($"Reactive subscriptions cannot resolve column '{column}' of table '{table}'.");
    }

    /// <summary>
    /// Compiles a single-table SELECT list (plain or <c>*</c> columns) into a projection over the table layout.
    /// </summary>
    /// <param name="columns">The SELECT columns.</param>
    /// <param name="layout">The table's layout.</param>
    /// <param name="table">The table name.</param>
    /// <param name="alias">The table's alias (or its name).</param>
    /// <returns>The projection; a <c>*</c> (or an empty list) projects every catalog column.</returns>
    public static ReactiveProjection Project(IReadOnlyList<SelectColumnNode> columns, ReactiveRowLayout layout, string table, string alias)
    {
        var names = new List<string>();
        var ordinals = new List<int>();
        foreach (SelectColumnNode column in columns)
        {
            if (IsStarColumn(column))
            {
                for (int i = 0; i < layout.Count; i++)
                {
                    names.Add(layout.Columns[i]);
                    ordinals.Add(i);
                }

                continue;
            }

            (string? qualifier, string source) = ColumnReference(column);
            names.Add(column.Alias ?? source);
            ordinals.Add(RequireOrdinal(layout, table, alias, qualifier, source));
        }

        if (names.Count == 0)
        {
            return new ReactiveProjection(layout.Schema, Enumerable.Range(0, layout.Count).ToArray());
        }

        return new ReactiveProjection(new ReactiveRowSchema(names), [.. ordinals]);
    }

    /// <summary>Returns whether a SELECT column is <c>*</c> or <c>alias.*</c>.</summary>
    /// <param name="column">The SELECT column.</param>
    /// <returns><see langword="true"/> for a wildcard column.</returns>
    public static bool IsStarColumn(SelectColumnNode column)
    {
        string raw = column.RawExpression.Trim();
        return raw == "*" || (column.Expression is null && raw.EndsWith("*", StringComparison.Ordinal));
    }

    /// <summary>Resolves a plain SELECT column to its (qualifier, column name).</summary>
    /// <param name="column">The SELECT column.</param>
    /// <returns>The qualifier (or <c>null</c>) and the bare column name.</returns>
    public static (string? Qualifier, string Column) ColumnReference(SelectColumnNode column) => column.Expression switch
    {
        ColumnRefNode columnRef => (string.IsNullOrWhiteSpace(columnRef.TableOrAlias) ? null : columnRef.TableOrAlias, columnRef.Column),
        ResolvedColumnRefNode resolved => (string.IsNullOrWhiteSpace(resolved.TableName) ? null : resolved.TableName, resolved.Column),
        _ => ReactiveGroupAggregator.SplitQualified(column.RawExpression.Trim()),
    };

    /// <summary>Returns the output name of a plain SELECT column: its alias, or the bare column name.</summary>
    /// <param name="column">The SELECT column.</param>
    /// <returns>The output name.</returns>
    public static string OutputName(SelectColumnNode column) => column.Alias ?? ColumnReference(column).Column;
}
