namespace DataVo.Core;

/// <summary>
/// Runs a filtered query with the ordering, paging and projection added to it. Each call decides once whether SQL
/// answers the whole query: the filter must bind, every sort key must be a compiled column, and a projection that
/// is not compiled reads models instead of its columns. Anything else runs the same LINQ operators in memory over
/// the filter's rows, so both routes return the same rows in the same order.
/// </summary>
internal static class DataVoComposed
{
    /// <summary>Reads the models the query returns.</summary>
    /// <param name="source">The filtered query.</param>
    /// <param name="shape">The ordering and paging.</param>
    /// <param name="cap">The most rows the caller reads (1 for <c>FirstOrDefault</c>), or -1 for all.</param>
    /// <returns>The models, in order.</returns>
    public static IEnumerable<T> ReadModels<T, TState>(DataVoQuery<T, TState> source, DataVoQueryShape<T> shape, long cap)
    {
        if (shape.IsEmpty || cap == 0)
        {
            return [];
        }

        DataVoTable<T> table = source.Table;
        if (TryQuery(source, shape, table.ModelColumns, cap) is { } rows)
        {
            return Map(rows, table.RowReader);
        }

        IEnumerable<T> models = shape.Apply(source);
        return cap < 0 ? models : models.Take((int)cap);
    }

    /// <summary>
    /// Reads the projected values the query returns. A compiled projection selects only its columns and builds
    /// no model; otherwise models are read and <paramref name="selector"/> projects them.
    /// </summary>
    /// <param name="source">The filtered query.</param>
    /// <param name="shape">The ordering and paging.</param>
    /// <param name="projection">The compiled projection, or <see langword="null"/>.</param>
    /// <param name="selector">The original selector.</param>
    /// <param name="cap">The most rows the caller reads, or -1 for all.</param>
    /// <returns>The projected values, in order.</returns>
    public static IEnumerable<TResult> ReadProjected<T, TState, TResult>(
        DataVoQuery<T, TState> source,
        DataVoQueryShape<T> shape,
        DataVoProjection<T, TResult>? projection,
        Func<T, TResult> selector,
        long cap)
    {
        if (projection is not null && !shape.IsEmpty && cap != 0 && TryQuery(source, shape, projection.ColumnList, cap) is { } rows)
        {
            return Map(rows, projection.Read);
        }

        return ReadModels(source, shape, cap).Select(selector);
    }

    /// <summary>Gets the statement the query runs, or <see langword="null"/> when it runs in memory.</summary>
    /// <param name="source">The filtered query.</param>
    /// <param name="shape">The ordering and paging.</param>
    /// <param name="columns">The SELECT list the query reads.</param>
    /// <returns>The SQL text, with its parameters as <c>@name</c>.</returns>
    public static string? Sql<T, TState>(DataVoQuery<T, TState> source, DataVoQueryShape<T> shape, string columns) =>
        shape.IsOrderCompiled && source.TryBind(out string? where, out _)
            ? DataVoSql.Select(columns, source.Table.TableName, where, shape.OrderBy, shape.Take >= 0, shape.Skip > 0)
            : null;

    /// <summary>Counts the rows the query returns. Ordering and projection do not change the count, so neither is run.</summary>
    /// <param name="source">The filtered query.</param>
    /// <param name="shape">The ordering and paging.</param>
    /// <returns>The number of rows.</returns>
    public static int Count<T, TState>(DataVoQuery<T, TState> source, DataVoQueryShape<T> shape)
    {
        if (shape.IsEmpty)
        {
            return 0;
        }

        long needed = shape.Take < 0 ? 0 : shape.Skip + shape.Take;
        return shape.CountOf(source.CountUpTo(needed > int.MaxValue ? 0 : (int)needed));
    }

    /// <summary>Gets whether the query returns any row, counting at most one row past the skipped ones.</summary>
    /// <param name="source">The filtered query.</param>
    /// <param name="shape">The ordering and paging.</param>
    /// <returns><see langword="true"/> when at least one row is returned.</returns>
    public static bool Any<T, TState>(DataVoQuery<T, TState> source, DataVoQueryShape<T> shape)
    {
        if (shape.IsEmpty)
        {
            return false;
        }

        long needed = shape.Skip + 1;
        return source.CountUpTo(needed > int.MaxValue ? 0 : (int)needed) > shape.Skip;
    }

    /// <summary>Returns the first value, or the default value when there is none.</summary>
    /// <param name="rows">The values, read with a cap of 1.</param>
    /// <returns>The first value, or the default value.</returns>
    public static TResult? First<TResult>(IEnumerable<TResult> rows)
    {
        foreach (TResult row in rows)
        {
            return row;
        }

        return default;
    }

    /// <summary>Returns the only value, or the default value when there is none.</summary>
    /// <param name="rows">The values, read with a cap of 2.</param>
    /// <returns>The only value, or the default value.</returns>
    /// <exception cref="InvalidOperationException">There is more than one value.</exception>
    public static TResult? Single<TResult>(IEnumerable<TResult> rows)
    {
        using IEnumerator<TResult> values = rows.GetEnumerator();
        if (!values.MoveNext())
        {
            return default;
        }

        TResult only = values.Current;
        return values.MoveNext() ? throw new InvalidOperationException("Sequence contains more than one element") : only;
    }

    /// <summary>Runs the query as SQL when the filter binds and every sort key is compiled; null otherwise.</summary>
    private static IEnumerable<DataVoRow>? TryQuery<T, TState>(DataVoQuery<T, TState> source, DataVoQueryShape<T> shape, string columns, long cap)
    {
        if (!shape.IsOrderCompiled || !source.TryBind(out string? where, out DataVoQueryParameter[] parameters))
        {
            return null;
        }

        if (shape.Skip > int.MaxValue)
        {
            return []; // OFFSET is a 32-bit count, and no table holds more rows than it can skip
        }

        return source.Table.Query(columns, where, parameters, shape.OrderBy, shape.Skip, shape.LimitFor(cap));
    }

    private static IEnumerable<TResult> Map<TResult>(IEnumerable<DataVoRow> rows, Func<DataVoRow, TResult> read)
    {
        foreach (DataVoRow row in rows)
        {
            yield return read(row);
        }
    }
}
