namespace DataVo.Core;

/// <summary>
/// One sort key of an ordered table query: the delegate that orders rows in memory, and the column that orders
/// them in SQL when <c>DataVo.Generators</c> compiled the key.
/// </summary>
/// <typeparam name="T">The model type.</typeparam>
internal abstract class DataVoSortKey<T>(DataVoOrderKey? column, bool descending)
{
    /// <summary>Gets the compiled column, or <see langword="null"/> when the key can only sort in memory.</summary>
    public DataVoOrderKey? Column { get; } = column;

    /// <summary>Gets whether the key sorts from largest to smallest.</summary>
    public bool Descending { get; } = descending;

    /// <summary>Sorts rows by this key, as <c>Enumerable.OrderBy</c> or <c>OrderByDescending</c> would.</summary>
    /// <param name="rows">The rows to sort.</param>
    /// <returns>The sorted rows.</returns>
    public abstract IOrderedEnumerable<T> First(IEnumerable<T> rows);

    /// <summary>Breaks ties of an earlier key by this one, as <c>Enumerable.ThenBy</c> or <c>ThenByDescending</c> would.</summary>
    /// <param name="rows">The rows sorted by the earlier keys.</param>
    /// <returns>The sorted rows.</returns>
    public abstract IOrderedEnumerable<T> Then(IOrderedEnumerable<T> rows);
}

/// <summary>A sort key over a value of type <typeparamref name="TKey"/>.</summary>
/// <typeparam name="T">The model type.</typeparam>
/// <typeparam name="TKey">The type of the value rows are sorted by.</typeparam>
internal sealed class DataVoSortKey<T, TKey>(Func<T, TKey> selector, IComparer<TKey>? comparer, bool descending, DataVoOrderKey? column)
    : DataVoSortKey<T>(column, descending)
{
    public override IOrderedEnumerable<T> First(IEnumerable<T> rows) =>
        Descending ? rows.OrderByDescending(selector, comparer) : rows.OrderBy(selector, comparer);

    public override IOrderedEnumerable<T> Then(IOrderedEnumerable<T> rows) =>
        Descending ? rows.ThenByDescending(selector, comparer) : rows.ThenBy(selector, comparer);
}

/// <summary>
/// The ordering and paging a table query adds to its filter: the sort keys of <c>OrderBy</c> and <c>ThenBy</c>,
/// then the rows <c>Skip</c> and <c>Take</c> keep. Every method returns a new shape; a shape never changes.
/// </summary>
/// <remarks>
/// Paging composes as <see cref="Enumerable.Skip{TSource}"/> and <see cref="Enumerable.Take{TSource}(IEnumerable{TSource}, int)"/>
/// do: a count of zero or less skips nothing or takes nothing, and chained calls narrow the same window, so
/// <c>Skip(2).Take(5).Skip(1).Take(10)</c> keeps rows 3 to 6.
/// </remarks>
/// <typeparam name="T">The model type.</typeparam>
internal sealed class DataVoQueryShape<T>
{
    /// <summary>The shape of a query that neither orders nor pages.</summary>
    public static readonly DataVoQueryShape<T> None = new([], 0, -1);

    private string? _orderBy;

    private DataVoQueryShape(DataVoSortKey<T>[] keys, long skip, long take)
    {
        Keys = keys;
        Skip = skip;
        Take = take;
    }

    /// <summary>Gets the sort keys, most significant first.</summary>
    public DataVoSortKey<T>[] Keys { get; }

    /// <summary>Gets how many rows to skip after sorting.</summary>
    public long Skip { get; }

    /// <summary>Gets how many rows to keep after skipping, or -1 for all of them.</summary>
    public long Take { get; }

    /// <summary>Gets whether the query is sorted.</summary>
    public bool IsOrdered => Keys.Length > 0;

    /// <summary>Gets whether the query certainly returns no row, because it takes none.</summary>
    public bool IsEmpty => Take == 0;

    /// <summary>Gets whether every sort key has a compiled column, so SQL can sort.</summary>
    public bool IsOrderCompiled => Array.TrueForAll(Keys, static key => key.Column is not null);

    /// <summary>Gets the ORDER BY list, or <see langword="null"/> when the query is not sorted or SQL cannot sort it.</summary>
    public string? OrderBy => !IsOrdered || !IsOrderCompiled ? null : _orderBy ??= string.Join(", ", Keys.Select(static key => key.Column!.Sql));

    /// <summary>Starts a new ordering. Only valid on a shape that neither orders nor pages.</summary>
    /// <param name="key">The primary sort key.</param>
    /// <returns>The ordered shape.</returns>
    public DataVoQueryShape<T> OrderedBy(DataVoSortKey<T> key) => new([key], 0, -1);

    /// <summary>Adds a key that breaks ties of the existing ones. Only valid before the shape pages.</summary>
    /// <param name="key">The next sort key.</param>
    /// <returns>The ordered shape.</returns>
    public DataVoQueryShape<T> ThenBy(DataVoSortKey<T> key) => new([.. Keys, key], 0, -1);

    /// <summary>Skips rows, as <see cref="Enumerable.Skip{TSource}"/> does.</summary>
    /// <param name="count">The rows to skip; zero or less skips none.</param>
    /// <returns>The paged shape.</returns>
    public DataVoQueryShape<T> Skipping(int count)
    {
        if (count <= 0)
        {
            return this;
        }

        return new(Keys, Skip + count, Take < 0 ? -1 : Math.Max(0, Take - count));
    }

    /// <summary>Keeps at most <paramref name="count"/> rows, as <see cref="Enumerable.Take{TSource}(IEnumerable{TSource}, int)"/> does.</summary>
    /// <param name="count">The rows to keep; zero or less keeps none.</param>
    /// <returns>The paged shape.</returns>
    public DataVoQueryShape<T> Taking(int count)
    {
        long take = Math.Max(0, count);
        return new(Keys, Skip, Take < 0 ? take : Math.Min(Take, take));
    }

    /// <summary>Gets the number of rows the shape keeps from <paramref name="total"/> matching rows.</summary>
    /// <param name="total">The number of rows that match the filter.</param>
    /// <returns>The rows left after paging.</returns>
    public int CountOf(long total)
    {
        long left = Math.Max(0, total - Skip);
        return (int)(Take < 0 ? left : Math.Min(left, Take));
    }

    /// <summary>Sorts and pages rows in memory, with the semantics of the LINQ operators the shape stands for.</summary>
    /// <param name="rows">The rows that match the filter, in storage order.</param>
    /// <returns>The rows the query returns.</returns>
    public IEnumerable<T> Apply(IEnumerable<T> rows)
    {
        if (IsOrdered)
        {
            IOrderedEnumerable<T> ordered = Keys[0].First(rows);
            for (int i = 1; i < Keys.Length; i++)
            {
                ordered = Keys[i].Then(ordered);
            }

            rows = ordered;
        }

        return Page(rows, Skip, Take);
    }

    /// <summary>Gets the most rows a query with this shape can return, capped at <paramref name="cap"/> (-1 for no cap).</summary>
    /// <param name="cap">The most rows the caller reads, such as 1 for <c>FirstOrDefault</c>.</param>
    /// <returns>The row limit, or -1 for none.</returns>
    public long LimitFor(long cap) => Take < 0 ? cap : cap < 0 ? Take : Math.Min(Take, cap);

    /// <summary>Skips and takes rows, counting in 64 bits so very large skips stay exact.</summary>
    private static IEnumerable<T> Page(IEnumerable<T> rows, long skip, long take)
    {
        if (skip == 0 && take < 0)
        {
            return rows;
        }

        return take == 0 ? [] : PageIterator(rows, skip, take);
    }

    private static IEnumerable<T> PageIterator(IEnumerable<T> rows, long skip, long take)
    {
        foreach (T row in rows)
        {
            if (skip > 0)
            {
                skip--;
                continue;
            }

            yield return row;
            if (take > 0 && --take == 0)
            {
                yield break;
            }
        }
    }
}
