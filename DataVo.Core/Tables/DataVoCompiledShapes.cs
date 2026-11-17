using System.ComponentModel;

namespace DataVo.Core;

/// <summary>
/// A sort key that <c>DataVo.Generators</c> compiled: one column of the model, sorted up or down. Generated code
/// creates one per <c>OrderBy</c> or <c>ThenBy</c> call site; application code does not use this type.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class DataVoOrderKey
{
    /// <summary>Describes a compiled sort key.</summary>
    /// <param name="column">The column rows are sorted by.</param>
    /// <param name="descending">Whether rows are sorted from largest to smallest.</param>
    public DataVoOrderKey(string column, bool descending)
    {
        Column = column ?? throw new ArgumentNullException(nameof(column));
        Descending = descending;
        Sql = descending ? column + " DESC" : column;
    }

    /// <summary>Gets the column rows are sorted by.</summary>
    public string Column { get; }

    /// <summary>Gets whether rows are sorted from largest to smallest.</summary>
    public bool Descending { get; }

    /// <summary>Gets the key as an ORDER BY entry.</summary>
    internal string Sql { get; }
}

/// <summary>
/// A <c>Select</c> projection that <c>DataVo.Generators</c> compiled: the columns it reads, and the code that builds
/// its result from those columns alone, without building the model. Generated code creates one per call site.
/// </summary>
/// <typeparam name="T">The model type.</typeparam>
/// <typeparam name="TResult">The type of the projected value.</typeparam>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class DataVoProjection<T, TResult>
{
    /// <summary>Describes a compiled projection.</summary>
    /// <param name="columns">The columns the projection reads; at least one.</param>
    /// <param name="read">Builds the projected value from a row holding <paramref name="columns"/>.</param>
    /// <exception cref="ArgumentException"><paramref name="columns"/> is empty.</exception>
    public DataVoProjection(string[] columns, Func<DataVoRow, TResult> read)
    {
        if (columns is null || columns.Length == 0)
        {
            throw new ArgumentException("A projection reads at least one column.", nameof(columns));
        }

        Columns = columns;
        ColumnList = string.Join(", ", columns);
        Read = read ?? throw new ArgumentNullException(nameof(read));
    }

    /// <summary>Gets the columns the projection reads.</summary>
    public IReadOnlyList<string> Columns { get; }

    /// <summary>Gets the columns as a SELECT list.</summary>
    internal string ColumnList { get; }

    /// <summary>Gets the code that builds the projected value from a row.</summary>
    internal Func<DataVoRow, TResult> Read { get; }
}

/// <summary>
/// Describes the columns an <c>ExecuteUpdate</c> call sets. Write it as a lambda that chains <see cref="Set{TValue}(Func{T, TValue}, TValue)"/>
/// calls; <c>DataVo.Generators</c> reads the lambda at build time and turns it into the SET list of one
/// <c>UPDATE</c> statement. The lambda itself is never run.
/// </summary>
/// <typeparam name="T">The model type.</typeparam>
/// <example>
/// <code>
/// int changed = db.Table&lt;Player&gt;()
///     .Where(10, static (p, min) => p.Level >= min)
///     .ExecuteUpdate(100L, static (set, bonus) => set
///         .Set(p => p.Gold, p => p.Gold + bonus)
///         .SetNull(p => p.Guild));
/// </code>
/// </example>
public sealed class DataVoSetters<T>
{
    private DataVoSetters()
    {
    }

    /// <summary>Sets a column to a value.</summary>
    /// <typeparam name="TValue">The column's type.</typeparam>
    /// <param name="column">The column, as <c>p =&gt; p.Column</c>.</param>
    /// <param name="value">The new value: a literal, the state value or a field or property of it, or a static member.</param>
    /// <returns>The same setters, to chain another call.</returns>
    /// <exception cref="NotSupportedException">Always, when called: setters only describe a compiled update.</exception>
    public DataVoSetters<T> Set<TValue>(Func<T, TValue> column, TValue value) => throw NotRun();

    /// <summary>Sets a column to a value computed from the row's own columns, such as <c>p =&gt; p.Gold + bonus</c>.</summary>
    /// <typeparam name="TValue">The column's type.</typeparam>
    /// <param name="column">The column, as <c>p =&gt; p.Column</c>.</param>
    /// <param name="value">The new value: columns and values joined by <c>+</c>, <c>-</c> and <c>*</c>.</param>
    /// <returns>The same setters, to chain another call.</returns>
    /// <exception cref="NotSupportedException">Always, when called: setters only describe a compiled update.</exception>
    public DataVoSetters<T> Set<TValue>(Func<T, TValue> column, Func<T, TValue> value) => throw NotRun();

    /// <summary>
    /// Sets a nullable column to null. <c>Set(p =&gt; p.Column, null)</c> cannot choose between the two
    /// <c>Set</c> overloads, so a literal null is written with this method.
    /// </summary>
    /// <typeparam name="TValue">The column's type.</typeparam>
    /// <param name="column">The column, as <c>p =&gt; p.Column</c>.</param>
    /// <returns>The same setters, to chain another call.</returns>
    /// <exception cref="NotSupportedException">Always, when called: setters only describe a compiled update.</exception>
    public DataVoSetters<T> SetNull<TValue>(Func<T, TValue> column) => throw NotRun();

    private static NotSupportedException NotRun() =>
        new("DataVoSetters only describe an ExecuteUpdate call; DataVo.Generators compiles them at build time and they are never run.");
}
