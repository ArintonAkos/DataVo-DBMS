using System.Globalization;
using System.Runtime.InteropServices;
using DataVo.Core.Parser.Utils;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A typed row identity or join key made of one or more <see cref="CellValue"/> components, used as the key of
/// reactive arrangements in place of rendered strings.
/// </summary>
/// <remarks>
/// A single-column key is stored inline, so building and probing it allocates nothing; only composite keys
/// keep their extra components in a small array. Equality follows SQL value equality
/// (<see cref="CellValueEqualityComparer"/>), and the hash normalizes numeric widths so an <c>INT</c> 5, a
/// <c>BIGINT</c> 5 and a <c>DOUBLE</c> 5.0 land in the same bucket.
/// </remarks>
/// <example>
/// <code>
/// PackedKey id = PackedKey.From(row.Cells, primaryKeyOrdinals);
/// </code>
/// </example>
internal readonly struct PackedKey : IEquatable<PackedKey>
{
    private readonly CellValue _first;
    private readonly CellValue[]? _rest;
    private readonly int _hash;

    private PackedKey(CellValue first, CellValue[]? rest, int hash)
    {
        _first = first;
        _rest = rest;
        _hash = hash;
    }

    /// <summary>Gets whether any component is SQL <c>NULL</c> (an equi-join never matches such a key).</summary>
    public bool HasNull
    {
        get
        {
            if (_first.IsNull)
            {
                return true;
            }

            if (_rest is not null)
            {
                foreach (CellValue cell in _rest)
                {
                    if (cell.IsNull)
                    {
                        return true;
                    }
                }
            }

            return false;
        }
    }

    /// <summary>Builds a key from the cells at <paramref name="ordinals"/> of a stored row.</summary>
    /// <param name="cells">The stored row cells.</param>
    /// <param name="ordinals">The ordinals of the key columns, in key order (at least one).</param>
    /// <returns>The key.</returns>
    public static PackedKey From(ReadOnlySpan<CellValue> cells, int[] ordinals)
    {
        CellValue first = cells[ordinals[0]];
        int hash = HashCell(first);
        if (ordinals.Length == 1)
        {
            return new PackedKey(first, null, hash | 1);
        }

        var rest = new CellValue[ordinals.Length - 1];
        for (int i = 1; i < ordinals.Length; i++)
        {
            rest[i - 1] = cells[ordinals[i]];
            hash = unchecked((hash * 31) + HashCell(rest[i - 1]));
        }

        // The low bit is forced on so a real key never shares the default key's zero hash.
        return new PackedKey(first, rest, hash | 1);
    }

    /// <inheritdoc />
    public bool Equals(PackedKey other)
    {
        if (_hash != other._hash || (_rest is null) != (other._rest is null))
        {
            return false;
        }

        if (!CellValueEqualityComparer.AreEqual(_first, other._first))
        {
            return false;
        }

        if (_rest is null)
        {
            return true;
        }

        if (_rest.Length != other._rest!.Length)
        {
            return false;
        }

        for (int i = 0; i < _rest.Length; i++)
        {
            if (!CellValueEqualityComparer.AreEqual(_rest[i], other._rest[i]))
            {
                return false;
            }
        }

        return true;
    }

    /// <inheritdoc />
    public override bool Equals(object? obj) => obj is PackedKey other && Equals(other);

    /// <inheritdoc />
    public override int GetHashCode() => _hash;

    /// <summary>Orders two keys component by component with <see cref="ReactiveCellOrdering"/>.</summary>
    /// <param name="other">The key to compare with.</param>
    /// <returns>A negative, zero or positive value, like <see cref="IComparer{T}.Compare"/>.</returns>
    public int CompareTo(PackedKey other)
    {
        int comparison = ReactiveCellOrdering.Compare(_first, other._first);
        if (comparison != 0)
        {
            return comparison;
        }

        int leftCount = _rest?.Length ?? 0;
        int rightCount = other._rest?.Length ?? 0;
        int shared = Math.Min(leftCount, rightCount);
        for (int i = 0; i < shared; i++)
        {
            comparison = ReactiveCellOrdering.Compare(_rest![i], other._rest![i]);
            if (comparison != 0)
            {
                return comparison;
            }
        }

        return leftCount.CompareTo(rightCount);
    }

    /// <summary>
    /// Renders the key in the length-prefixed text form exposed by the join's hidden <c>__rid</c>/<c>__sid</c>
    /// identity columns: <c>v&lt;len&gt;:&lt;text&gt;</c> per component, or <c>n</c> for <c>NULL</c>.
    /// </summary>
    /// <returns>The rendered identity text.</returns>
    public string Render()
    {
        var builder = new System.Text.StringBuilder();
        AppendComponent(builder, _first);
        if (_rest is not null)
        {
            foreach (CellValue cell in _rest)
            {
                AppendComponent(builder, cell);
            }
        }

        return builder.ToString();
    }

    private static void AppendComponent(System.Text.StringBuilder builder, CellValue cell)
    {
        string? text = cell.IsNull ? null : Convert.ToString(cell.ToObject(), CultureInfo.InvariantCulture);
        if (text is null)
        {
            builder.Append('n');
            return;
        }

        builder.Append('v')
            .Append(text.Length.ToString(CultureInfo.InvariantCulture))
            .Append(':')
            .Append(text);
    }

    // Hash consistent with CellValueEqualityComparer.AreEqual across numeric widths: integral values hash as
    // a long whatever their storage type; other doubles and decimals hash as a double.
    private static int HashCell(CellValue cell)
    {
        switch (cell.Type)
        {
            case CellType.Double:
            {
                double value = cell.AsDouble();
                return IsIntegral(value) ? ((long)value).GetHashCode() : value.GetHashCode();
            }

            case CellType.Decimal:
            {
                decimal value = cell.AsDecimal();
                return value == decimal.Truncate(value) && value >= long.MinValue && value <= long.MaxValue
                    ? ((long)value).GetHashCode()
                    : ((double)value).GetHashCode();
            }

            default:
                return CellValueEqualityComparer.Hash(cell);
        }
    }

    private static bool IsIntegral(double value) =>
        value == Math.Floor(value) && value >= long.MinValue && value < long.MaxValue;
}

/// <summary>
/// The ordering used by typed reactive arrangements. It matches the engine's <c>ORDER BY</c> comparison
/// (<see cref="ExpressionValueComparer.Compare"/>) without boxing the common scalar types.
/// </summary>
/// <remarks>
/// Text cells go through <see cref="ExpressionValueComparer"/> itself (strings are references, so nothing is
/// boxed), which keeps its numeric-aware text ordering; every other pair uses <see cref="CellValueComparer"/>.
/// </remarks>
internal static class ReactiveCellOrdering
{
    /// <summary>Compares two cells with SQL <c>ORDER BY</c> semantics (<c>NULL</c> lowest).</summary>
    /// <param name="left">The left cell.</param>
    /// <param name="right">The right cell.</param>
    /// <returns>A negative, zero or positive value.</returns>
    public static int Compare(CellValue left, CellValue right)
    {
        if (left.Type == CellType.String || right.Type == CellType.String)
        {
            return ExpressionValueComparer.Compare(AsComparable(left), AsComparable(right), trimQuotedStrings: true);
        }

        return CellValueComparer.Compare(left, right);
    }

    /// <summary>Returns whether two cell spans hold equal values, column by column.</summary>
    /// <param name="left">The first row's cells.</param>
    /// <param name="right">The second row's cells.</param>
    /// <returns><see langword="true"/> when every pair of cells compares equal.</returns>
    public static bool RowsEqual(ReadOnlySpan<CellValue> left, ReadOnlySpan<CellValue> right)
    {
        if (left.Length != right.Length)
        {
            return false;
        }

        for (int i = 0; i < left.Length; i++)
        {
            if (Compare(left[i], right[i]) != 0)
            {
                return false;
            }
        }

        return true;
    }

    private static object? AsComparable(CellValue cell) =>
        cell.Type == CellType.String ? cell.AsString() : cell.ToObject();
}

/// <summary>
/// A fixed column layout for rows kept inside a reactive arrangement, and the loader that copies a change
/// image (a typed <see cref="Changes.TypedRow"/>) into it as unboxed <see cref="CellValue"/> cells.
/// </summary>
/// <remarks>
/// Column names resolve case-insensitively; a column the image does not carry loads as <c>NULL</c>. For typed
/// images the source-to-layout ordinal map is cached per source schema, so steady-state loads do no name
/// lookups. Not thread-safe: an operator's dispatch runs on one thread at a time.
/// </remarks>
/// <example>
/// <code>
/// var layout = new ReactiveRowLayout(["Id", "Score"]);
/// CellValue[] cells = new CellValue[layout.Count];
/// if (layout.TryLoadAfter(change, cells)) { /* read cells */ }
/// </code>
/// </example>
internal sealed class ReactiveRowLayout
{
    private readonly string[] _columns;
    private readonly Dictionary<string, int> _ordinals;
    private ReactiveRowSchema? _cachedSchema;
    private int[] _cachedMap = [];
    private bool _cachedIsIdentity;

    /// <summary>Creates a layout over the given distinct column names, in storage order.</summary>
    /// <param name="columns">The column names.</param>
    public ReactiveRowLayout(IReadOnlyList<string> columns)
    {
        _columns = columns.ToArray();
        _ordinals = new Dictionary<string, int>(_columns.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < _columns.Length; i++)
        {
            _ordinals.TryAdd(_columns[i], i);
        }

        Schema = new ReactiveRowSchema(_columns);
    }

    /// <summary>Gets the number of stored columns.</summary>
    public int Count => _columns.Length;

    /// <summary>Gets the stored column names, in storage order.</summary>
    public IReadOnlyList<string> Columns => _columns;

    /// <summary>Gets a schema over the stored columns, for reading a stored row as a <see cref="RowRef"/>.</summary>
    public ReactiveRowSchema Schema { get; }

    /// <summary>Resolves a column name to its stored ordinal, or -1 when the layout does not hold it.</summary>
    /// <param name="column">The column name (case-insensitive).</param>
    /// <returns>The ordinal, or -1.</returns>
    public int OrdinalOf(string column) => _ordinals.TryGetValue(column, out int ordinal) ? ordinal : -1;

    /// <summary>Copies a change's before-image into <paramref name="destination"/> in layout order.</summary>
    /// <param name="change">The change.</param>
    /// <param name="destination">The destination cells; its length must equal <see cref="Count"/>.</param>
    /// <returns><see langword="false"/> when the change carries no before-image (an insert).</returns>
    public bool TryLoadBefore(Changes.RowChange change, Span<CellValue> destination) =>
        change.Kind != Changes.ChangeKind.Insert && TryLoad(change.TypedBefore, destination);

    /// <summary>Copies a change's after-image into <paramref name="destination"/> in layout order.</summary>
    /// <param name="change">The change.</param>
    /// <param name="destination">The destination cells; its length must equal <see cref="Count"/>.</param>
    /// <returns><see langword="false"/> when the change carries no after-image (a delete).</returns>
    public bool TryLoadAfter(Changes.RowChange change, Span<CellValue> destination) =>
        change.Kind != Changes.ChangeKind.Delete && TryLoad(change.TypedAfter, destination);

    /// <summary>Copies a typed image into <paramref name="destination"/> in layout order.</summary>
    /// <param name="row">The image, or <c>null</c>.</param>
    /// <param name="destination">The destination cells; its length must equal <see cref="Count"/>.</param>
    /// <returns><see langword="false"/> when <paramref name="row"/> is <c>null</c>.</returns>
    public bool TryLoad(Changes.TypedRow? row, Span<CellValue> destination)
    {
        if (row is not { } image)
        {
            return false;
        }

        Load(image.AsRowRef(), image.Schema, destination);
        return true;
    }

    /// <summary>
    /// Returns a typed image's cells in layout order: the image's own array when its columns are exactly this
    /// layout, otherwise a fresh copy. The result may be kept; change capture never mutates a captured image.
    /// </summary>
    /// <param name="row">The image.</param>
    /// <returns>The cells in layout order.</returns>
    public CellValue[] AdoptOrCopy(Changes.TypedRow row)
    {
        if (TryAdopt(row, out CellValue[] cells))
        {
            return cells;
        }

        cells = new CellValue[_columns.Length];
        Load(row.AsRowRef(), row.Schema, cells);
        return cells;
    }

    /// <summary>Copies a typed image into <paramref name="destination"/> in layout order.</summary>
    /// <param name="row">The source row.</param>
    /// <param name="schema">The source row's schema (used to cache the ordinal map).</param>
    /// <param name="destination">The destination cells; its length must equal <see cref="Count"/>.</param>
    public void Load(RowRef row, ReactiveRowSchema schema, Span<CellValue> destination)
    {
        int[] map = MapFor(schema);
        ReadOnlySpan<CellValue> source = row.Values;
        for (int i = 0; i < map.Length; i++)
        {
            int ordinal = map[i];
            destination[i] = ordinal >= 0 ? source[ordinal] : CellValue.Null;
        }
    }

    /// <summary>
    /// Returns the typed image's own cell array when its columns are exactly this layout, so the arrangement
    /// can keep it without copying. Change capture never mutates a captured typed image.
    /// </summary>
    /// <param name="row">The typed image.</param>
    /// <param name="cells">The shareable cell array, when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the image can be adopted as-is.</returns>
    public bool TryAdopt(Changes.TypedRow row, out CellValue[] cells)
    {
        MapFor(row.Schema);
        if (_cachedIsIdentity
            && MemoryMarshal.TryGetArray(row.Cells, out ArraySegment<CellValue> segment)
            && segment.Offset == 0
            && segment.Count == segment.Array!.Length)
        {
            cells = segment.Array;
            return true;
        }

        cells = [];
        return false;
    }

    private int[] MapFor(ReactiveRowSchema schema)
    {
        if (ReferenceEquals(schema, _cachedSchema))
        {
            return _cachedMap;
        }

        var map = new int[_columns.Length];
        bool identity = schema.ColumnCount == _columns.Length;
        for (int i = 0; i < _columns.Length; i++)
        {
            map[i] = schema.TryGetOrdinal(_columns[i], out int ordinal) ? ordinal : -1;
            identity &= map[i] == i;
        }

        _cachedSchema = schema;
        _cachedMap = map;
        _cachedIsIdentity = identity;
        return map;
    }
}
