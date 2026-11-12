using System.ComponentModel;

namespace DataVo.Core;

/// <summary>
/// How a compiled condition compares a column with a value. Used by the code <c>DataVo.Generators</c> writes;
/// application code writes the comparison in a query's lambda instead.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public enum ColumnarTest : byte
{
    /// <summary><c>column == value</c>.</summary>
    Equal,

    /// <summary><c>column != value</c>.</summary>
    NotEqual,

    /// <summary><c>column &lt; value</c>.</summary>
    Less,

    /// <summary><c>column &lt;= value</c>.</summary>
    LessOrEqual,

    /// <summary><c>column &gt; value</c>.</summary>
    Greater,

    /// <summary><c>column &gt;= value</c>.</summary>
    GreaterOrEqual,
}

/// <summary>
/// A compiled condition that can also describe itself as comparisons of columns with values, so a table with a
/// columnar index tests whole runs of a column at once instead of one row at a time. Implemented by the code
/// <c>DataVo.Generators</c> writes; application code does not implement this interface.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public interface IColumnarFilter
{
    /// <summary>
    /// Writes the condition in postfix order: each comparison or null test, then <c>And</c> or <c>Or</c> after
    /// the two operands it joins.
    /// </summary>
    /// <param name="builder">Receives the steps.</param>
    void Describe(ref ColumnarPredicateBuilder builder);
}

/// <summary>One step of a condition in postfix order.</summary>
internal struct ColumnarStep
{
    /// <summary>Compares a column with <see cref="Bits"/>.</summary>
    public const byte Compare = 0;

    /// <summary>Tests a column for NULL (<see cref="Test"/> is <see cref="ColumnarTest.Equal"/>) or for a value.</summary>
    public const byte NullTest = 1;

    /// <summary>Joins the two results before it with AND.</summary>
    public const byte And = 2;

    /// <summary>Joins the two results before it with OR.</summary>
    public const byte Or = 3;

    /// <summary>What the step does: one of the constants above.</summary>
    public byte Kind;

    /// <summary>The comparison.</summary>
    public ColumnarTest Test;

    /// <summary>The type of the value: <c>i l f d b</c> for int, long, float, double and bool.</summary>
    public char ValueKind;

    /// <summary>Whether a NULL in the column meets the comparison, as it does for C#'s <c>!=</c> and never does in SQL.</summary>
    public bool NullsMatch;

    /// <summary>The column: its table position until the scan binds the step, then its slot in the index.</summary>
    public int Column;

    /// <summary>The value's bits.</summary>
    public long Bits;
}

/// <summary>
/// Collects the steps of a compiled condition. Used by the code <c>DataVo.Generators</c> writes.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public ref struct ColumnarPredicateBuilder
{
    private readonly Span<ColumnarStep> _steps;
    private readonly ReadOnlySpan<int> _ordinals;
    private int _count;
    private bool _failed;

    internal ColumnarPredicateBuilder(Span<ColumnarStep> steps, ReadOnlySpan<int> ordinals)
    {
        _steps = steps;
        _ordinals = ordinals;
        _count = 0;
        _failed = false;
    }

    /// <summary>The steps written, or an empty span with <paramref name="complete"/> false when the condition cannot be described.</summary>
    internal readonly ReadOnlySpan<ColumnarStep> Steps(out bool complete)
    {
        complete = !_failed;
        return _failed ? default : _steps.Slice(0, _count);
    }

    /// <summary>Compares an <c>INT</c> column with a value.</summary>
    /// <param name="column">The column's position among the condition's columns.</param>
    /// <param name="test">The comparison.</param>
    /// <param name="value">The value.</param>
    public void Compare(int column, ColumnarTest test, int value) => Add(column, test, 'i', value);

    /// <summary>Compares a <c>BIGINT</c> column with a value.</summary>
    /// <inheritdoc cref="Compare(int, ColumnarTest, int)"/>
    public void Compare(int column, ColumnarTest test, long value) => Add(column, test, 'l', value);

    /// <summary>Compares a <c>FLOAT</c> column with a value.</summary>
    /// <inheritdoc cref="Compare(int, ColumnarTest, int)"/>
    public void Compare(int column, ColumnarTest test, float value) => Add(column, test, 'f', BitConverter.SingleToInt32Bits(value));

    /// <summary>Compares a <c>DOUBLE</c> column with a value.</summary>
    /// <inheritdoc cref="Compare(int, ColumnarTest, int)"/>
    public void Compare(int column, ColumnarTest test, double value) => Add(column, test, 'd', BitConverter.DoubleToInt64Bits(value));

    /// <summary>Compares a <c>BIT</c> column with a value.</summary>
    /// <inheritdoc cref="Compare(int, ColumnarTest, int)"/>
    public void Compare(int column, ColumnarTest test, bool value) => Add(column, test, 'b', value ? 1 : 0);

    /// <summary>Records a comparison with a value of a type that has no columnar form; the query then tests rows.</summary>
    /// <inheritdoc cref="Compare(int, ColumnarTest, int)"/>
    public void Compare(int column, ColumnarTest test, Guid value) => _failed = true;

    /// <summary>Tests a column for NULL.</summary>
    /// <param name="column">The column's position among the condition's columns.</param>
    /// <param name="isNull">True to accept rows where the column is NULL, false to accept the others.</param>
    public void IsNull(int column, bool isNull)
    {
        if (Next(column, out int ordinal))
        {
            _steps[_count++] = new ColumnarStep
            {
                Kind = ColumnarStep.NullTest,
                Test = isNull ? ColumnarTest.Equal : ColumnarTest.NotEqual,
                Column = ordinal,
            };
        }
    }

    /// <summary>Joins the two results before this step with AND.</summary>
    public void And() => Join(ColumnarStep.And);

    /// <summary>Joins the two results before this step with OR.</summary>
    public void Or() => Join(ColumnarStep.Or);

    private void Add(int column, ColumnarTest test, char valueKind, long bits)
    {
        if (Next(column, out int ordinal))
        {
            _steps[_count++] = new ColumnarStep
            {
                Kind = ColumnarStep.Compare,
                Test = test,
                ValueKind = valueKind,
                NullsMatch = test == ColumnarTest.NotEqual, // C#: a missing value differs from every value
                Column = ordinal,
                Bits = bits,
            };
        }
    }

    private void Join(byte kind)
    {
        if (_count >= _steps.Length)
        {
            _failed = true;
        }
        else if (!_failed)
        {
            _steps[_count++] = new ColumnarStep { Kind = kind };
        }
    }

    private bool Next(int column, out int ordinal)
    {
        if (_failed || _count >= _steps.Length || (uint)column >= (uint)_ordinals.Length)
        {
            _failed = true;
            ordinal = -1;
            return false;
        }

        ordinal = _ordinals[column];
        return true;
    }
}
