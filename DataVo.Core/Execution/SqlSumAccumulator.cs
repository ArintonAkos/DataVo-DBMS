using System.Globalization;

namespace DataVo.Core.Execution;

/// <summary>
/// SQL <c>SUM</c>: the sum of integer values is an integer (a <see cref="long"/>), and it becomes a
/// <see cref="double"/> once a non-integer value is added or the integer sum overflows. The sum of no
/// values is NULL. Every aggregate path uses this, so they agree on the result type.
/// </summary>
internal struct SqlSumAccumulator
{
    private long _integer;
    private double _real;
    private bool _isReal;

    /// <summary>The number of non-null values added.</summary>
    public long Count { get; private set; }

    public void Add(long value)
    {
        _real += value;
        Count++;
        if (_isReal)
        {
            return;
        }

        long sum = unchecked(_integer + value);
        if (((_integer ^ sum) & (value ^ sum)) < 0)
        {
            _isReal = true; // overflow: keep the floating-point running sum
            return;
        }

        _integer = sum;
    }

    public void Add(double value)
    {
        _real += value;
        _isReal = true;
        Count++;
    }

    /// <summary>Adds a boxed non-null value: integers stay exact, anything else is converted to a double.</summary>
    public void Add(object value)
    {
        switch (value)
        {
            case int i: Add((long)i); break;
            case long l: Add(l); break;
            case short s: Add((long)s); break;
            case byte b: Add((long)b); break;
            case bool flag: Add(flag ? 1L : 0L); break;
            default: Add(Convert.ToDouble(value, CultureInfo.InvariantCulture)); break;
        }
    }

    public void Merge(SqlSumAccumulator other)
    {
        if (other.Count == 0)
        {
            return;
        }

        if (!other._isReal)
        {
            long count = Count;
            double real = _real;
            Add(other._integer); // exact integer merge, switching to real on overflow
            Count = count + other.Count;
            _real = real + other._real;
            return;
        }

        _real += other._real;
        _isReal = true;
        Count += other.Count;
    }

    /// <summary>NULL over no values; otherwise a <see cref="long"/> for an all-integer sum, else a <see cref="double"/>.</summary>
    public readonly object? Result => Count == 0 ? null : _isReal ? (object)_real : _integer;

    /// <summary>The running sum as a double (for AVG).</summary>
    public readonly double Real => _real;
}
