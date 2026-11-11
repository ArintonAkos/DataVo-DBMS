using System.Runtime.CompilerServices;
using DataVo.Core.Runtime.Reactive;
using StorageKind = DataVo.Core.StorageEngine.Serialization.RowSerializer.StorageColumnType;

namespace DataVo.Core;

/// <summary>What a scan does with one column of each matching row: the operation and the column's type together.</summary>
internal enum RowFoldKind : byte
{
    /// <summary>The scan only counts.</summary>
    None,

    /// <summary>Adds an <c>INT</c> column to <see cref="RowAggregate.Whole"/>.</summary>
    SumInt32,

    /// <summary>Adds a <c>BIGINT</c> column to <see cref="RowAggregate.Whole"/>.</summary>
    SumInt64,

    /// <summary>Adds a <c>FLOAT</c> column to <see cref="RowAggregate.Real"/>.</summary>
    SumFloat,

    /// <summary>Adds a <c>DOUBLE</c> column to <see cref="RowAggregate.Real"/>.</summary>
    SumDouble,

    /// <summary>Keeps the smallest value of an <c>INT</c> column in <see cref="RowAggregate.Whole"/>.</summary>
    MinInt32,

    /// <summary>Keeps the smallest value of a <c>BIGINT</c> column in <see cref="RowAggregate.Whole"/>.</summary>
    MinInt64,

    /// <summary>Keeps the smallest value of a <c>FLOAT</c> column in <see cref="RowAggregate.Real"/>.</summary>
    MinFloat,

    /// <summary>Keeps the smallest value of a <c>DOUBLE</c> column in <see cref="RowAggregate.Real"/>.</summary>
    MinDouble,

    /// <summary>Keeps the earliest day number of a <c>DATE</c> column in <see cref="RowAggregate.Whole"/>.</summary>
    MinDate,

    /// <summary>Keeps the largest value of an <c>INT</c> column in <see cref="RowAggregate.Whole"/>.</summary>
    MaxInt32,

    /// <summary>Keeps the largest value of a <c>BIGINT</c> column in <see cref="RowAggregate.Whole"/>.</summary>
    MaxInt64,

    /// <summary>Keeps the largest value of a <c>FLOAT</c> column in <see cref="RowAggregate.Real"/>.</summary>
    MaxFloat,

    /// <summary>Keeps the largest value of a <c>DOUBLE</c> column in <see cref="RowAggregate.Real"/>.</summary>
    MaxDouble,

    /// <summary>Keeps the latest day number of a <c>DATE</c> column in <see cref="RowAggregate.Whole"/>.</summary>
    MaxDate,
}

/// <summary>
/// What a compiled scan accumulates over the rows its condition accepts: how many there are, and the sum, the
/// smallest or the largest value of one column. The totals live in this struct inside the scan loop, so nothing
/// is allocated per row.
/// </summary>
internal struct RowAggregate
{
    /// <summary>The number of matching rows.</summary>
    public int Count;

    /// <summary>The number of matching rows whose folded column held a value. Kept for minimum and maximum only.</summary>
    public int Values;

    /// <summary>The result over an integer or date column. NULL cells change nothing.</summary>
    public long Whole;

    /// <summary>The result over a floating-point column, held as a double. NULL cells change nothing.</summary>
    public double Real;

    /// <summary>
    /// Chooses how a scan reads and combines a column, or <see cref="RowFoldKind.None"/> when the column is not
    /// stored as the type the caller was compiled for.
    /// </summary>
    /// <param name="column">The column's stored type.</param>
    /// <param name="fold">The operation.</param>
    /// <param name="kind">The type the caller expects: <c>i l f d</c> for <c>INT</c>, <c>BIGINT</c>, <c>FLOAT</c> and <c>DOUBLE</c>, or <c>t</c> for <c>DATE</c>.</param>
    public static RowFoldKind KindFor(StorageKind column, RowFold fold, char kind) => (column, kind, fold) switch
    {
        (StorageKind.Int, 'i', RowFold.Sum) => RowFoldKind.SumInt32,
        (StorageKind.BigInt, 'l', RowFold.Sum) => RowFoldKind.SumInt64,
        (StorageKind.Float, 'f', RowFold.Sum) => RowFoldKind.SumFloat,
        (StorageKind.Double, 'd', RowFold.Sum) => RowFoldKind.SumDouble,
        (StorageKind.Int, 'i', RowFold.Min) => RowFoldKind.MinInt32,
        (StorageKind.BigInt, 'l', RowFold.Min) => RowFoldKind.MinInt64,
        (StorageKind.Float, 'f', RowFold.Min) => RowFoldKind.MinFloat,
        (StorageKind.Double, 'd', RowFold.Min) => RowFoldKind.MinDouble,
        (StorageKind.Date, 't', RowFold.Min) => RowFoldKind.MinDate,
        (StorageKind.Int, 'i', RowFold.Max) => RowFoldKind.MaxInt32,
        (StorageKind.BigInt, 'l', RowFold.Max) => RowFoldKind.MaxInt64,
        (StorageKind.Float, 'f', RowFold.Max) => RowFoldKind.MaxFloat,
        (StorageKind.Double, 'd', RowFold.Max) => RowFoldKind.MaxDouble,
        (StorageKind.Date, 't', RowFold.Max) => RowFoldKind.MaxDate,
        _ => RowFoldKind.None,
    };

    /// <summary>Counts one matching row and folds its column in, unless the cell is NULL.</summary>
    /// <exception cref="OverflowException">An integer sum left the range of <see cref="long"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(in RowRef row, RowFoldKind kind, int ordinal)
    {
        Count++;
        switch (kind)
        {
            case RowFoldKind.SumInt32:
            case RowFoldKind.MinInt32:
            case RowFoldKind.MaxInt32:
                if (row.TryGetInt32(ordinal, out int small))
                {
                    Fold(kind, small);
                }

                break;
            case RowFoldKind.SumInt64:
            case RowFoldKind.MinInt64:
            case RowFoldKind.MaxInt64:
                if (row.TryGetInt64(ordinal, out long large))
                {
                    Fold(kind, large);
                }

                break;
            case RowFoldKind.SumFloat:
            case RowFoldKind.MinFloat:
            case RowFoldKind.MaxFloat:
                if (row.TryGetFloat(ordinal, out float single))
                {
                    Fold(kind, (double)single);
                }

                break;
            case RowFoldKind.SumDouble:
            case RowFoldKind.MinDouble:
            case RowFoldKind.MaxDouble:
                if (row.TryGetDouble(ordinal, out double real))
                {
                    Fold(kind, real);
                }

                break;
            case RowFoldKind.MinDate:
            case RowFoldKind.MaxDate:
                if (row.TryGetDayNumber(ordinal, out int day))
                {
                    Fold(kind, day);
                }

                break;
        }
    }

    /// <summary>Counts one matching row and folds its column in, read from the stored bytes at a checked offset.</summary>
    /// <exception cref="OverflowException">An integer sum left the range of <see cref="long"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AddBytes(ReadOnlySpan<byte> row, RowFoldKind kind, int offset)
    {
        Count++;
        switch (kind)
        {
            case RowFoldKind.SumInt32:
                Whole = checked(Whole + RowBytes.Int32(row, offset));
                break;
            case RowFoldKind.SumInt64:
                Whole = checked(Whole + RowBytes.Int64(row, offset));
                break;
            case RowFoldKind.SumFloat:
                Real += RowBytes.Single(row, offset);
                break;
            case RowFoldKind.SumDouble:
                Real += RowBytes.Double(row, offset);
                break;
            case RowFoldKind.MinInt32:
                Lower(RowBytes.Int32(row, offset));
                break;
            case RowFoldKind.MinInt64:
                Lower(RowBytes.Int64(row, offset));
                break;
            case RowFoldKind.MinFloat:
                Lower((double)RowBytes.Single(row, offset));
                break;
            case RowFoldKind.MinDouble:
                Lower(RowBytes.Double(row, offset));
                break;
            case RowFoldKind.MinDate:
                Lower(RowBytes.DayNumber(row, offset));
                break;
            case RowFoldKind.MaxInt32:
                Raise(RowBytes.Int32(row, offset));
                break;
            case RowFoldKind.MaxInt64:
                Raise(RowBytes.Int64(row, offset));
                break;
            case RowFoldKind.MaxFloat:
                Raise((double)RowBytes.Single(row, offset));
                break;
            case RowFoldKind.MaxDouble:
                Raise(RowBytes.Double(row, offset));
                break;
            case RowFoldKind.MaxDate:
                Raise(RowBytes.DayNumber(row, offset));
                break;
        }
    }

    /// <summary>Counts one matching row and folds its column in, read from a typed cell of a checked type.</summary>
    /// <exception cref="OverflowException">An integer sum left the range of <see cref="long"/>.</exception>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AddCells(ReadOnlySpan<CellValue> cells, RowFoldKind kind, int ordinal)
    {
        Count++;
        switch (kind)
        {
            case RowFoldKind.SumInt32:
                Whole = checked(Whole + cells[ordinal].AsInt32());
                break;
            case RowFoldKind.SumInt64:
                Whole = checked(Whole + cells[ordinal].AsInt64());
                break;
            case RowFoldKind.SumFloat:
                Real += (float)cells[ordinal].AsDouble(); // a FLOAT column holds a float, widened in the cell
                break;
            case RowFoldKind.SumDouble:
                Real += cells[ordinal].AsDouble();
                break;
            case RowFoldKind.MinInt32:
                Lower(cells[ordinal].AsInt32());
                break;
            case RowFoldKind.MinInt64:
                Lower(cells[ordinal].AsInt64());
                break;
            case RowFoldKind.MinFloat:
            case RowFoldKind.MinDouble:
                Lower(cells[ordinal].AsDouble());
                break;
            case RowFoldKind.MinDate:
                Lower(cells[ordinal].AsDate().DayNumber);
                break;
            case RowFoldKind.MaxInt32:
                Raise(cells[ordinal].AsInt32());
                break;
            case RowFoldKind.MaxInt64:
                Raise(cells[ordinal].AsInt64());
                break;
            case RowFoldKind.MaxFloat:
            case RowFoldKind.MaxDouble:
                Raise(cells[ordinal].AsDouble());
                break;
            case RowFoldKind.MaxDate:
                Raise(cells[ordinal].AsDate().DayNumber);
                break;
        }
    }

    /// <summary>Folds in an integer or day number the caller has already read.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Fold(RowFoldKind kind, long value)
    {
        if (kind <= RowFoldKind.SumDouble)
        {
            Whole = checked(Whole + value);
        }
        else if (kind <= RowFoldKind.MinDate)
        {
            Lower(value);
        }
        else
        {
            Raise(value);
        }
    }

    /// <summary>Folds in a floating-point value the caller has already read.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Fold(RowFoldKind kind, double value)
    {
        if (kind <= RowFoldKind.SumDouble)
        {
            Real += value;
        }
        else if (kind <= RowFoldKind.MinDate)
        {
            Lower(value);
        }
        else
        {
            Raise(value);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Lower(long value)
    {
        if (Values++ == 0 || value < Whole)
        {
            Whole = value;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Raise(long value)
    {
        if (Values++ == 0 || value > Whole)
        {
            Whole = value;
        }
    }

    // NaN orders below every number, as it does for Enumerable.Min: once seen, it is the minimum.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Lower(double value)
    {
        if (Values++ == 0 || value < Real || double.IsNaN(value))
        {
            Real = value;
        }
    }

    // ...and so it is the maximum only when every value is NaN: any number replaces it.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Raise(double value)
    {
        if (Values++ == 0 || value > Real || double.IsNaN(Real))
        {
            Real = value;
        }
    }
}
