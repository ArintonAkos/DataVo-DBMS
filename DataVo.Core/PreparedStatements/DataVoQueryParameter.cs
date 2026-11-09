using System.Globalization;
using System.Runtime.CompilerServices;

namespace DataVo.Core;

/// <summary>
/// Identifies the unboxed storage kind of a <see cref="DataVoQueryParameter"/>.
/// </summary>
public enum DataVoQueryParameterKind : byte
{
    /// <summary>
    /// Represents a SQL <c>NULL</c> parameter value.
    /// </summary>
    Null = 0,

    /// <summary>
    /// Represents an unboxed 64-bit signed integer parameter value (covers <see cref="int"/> and <see cref="long"/>).
    /// </summary>
    Int64 = 1,

    /// <summary>
    /// Represents an unboxed double-precision floating-point parameter value (covers <see cref="float"/> and <see cref="double"/>).
    /// </summary>
    Double = 2,

    /// <summary>
    /// Represents an unboxed boolean parameter value.
    /// </summary>
    Boolean = 3,

    /// <summary>
    /// Represents an unboxed <see cref="Guid"/> parameter value.
    /// </summary>
    Guid = 4,

    /// <summary>
    /// Represents a string parameter value.
    /// </summary>
    String = 5,

    /// <summary>
    /// Represents an arbitrary boxed CLR object parameter value.
    /// </summary>
    Object = 6,
}

/// <summary>
/// Represents a named SQL query parameter passed to a <see cref="DataVoPreparedStatement"/> without boxing primitive values.
/// </summary>
/// <remarks>
/// <para>
/// Primitive values (<see cref="int"/>, <see cref="long"/>, <see cref="double"/>, <see cref="float"/>, <see cref="bool"/>, and <see cref="Guid"/>)
/// are stored inline inside the struct so that hot-path parameter binding allocates zero heap objects.
/// </para>
/// <para>
/// Implicit conversions from <c>(string Name, T Value)</c> value tuples allow ergonomic invocation such as
/// <c>prepared.Execute(("id", 42))</c>.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// DataVoPreparedStatement lookup = context.Prepare("SELECT Id, Name FROM Users WHERE Id = @id");
/// QueryResult result = lookup.Execute(("id", 42));
/// </code>
/// </example>
public readonly struct DataVoQueryParameter
{
    private readonly long _bits;
    private readonly Guid _guid;
    private readonly object? _object;

    /// <summary>
    /// Gets the parameter name (with or without a leading <c>@</c> prefix).
    /// </summary>
    public string Name { get; }

    /// <summary>
    /// Gets the unboxed storage kind for this parameter value.
    /// </summary>
    public DataVoQueryParameterKind Kind { get; }

    /// <summary>
    /// Gets a value indicating whether the parameter value is SQL <c>NULL</c>.
    /// </summary>
    public bool IsNull => Kind == DataVoQueryParameterKind.Null;

    /// <summary>
    /// Initializes a new parameter bound to a 32-bit signed integer.
    /// </summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="value">The integer value.</param>
    public DataVoQueryParameter(string name, int value)
    {
        Name = name;
        Kind = DataVoQueryParameterKind.Int64;
        _bits = value;
        _guid = default;
        _object = null;
    }

    /// <summary>
    /// Initializes a new parameter bound to a 64-bit signed integer.
    /// </summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="value">The 64-bit integer value.</param>
    public DataVoQueryParameter(string name, long value)
    {
        Name = name;
        Kind = DataVoQueryParameterKind.Int64;
        _bits = value;
        _guid = default;
        _object = null;
    }

    /// <summary>
    /// Initializes a new parameter bound to a double-precision floating-point number.
    /// </summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="value">The floating-point value.</param>
    public DataVoQueryParameter(string name, double value)
    {
        Name = name;
        Kind = DataVoQueryParameterKind.Double;
        _bits = BitConverter.DoubleToInt64Bits(value);
        _guid = default;
        _object = null;
    }

    /// <summary>
    /// Initializes a new parameter bound to a single-precision floating-point number.
    /// </summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="value">The floating-point value.</param>
    public DataVoQueryParameter(string name, float value)
    {
        Name = name;
        Kind = DataVoQueryParameterKind.Double;
        _bits = BitConverter.DoubleToInt64Bits(value);
        _guid = default;
        _object = null;
    }

    /// <summary>
    /// Initializes a new parameter bound to a boolean value.
    /// </summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="value">The boolean value.</param>
    public DataVoQueryParameter(string name, bool value)
    {
        Name = name;
        Kind = DataVoQueryParameterKind.Boolean;
        _bits = value ? 1L : 0L;
        _guid = default;
        _object = null;
    }

    /// <summary>
    /// Initializes a new parameter bound to a <see cref="Guid"/> value.
    /// </summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="value">The GUID value.</param>
    public DataVoQueryParameter(string name, Guid value)
    {
        Name = name;
        Kind = DataVoQueryParameterKind.Guid;
        _bits = 0;
        _guid = value;
        _object = null;
    }

    /// <summary>
    /// Initializes a new parameter bound to a string value (or <c>null</c>).
    /// </summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="value">The string value, or <c>null</c> for SQL <c>NULL</c>.</param>
    public DataVoQueryParameter(string name, string? value)
    {
        Name = name;
        if (value is null)
        {
            Kind = DataVoQueryParameterKind.Null;
            _bits = 0;
            _guid = default;
            _object = null;
        }
        else
        {
            Kind = DataVoQueryParameterKind.String;
            _bits = 0;
            _guid = default;
            _object = value;
        }
    }

    private DataVoQueryParameter(
        string name,
        DataVoQueryParameterKind kind,
        long bits,
        Guid guid,
        object? obj)
    {
        Name = name;
        Kind = kind;
        _bits = bits;
        _guid = guid;
        _object = obj;
    }

    /// <summary>
    /// Creates a SQL <c>NULL</c> parameter for the given parameter name.
    /// </summary>
    /// <param name="name">The parameter name.</param>
    /// <returns>A parameter whose <see cref="IsNull"/> property is <c>true</c>.</returns>
    public static DataVoQueryParameter Null(string name)
        => new(name, DataVoQueryParameterKind.Null, 0, default, null);

    /// <summary>
    /// Creates a <see cref="DataVoQueryParameter"/> from an untyped CLR object value, unpacking common primitives.
    /// </summary>
    /// <param name="name">The parameter name.</param>
    /// <param name="value">The CLR value, or <c>null</c> / <see cref="DBNull.Value"/> for SQL <c>NULL</c>.</param>
    /// <returns>The constructed query parameter.</returns>
    public static DataVoQueryParameter FromObject(string name, object? value)
    {
        if (value is null || value is DBNull)
        {
            return Null(name);
        }

        return value switch
        {
            int i => new DataVoQueryParameter(name, i),
            long l => new DataVoQueryParameter(name, l),
            short s => new DataVoQueryParameter(name, (long)s),
            byte b => new DataVoQueryParameter(name, (long)b),
            double d => new DataVoQueryParameter(name, d),
            float f => new DataVoQueryParameter(name, f),
            decimal dec => new DataVoQueryParameter(name, (double)dec),
            bool flag => new DataVoQueryParameter(name, flag),
            Guid g => new DataVoQueryParameter(name, g),
            string str => new DataVoQueryParameter(name, str),
            float[] floats => new DataVoQueryParameter(name, FormatVectorString(floats)),
            double[] doubles => new DataVoQueryParameter(name, FormatVectorString(doubles)),
            decimal[] decimals => new DataVoQueryParameter(name, FormatVectorString(decimals)),
            IEnumerable<float> floatSeq => new DataVoQueryParameter(name, FormatVectorString(floatSeq)),
            IEnumerable<double> doubleSeq => new DataVoQueryParameter(name, FormatVectorString(doubleSeq)),
            IEnumerable<decimal> decimalSeq => new DataVoQueryParameter(name, FormatVectorString(decimalSeq)),
#if NET6_0_OR_GREATER
            DateOnly dateOnly => new DataVoQueryParameter(name, dateOnly.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)),
#endif
            DateTime dateTime => new DataVoQueryParameter(name, dateTime.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture)),
            _ => new DataVoQueryParameter(name, DataVoQueryParameterKind.Object, 0, default, value),
        };
    }

    /// <summary>
    /// Gets the unboxed 64-bit integer value when <see cref="Kind"/> is <see cref="DataVoQueryParameterKind.Int64"/>.
    /// </summary>
    public long AsInt64() => _bits;

    /// <summary>
    /// Gets the unboxed 32-bit integer value when <see cref="Kind"/> is <see cref="DataVoQueryParameterKind.Int64"/>.
    /// </summary>
    public int AsInt32() => checked((int)_bits);

    /// <summary>
    /// Gets the unboxed double-precision value when <see cref="Kind"/> is <see cref="DataVoQueryParameterKind.Double"/>.
    /// </summary>
    public double AsDouble() => BitConverter.Int64BitsToDouble(_bits);

    /// <summary>
    /// Gets the unboxed boolean value when <see cref="Kind"/> is <see cref="DataVoQueryParameterKind.Boolean"/>.
    /// </summary>
    public bool AsBoolean() => _bits != 0;

    /// <summary>
    /// Gets the unboxed <see cref="Guid"/> value when <see cref="Kind"/> is <see cref="DataVoQueryParameterKind.Guid"/>.
    /// </summary>
    public Guid AsGuid() => _guid;

    /// <summary>
    /// Gets the string value when <see cref="Kind"/> is <see cref="DataVoQueryParameterKind.String"/>.
    /// </summary>
    public string? AsString() => _object as string;

    /// <summary>
    /// Converts this parameter to a boxed CLR value suitable for AST literal nodes.
    /// </summary>
    /// <returns>The CLR representation of the parameter value, or <c>null</c> when <see cref="IsNull"/> is <c>true</c>.</returns>
    public object? ToBoxedValue()
    {
        return Kind switch
        {
            DataVoQueryParameterKind.Null => null,
            DataVoQueryParameterKind.Int64 => _bits is >= int.MinValue and <= int.MaxValue
                ? BoxedIntegerCache.Get((int)_bits)
                : _bits,
            DataVoQueryParameterKind.Double => BitConverter.Int64BitsToDouble(_bits),
            DataVoQueryParameterKind.Boolean => _bits != 0 ? BoxedBooleanTrue : BoxedBooleanFalse,
            DataVoQueryParameterKind.Guid => _guid.ToString(),
            DataVoQueryParameterKind.String => _object,
            DataVoQueryParameterKind.Object => _object?.ToString(),
            _ => null,
        };
    }

    private static readonly object BoxedBooleanTrue = true;
    private static readonly object BoxedBooleanFalse = false;

    /// <summary>
    /// Checks whether this parameter's name matches <paramref name="expectedNameWithoutPrefix"/> (ignoring a leading <c>@</c>, <c>$</c> or <c>:</c>).
    /// </summary>
    /// <param name="expectedNameWithoutPrefix">The parameter identifier without a leading prefix.</param>
    /// <returns><c>true</c> when the names match case-insensitively; otherwise <c>false</c>.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool NameMatches(ReadOnlySpan<char> expectedNameWithoutPrefix)
    {
        ReadOnlySpan<char> span = Name.AsSpan();
        if (!span.IsEmpty && span[0] is ('@' or '$' or ':'))
        {
            span = span[1..];
        }

        return span.Equals(expectedNameWithoutPrefix, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// Converts a <c>(string Name, int Value)</c> tuple into a <see cref="DataVoQueryParameter"/>.
    /// </summary>
    public static implicit operator DataVoQueryParameter((string Name, int Value) tuple)
        => new(tuple.Name, tuple.Value);

    /// <summary>
    /// Converts a <c>(string Name, long Value)</c> tuple into a <see cref="DataVoQueryParameter"/>.
    /// </summary>
    public static implicit operator DataVoQueryParameter((string Name, long Value) tuple)
        => new(tuple.Name, tuple.Value);

    /// <summary>
    /// Converts a <c>(string Name, double Value)</c> tuple into a <see cref="DataVoQueryParameter"/>.
    /// </summary>
    public static implicit operator DataVoQueryParameter((string Name, double Value) tuple)
        => new(tuple.Name, tuple.Value);

    /// <summary>
    /// Converts a <c>(string Name, float Value)</c> tuple into a <see cref="DataVoQueryParameter"/>.
    /// </summary>
    public static implicit operator DataVoQueryParameter((string Name, float Value) tuple)
        => new(tuple.Name, tuple.Value);

    /// <summary>
    /// Converts a <c>(string Name, bool Value)</c> tuple into a <see cref="DataVoQueryParameter"/>.
    /// </summary>
    public static implicit operator DataVoQueryParameter((string Name, bool Value) tuple)
        => new(tuple.Name, tuple.Value);

    /// <summary>
    /// Converts a <c>(string Name, Guid Value)</c> tuple into a <see cref="DataVoQueryParameter"/>.
    /// </summary>
    public static implicit operator DataVoQueryParameter((string Name, Guid Value) tuple)
        => new(tuple.Name, tuple.Value);

    /// <summary>
    /// Converts a <c>(string Name, string? Value)</c> tuple into a <see cref="DataVoQueryParameter"/>.
    /// </summary>
    public static implicit operator DataVoQueryParameter((string Name, string? Value) tuple)
        => new(tuple.Name, tuple.Value);

    /// <summary>
    /// Converts a <c>(string Name, object? Value)</c> tuple into a <see cref="DataVoQueryParameter"/>.
    /// </summary>
    public static implicit operator DataVoQueryParameter((string Name, object? Value) tuple)
        => FromObject(tuple.Name, tuple.Value);

    private static string FormatVectorString<T>(IEnumerable<T> values) where T : IFormattable
    {
        return $"[{string.Join(", ", values.Select(value => value.ToString(null, CultureInfo.InvariantCulture)))}]";
    }
}

/// <summary>
/// Caches boxed 32-bit integers in the common range <c>[-1 .. 32767]</c> in lazily allocated 1,024-entry pages
/// so repeated point lookups and parameter bindings do not allocate boxed <see cref="int"/> instances.
/// </summary>
internal static class BoxedIntegerCache
{
    private const int MinCached = -1;
    private const int MaxCached = 32767;
    private const int PageShift = 10;
    private const int PageSize = 1 << PageShift;
    private const int PageMask = PageSize - 1;
    private static readonly object[]?[] Pages = new object[]?[(MaxCached - MinCached + PageSize) >> PageShift];

    /// <summary>
    /// Returns a cached boxed <see cref="int"/> when <paramref name="value"/> falls within <c>[-1 .. 32767]</c>,
    /// or boxes <paramref name="value"/> otherwise.
    /// </summary>
    /// <param name="value">The 32-bit integer value to box.</param>
    /// <returns>A boxed <see cref="int"/> instance.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static object Get(int value)
    {
        int offset = value - MinCached;
        if ((uint)offset <= (uint)(MaxCached - MinCached))
        {
            int pageIndex = offset >> PageShift;
            object[]? page = Volatile.Read(ref Pages[pageIndex]) ?? InitializePage(pageIndex);
            return page[offset & PageMask];
        }

        return value;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static object[] InitializePage(int pageIndex)
    {
        var created = new object[PageSize];
        int baseValue = MinCached + (pageIndex << PageShift);
        for (int i = 0; i < PageSize; i++)
        {
            created[i] = baseValue + i;
        }

        return Interlocked.CompareExchange(ref Pages[pageIndex], created, null) ?? created;
    }
}
