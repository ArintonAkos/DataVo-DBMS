#if NET10_0_OR_GREATER
namespace DataVo.Core.CompiledQueries;

public readonly ref partial struct CompiledRowReader
{
    /// <summary>Reads a modern <see cref="DateOnly"/> column; throws if the cell is not Date or is NULL.</summary>
    public DateOnly GetDateOnly(string column) => GetDate(column).ToDateOnly();

    /// <summary>Reads a modern <see cref="DateOnly"/> column by ordinal; throws if the cell is not Date or is NULL.</summary>
    public DateOnly GetDateOnly(int ordinal) => GetDate(ordinal).ToDateOnly();

    /// <summary>Reads a nullable modern <see cref="DateOnly"/> column; SQL NULL returns <c>null</c>.</summary>
    public DateOnly? GetDateOnlyOrNull(string column)
    {
        DataVoDate? value = GetDateOrNull(column);
        return value.HasValue ? value.Value.ToDateOnly() : null;
    }

    /// <summary>Reads a nullable modern <see cref="DateOnly"/> column by ordinal; SQL NULL returns <c>null</c>.</summary>
    public DateOnly? GetDateOnlyOrNull(int ordinal)
    {
        DataVoDate? value = GetDateOrNull(ordinal);
        return value.HasValue ? value.Value.ToDateOnly() : null;
    }
}
#endif
