#if NET10_0_OR_GREATER
namespace DataVo.Core.Runtime.Reactive;

public readonly partial struct CellValue
{
    /// <summary>Creates a DATE cell from a modern <see cref="DateOnly"/> value.</summary>
    public static CellValue From(DateOnly value) => From(DataVoDate.FromDateOnly(value));

    /// <summary>Reads the DATE cell as a modern <see cref="DateOnly"/> value.</summary>
    public DateOnly AsDateOnly() => AsDate().ToDateOnly();
}
#endif
