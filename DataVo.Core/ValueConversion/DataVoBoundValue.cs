using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.ValueConversion;

/// <summary>
/// Carries an already-converted cell through legacy dictionary write paths without serializing a vector to text.
/// </summary>
internal sealed class DataVoBoundValue(CellValue cell)
{
    public CellValue Cell { get; } = cell;

    public object? ToMaterializedValue() => Cell.ToObject();
}
