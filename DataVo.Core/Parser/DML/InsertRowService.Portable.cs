#if !NET8_0_OR_GREATER
namespace DataVo.Core.Parser.DML;

internal sealed partial class InsertRowService
{
    private static ReadOnlySpan<RowKey> AsRowIdSpan(IReadOnlyList<RowKey> rowIds) => rowIds switch
    {
        RowKey[] array => array,
        _ => rowIds.ToArray(),
    };
}
#endif
