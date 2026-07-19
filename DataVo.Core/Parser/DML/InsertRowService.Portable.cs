#if NETSTANDARD2_1
namespace DataVo.Core.Parser.DML;

internal sealed partial class InsertRowService
{
    private static ReadOnlySpan<long> AsRowIdSpan(IReadOnlyList<long> rowIds) => rowIds switch
    {
        long[] array => array,
        _ => rowIds.ToArray(),
    };
}
#endif
