#if NET10_0_OR_GREATER
using System.Runtime.InteropServices;

namespace DataVo.Core.Parser.DML;

internal sealed partial class InsertRowService
{
    private static ReadOnlySpan<long> AsRowIdSpan(IReadOnlyList<long> rowIds) => rowIds switch
    {
        List<long> list => CollectionsMarshal.AsSpan(list),
        long[] array => array,
        _ => rowIds.ToArray(),
    };
}
#endif
