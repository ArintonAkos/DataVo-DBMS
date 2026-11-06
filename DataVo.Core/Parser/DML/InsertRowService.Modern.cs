#if NET10_0_OR_GREATER
using System.Runtime.InteropServices;

namespace DataVo.Core.Parser.DML;

internal sealed partial class InsertRowService
{
    private static ReadOnlySpan<RowKey> AsRowIdSpan(IReadOnlyList<RowKey> rowIds) => rowIds switch
    {
        List<RowKey> list => CollectionsMarshal.AsSpan(list),
        RowKey[] array => array,
        _ => rowIds.ToArray(),
    };
}
#endif
