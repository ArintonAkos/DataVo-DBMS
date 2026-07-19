#if NETSTANDARD2_1
using DataVo.Core.StorageEngine;

namespace DataVo.Core.CompiledQueries;

internal static partial class CompiledIntegerScan
{
    private static void ScanRows<TState>(
        Dictionary<long, StoredRow> rows,
        string whereColumn,
        int expected,
        Func<StoredRow, TState, bool> onMatch,
        TState state)
    {
        foreach ((_, StoredRow row) in rows)
        {
            if (TryReadInt32(row, whereColumn, out int value)
                && value == expected
                && !onMatch(row, state))
            {
                return;
            }
        }
    }
}
#endif
