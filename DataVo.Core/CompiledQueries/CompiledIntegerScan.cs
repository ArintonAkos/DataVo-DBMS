using System.Runtime.Intrinsics;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine;

namespace DataVo.Core.CompiledQueries;

internal static class CompiledIntegerScan
{
    public static List<T> ScanMany<T>(
        Dictionary<long, StoredRow> rows,
        string whereColumn,
        int expected,
        CompiledRowMapper<T> mapper)
    {
        var results = new List<T>();
        ScanRows(rows, whereColumn, expected, (row, state) =>
        {
            state.Add(mapper(new CompiledRowReader(row.AsView())));
            return true;
        }, results);

        return results;
    }

    public static bool TryScanMany<T>(
        Dictionary<long, StoredRow> rows,
        string whereColumn,
        int expected,
        CompiledRowMapper<T> mapper,
        out List<T> results)
    {
        results = [];
        if (!IsInt32ScanEligible(rows, whereColumn))
        {
            return false;
        }

        results = ScanMany(rows, whereColumn, expected, mapper);
        return true;
    }

    public static T? ScanSingle<T>(
        Dictionary<long, StoredRow> rows,
        string whereColumn,
        int expected,
        CompiledRowMapper<T> mapper)
    {
        var state = new SingleScanState<T>();
        ScanRows(rows, whereColumn, expected, (row, state) =>
        {
            state.Result = mapper(new CompiledRowReader(row.AsView()));
            return false;
        }, state);

        return state.Result;
    }

    public static bool TryScanSingle<T>(
        Dictionary<long, StoredRow> rows,
        string whereColumn,
        int expected,
        CompiledRowMapper<T> mapper,
        out T? result)
    {
        result = default;
        if (!IsInt32ScanEligible(rows, whereColumn))
        {
            return false;
        }

        result = ScanSingle(rows, whereColumn, expected, mapper);
        return true;
    }

    private static void ScanRows<TState>(
        Dictionary<long, StoredRow> rows,
        string whereColumn,
        int expected,
        Func<StoredRow, TState, bool> onMatch,
        TState state)
    {
        if (Vector256.IsHardwareAccelerated && rows.Count >= Vector256<int>.Count)
        {
            ScanRowsVector256(rows, whereColumn, expected, onMatch, state);
            return;
        }

        if (Vector128.IsHardwareAccelerated && rows.Count >= Vector128<int>.Count)
        {
            ScanRowsVector128(rows, whereColumn, expected, onMatch, state);
            return;
        }

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

    private static void ScanRowsVector256<TState>(
        Dictionary<long, StoredRow> rows,
        string whereColumn,
        int expected,
        Func<StoredRow, TState, bool> onMatch,
        TState state)
    {
        const int Width = 8;
        Span<int> values = stackalloc int[Width];
        Span<long> rowIds = stackalloc long[Width];
        Vector256<int> target = Vector256.Create(expected);
        int buffered = 0;

        foreach ((long rowId, StoredRow row) in rows)
        {
            if (!TryReadInt32(row, whereColumn, out int value))
            {
                continue;
            }

            values[buffered] = value;
            rowIds[buffered] = rowId;
            buffered++;

            if (buffered == Width
                && !FlushVector256(values, rowIds, rows, target, onMatch, state))
            {
                return;
            }

            if (buffered == Width)
            {
                buffered = 0;
            }
        }

        for (int i = 0; i < buffered; i++)
        {
            if (values[i] == expected
                && rows.TryGetValue(rowIds[i], out StoredRow? row)
                && !onMatch(row, state))
            {
                return;
            }
        }
    }

    private static void ScanRowsVector128<TState>(
        Dictionary<long, StoredRow> rows,
        string whereColumn,
        int expected,
        Func<StoredRow, TState, bool> onMatch,
        TState state)
    {
        const int Width = 4;
        Span<int> values = stackalloc int[Width];
        Span<long> rowIds = stackalloc long[Width];
        Vector128<int> target = Vector128.Create(expected);
        int buffered = 0;

        foreach ((long rowId, StoredRow row) in rows)
        {
            if (!TryReadInt32(row, whereColumn, out int value))
            {
                continue;
            }

            values[buffered] = value;
            rowIds[buffered] = rowId;
            buffered++;

            if (buffered == Width
                && !FlushVector128(values, rowIds, rows, target, onMatch, state))
            {
                return;
            }

            if (buffered == Width)
            {
                buffered = 0;
            }
        }

        for (int i = 0; i < buffered; i++)
        {
            if (values[i] == expected
                && rows.TryGetValue(rowIds[i], out StoredRow? row)
                && !onMatch(row, state))
            {
                return;
            }
        }
    }

    private static bool FlushVector256<TState>(
        Span<int> values,
        Span<long> rowIds,
        Dictionary<long, StoredRow> rows,
        Vector256<int> target,
        Func<StoredRow, TState, bool> onMatch,
        TState state)
    {
        Vector256<int> vector = Vector256.Create(
            values[0], values[1], values[2], values[3],
            values[4], values[5], values[6], values[7]);
        Vector256<int> matches = Vector256.Equals(vector, target);

        for (int lane = 0; lane < Vector256<int>.Count; lane++)
        {
            if (matches.GetElement(lane) == 0)
            {
                continue;
            }

            if (rows.TryGetValue(rowIds[lane], out StoredRow? row)
                && !onMatch(row, state))
            {
                return false;
            }
        }

        return true;
    }

    private static bool FlushVector128<TState>(
        Span<int> values,
        Span<long> rowIds,
        Dictionary<long, StoredRow> rows,
        Vector128<int> target,
        Func<StoredRow, TState, bool> onMatch,
        TState state)
    {
        Vector128<int> vector = Vector128.Create(values[0], values[1], values[2], values[3]);
        Vector128<int> matches = Vector128.Equals(vector, target);

        for (int lane = 0; lane < Vector128<int>.Count; lane++)
        {
            if (matches.GetElement(lane) == 0)
            {
                continue;
            }

            if (rows.TryGetValue(rowIds[lane], out StoredRow? row)
                && !onMatch(row, state))
            {
                return false;
            }
        }

        return true;
    }

    private static bool TryReadInt32(StoredRow row, string column, out int value)
    {
        value = default;
        if (!row.Schema.TryGetOrdinal(column, out int ordinal))
        {
            return false;
        }

        CellValue cell = row[ordinal];
        if (cell.Type != CellType.Int32)
        {
            return false;
        }

        value = cell.AsInt32();
        return true;
    }

    private static bool IsInt32ScanEligible(Dictionary<long, StoredRow> rows, string whereColumn)
    {
        foreach ((_, StoredRow row) in rows)
        {
            if (!row.Schema.TryGetOrdinal(whereColumn, out int ordinal))
            {
                continue;
            }

            CellType type = row[ordinal].Type;
            if (type is not CellType.Int32 and not CellType.Null)
            {
                return false;
            }
        }

        return true;
    }

    private sealed class SingleScanState<T>
    {
        public T? Result;
    }
}
