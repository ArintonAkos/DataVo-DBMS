using DataVo.Core.Models.Catalog;
using DataVo.Core.CompiledQueries;

namespace DataVo.Core.StorageEngine;

internal readonly record struct FixedWidthPatchOperation(
    long RowId,
    DataVoFixedWidthValue Value0,
    DataVoFixedWidthValue Value1)
{
    public DataVoFixedWidthValue GetValue(int index) => index switch
    {
        0 => Value0,
        1 => Value1,
        _ => throw new ArgumentOutOfRangeException(nameof(index)),
    };
}

internal interface IFixedWidthPatchStorageEngine
{
    bool TryPatchFixedWidthRow(
        string databaseName,
        string tableName,
        long rowId,
        IReadOnlyList<Column> columns,
        ReadOnlySpan<int> ordinals,
        ReadOnlySpan<DataVoFixedWidthValue> values);

    int TryPatchFixedWidthRows(
        string databaseName,
        string tableName,
        IReadOnlyList<Column> columns,
        ReadOnlySpan<int> ordinals,
        IReadOnlyList<FixedWidthPatchOperation> operations)
    {
        if (ordinals.Length > 2)
        {
            throw new NotSupportedException("Batched fixed-width patch operations currently support up to two assignments.");
        }

        int affected = 0;
        Span<DataVoFixedWidthValue> values = stackalloc DataVoFixedWidthValue[ordinals.Length];
        foreach (FixedWidthPatchOperation operation in operations)
        {
            for (int i = 0; i < ordinals.Length; i++)
            {
                values[i] = operation.GetValue(i);
            }

            if (TryPatchFixedWidthRow(databaseName, tableName, operation.RowId, columns, ordinals, values))
            {
                affected++;
            }
        }

        return affected;
    }
}
