using DataVo.Core.Models.Catalog;
using DataVo.Core.CompiledQueries;

namespace DataVo.Core.StorageEngine;

internal readonly record struct FixedWidthPatchOperation(long RowId, DataVoFixedWidthValue[] Values);

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
        int affected = 0;
        foreach (FixedWidthPatchOperation operation in operations)
        {
            if (TryPatchFixedWidthRow(databaseName, tableName, operation.RowId, columns, ordinals, operation.Values))
            {
                affected++;
            }
        }

        return affected;
    }
}
