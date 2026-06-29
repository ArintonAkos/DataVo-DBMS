using DataVo.Core.Models.Catalog;
using DataVo.Core.CompiledQueries;

namespace DataVo.Core.StorageEngine;

internal interface IFixedWidthPatchStorageEngine
{
    bool TryPatchFixedWidthRow(
        string databaseName,
        string tableName,
        long rowId,
        IReadOnlyList<Column> columns,
        ReadOnlySpan<int> ordinals,
        ReadOnlySpan<DataVoFixedWidthValue> values);
}
