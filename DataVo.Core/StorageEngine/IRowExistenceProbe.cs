namespace DataVo.Core.StorageEngine;

/// <summary>
/// Optional storage-engine capability: answer "does this table hold any live rows?" without
/// materializing table contents. Backends that only expose full scans fall back to enumeration.
/// </summary>
internal interface IRowExistenceProbe
{
    bool HasAnyRows(string databaseName, string tableName);
}
