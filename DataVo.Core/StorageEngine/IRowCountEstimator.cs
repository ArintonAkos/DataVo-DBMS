namespace DataVo.Core.StorageEngine;

/// <summary>
/// Optional storage-engine capability: a cheap row count for query planning, without materializing
/// table contents. The value is an estimate (it may ignore uncommitted writes of the current flow, and
/// may count deleted slots); it must never be used as a query result.
/// </summary>
internal interface IRowCountEstimator
{
    long EstimateRowCount(string databaseName, string tableName);
}
