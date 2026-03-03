using DataVo.Core.BTree;
using DataVo.Core.Cache;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Serialization;

namespace DataVo.Core.Parser.DML;

/// <summary>
/// Executes VACUUM tableName — compacts tombstoned rows and rebuilds indexes.
/// </summary>
internal class Vacuum(VacuumStatement ast) : BaseDbAction
{
    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            string tableName = ast.TableName.Name;

            // 1. Compact the storage: remove tombstoned rows, get surviving rows with new IDs
            var compactedRows = StorageContext.Instance.CompactTable(tableName, databaseName);

            // 2. Rebuild all indexes for this table from scratch
            var indexes = Catalog.GetTableIndexes(tableName, databaseName);

            foreach (var index in indexes)
            {
                // Drop the old index
                IndexManager.Instance.DropIndex(index.IndexFileName, tableName, databaseName);

                // Recreate with fresh data
                var indexData = new Dictionary<string, List<long>>();

                foreach (var (newRowId, rawRow) in compactedRows)
                {
                    var row = RowSerializer.Deserialize(databaseName, tableName, rawRow, null);
                    string indexKey = IndexKeyEncoder.BuildKeyString(row, index.AttributeNames);

                    if (!indexData.ContainsKey(indexKey))
                        indexData[indexKey] = [];

                    indexData[indexKey].Add(newRowId);
                }

                IndexManager.Instance.CreateIndex(indexData, index.IndexFileName, tableName, databaseName);
            }

            Messages.Add($"VACUUM complete. {compactedRows.Count} rows compacted in {tableName}.");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }
}
