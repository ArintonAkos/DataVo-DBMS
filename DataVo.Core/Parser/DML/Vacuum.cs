using DataVo.Core.BTree;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Serialization;

namespace DataVo.Core.Parser.DML;

/// <summary>
/// Handles the <c>VACUUM</c> DML statement for a specific table.
/// <para>
/// Performs storage compaction by removing tombstoned (soft-deleted) rows and rewriting
/// the surviving rows with new contiguous row IDs. After compaction, all B-Tree indexes
/// associated with the table are dropped and rebuilt from scratch using the compacted data.
/// </para>
/// </summary>
/// <remarks>
/// <para>
/// <b>Side effects:</b>
/// <list type="bullet">
///   <item><description>Row IDs change after compaction — any external references to old row IDs become invalid.</description></item>
///   <item><description>All indexes (PK, UK, and user-defined) are dropped and recreated.</description></item>
///   <item><description>The physical storage file is rewritten.</description></item>
/// </list>
/// </para>
/// </remarks>
/// <param name="ast">The parsed <see cref="VacuumStatement"/> AST node containing the target table name.</param>
internal class Vacuum(VacuumStatement ast) : BaseDbAction
{
    /// <summary>
    /// Executes the VACUUM operation:
    /// <list type="number">
    ///   <item><description>Resolves the active database from the session cache.</description></item>
    ///   <item><description>Compacts the table storage, removing tombstoned rows and reassigning row IDs.</description></item>
    ///   <item><description>Retrieves all index definitions for the table from the system catalog.</description></item>
    ///   <item><description>For each index: drops the old B-Tree, deserializes each compacted row to extract index keys, and creates a fresh B-Tree with the new row IDs.</description></item>
    /// </list>
    /// </summary>
    /// <param name="session">The session identifier used to resolve the active database from the cache.</param>
    /// <remarks>
    /// On failure, the error message is logged and appended to <see cref="BaseDbAction.Messages"/>.
    /// </remarks>
    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = GetDatabaseName(session);

            string tableName = ast.TableName.Name;

            // 1. Compact the storage: remove tombstoned rows, get surviving rows with new IDs
            var compactedRows = Context.CompactTable(tableName, databaseName);

            // 2. Rebuild all indexes for this table from scratch
            var indexes = Catalog.GetTableIndexes(tableName, databaseName);

            foreach (var index in indexes)
            {
                // Drop the old index
                Indexes.DropIndex(index.IndexFileName, tableName, databaseName);

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

                Indexes.CreateIndex(indexData, index.IndexFileName, tableName, databaseName);
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
