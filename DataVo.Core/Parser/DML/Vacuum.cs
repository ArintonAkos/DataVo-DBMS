using DataVo.Core.Logging;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.DML;

/// <summary>
/// Handles the <c>VACUUM</c> DML statement for a specific table.
/// <para>
/// Performs storage compaction by removing tombstoned (soft-deleted) rows and rewriting
/// the surviving rows with new contiguous row IDs. After compaction, every index on the table is
/// rebuilt as its own kind (B-Tree, HNSW, FLAT), the columnar index is invalidated, and MVCC version
/// entries naming the old row IDs are discarded.
/// </para>
/// </summary>
/// <remarks>
/// <para>
/// <b>Side effects:</b>
/// <list type="bullet">
///   <item><description>Row IDs change after compaction — any external references to old row IDs become invalid.</description></item>
///   <item><description>All indexes (PK, UK, user-defined and vector) are dropped and recreated.</description></item>
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
    ///   <item><description>Rebuilds the table's indexes from the compacted rows through <see cref="DDL.TableRewrite"/>.</description></item>
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

            // Compaction renumbers the rows: no row-level writer (they hold the table's intent lock) may run meanwhile.
            Locks.AcquireWriteLock(databaseName, tableName);
            try
            {
                // 1. Compact the storage: remove tombstoned rows, get surviving rows with new IDs
                var compactedRows = Context.CompactTable(tableName, databaseName, allowIndexedCompaction: true);

                // 2. Rebuild every index as its own kind, invalidate the columnar index, drop stale MVCC entries
                DDL.TableRewrite.RebuildAfterRenumber(Engine, databaseName, tableName, Catalog.GetTableIndexes(tableName, databaseName));

                Messages.Add($"VACUUM complete. {compactedRows.Count} rows compacted in {tableName}.");
            }
            finally
            {
                Locks.ReleaseWriteLock(databaseName, tableName);
            }
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            AddError(ex);
        }
    }
}
