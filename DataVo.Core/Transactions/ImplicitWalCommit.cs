using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;

namespace DataVo.Core.Transactions;

internal static class ImplicitWalCommit
{
    public static bool IsEnabled(DataVoEngine engine)
    {
        return DataVoEngine.UsesGroupCommitWal(engine.Config)
            && engine.WalFrameAppender != null
            && engine.WalGroupCommitter != null;
    }

    public static void CommitIfEnabled(
        DataVoEngine engine,
        string databaseName,
        long mvccTransactionId,
        IReadOnlyList<WalOperation> operations)
    {
        if (!IsEnabled(engine) || operations.Count == 0)
        {
            return;
        }

        var entry = new WalEntry
        {
            TransactionId = Guid.NewGuid(),
            MvccTransactionId = mvccTransactionId,
            Timestamp = DateTime.UtcNow.Ticks,
            DatabaseName = databaseName,
            Operations = [.. operations],
            IsCheckpointed = false,
        };

        engine.CommitWalEntryThroughGroupCommit(entry);
    }

    public static Dictionary<string, object?> MaterializeTypedRow(ReactiveRowSchema schema, IReadOnlyList<CellValue> row)
    {
        var materialized = new Dictionary<string, object?>(schema.ColumnCount, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < schema.ColumnCount; i++)
        {
            materialized[schema.ColumnAt(i)] = row[i].ToObject();
        }

        return materialized;
    }
}
