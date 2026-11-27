using DataVo.Core.Parser.DML;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.Transactions;

namespace DataVo.Core;

/// <summary>
/// Direct-API writes inside an explicit transaction: instead of bypassing the session's open transaction, they buffer
/// their rows in it exactly like the equivalent SQL statement would, so COMMIT applies them through the shared
/// commit pipeline (MVCC conflict checks, the WAL, index maintenance and change capture) and ROLLBACK discards them.
/// </summary>
public sealed partial class DataVoContext
{
    /// <summary>
    /// Validates rows like <c>INSERT</c> inside a transaction (types, NOT NULL, keys against committed and buffered
    /// rows, foreign keys) and buffers them in <paramref name="transaction"/>. All rows are buffered or none.
    /// </summary>
    /// <param name="databaseName">The session's database.</param>
    /// <param name="tableName">The stored table name.</param>
    /// <param name="rows">The rows to insert.</param>
    /// <param name="transaction">The session's open transaction.</param>
    /// <returns>
    /// Provisional keys of the buffered rows. They identify the rows within the transaction only; COMMIT assigns
    /// their storage keys.
    /// </returns>
    private IReadOnlyList<RowKey> BufferInsertsInTransaction(
        string databaseName,
        string tableName,
        IReadOnlyList<IReadOnlyDictionary<string, object?>> rows,
        TransactionContext transaction)
    {
        using IDisposable engineScope = DataVoEngine.PushCurrent(Engine);
        int first = transaction.InsertedRows.TryGetValue(tableName, out List<Dictionary<string, object?>>? buffered) ? buffered.Count : 0;
        long statementTxId = MVCC.MvccCoordinator.ResolveStatementTransactionId(Engine, transaction.TransactionId);
        var service = new InsertRowService(Engine, Engine.StorageContext, Engine.Catalog, Engine.IndexManager);
        InsertRowsResult result = service.InsertRows(databaseName, tableName, rows, transaction, statementTxId);

        var keys = new RowKey[result.AcceptedRowCount];
        for (int i = 0; i < keys.Length; i++)
        {
            keys[i] = TransactionContext.BufferedRowKey(first + i);
        }

        return keys;
    }

    /// <summary>Converts typed rows into the dictionary rows the transactional insert path buffers.</summary>
    private static IReadOnlyList<IReadOnlyDictionary<string, object?>> ToDictionaryRows(
        ReactiveRowSchema columns,
        IReadOnlyList<CellValue[]> rows)
    {
        var result = new IReadOnlyDictionary<string, object?>[rows.Count];
        for (int i = 0; i < rows.Count; i++)
        {
            result[i] = ToDictionaryRow(columns, rows[i] ?? throw new ArgumentNullException(nameof(rows), "Rows cannot contain null entries."));
        }

        return result;
    }

    private static Dictionary<string, object?> ToDictionaryRow(ReactiveRowSchema columns, ReadOnlySpan<CellValue> row)
    {
        var dictionary = new Dictionary<string, object?>(row.Length, StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < row.Length; i++)
        {
            dictionary[columns.ColumnAt(i)] = row[i].ToObject();
        }

        return dictionary;
    }
}
