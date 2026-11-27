using DataVo.Core.Contracts.Results;
using DataVo.Core.Transactions;

namespace DataVo.Core.Batches;

/// <summary>
/// Applying a batch inside the session's explicit transaction. The folded batch is written through the same
/// transactional paths SQL uses: rows are found through the transaction's snapshot (its own buffered writes
/// included), deletes and updates run as parameterized <c>DELETE</c>/<c>UPDATE</c> statements (row locks, MVCC
/// conflict checks, constraint checks, cascades), and inserts are validated and buffered like <c>INSERT</c>. A
/// savepoint makes the batch all-or-nothing inside the transaction; COMMIT writes it, ROLLBACK discards it.
/// </summary>
internal sealed partial class BatchApplier
{
    private readonly Dictionary<string, DataVoPreparedStatement> _transactionalStatements = new(StringComparer.Ordinal);

    /// <summary>
    /// Writes the prepared batch into <paramref name="transaction"/>, counting rows the way <see cref="Execute"/> does.
    /// </summary>
    /// <param name="sessionId">The session that owns the transaction.</param>
    /// <param name="transaction">The session's open transaction.</param>
    /// <returns>What happened to the rows, as seen by the transaction.</returns>
    public DataVoBatchResult ExecuteInTransaction(Guid sessionId, TransactionContext transaction)
    {
        string savepoint = "__datavo_batch_" + Guid.NewGuid().ToString("N");
        RunStatement($"SAVEPOINT {savepoint}", sessionId, []);
        try
        {
            DataVoBatchResult result = WriteInTransaction(sessionId, transaction);
            RunStatement($"RELEASE SAVEPOINT {savepoint}", sessionId, []);
            return result;
        }
        catch
        {
            RunStatement($"ROLLBACK TO SAVEPOINT {savepoint}", sessionId, []);
            RunStatement($"RELEASE SAVEPOINT {savepoint}", sessionId, []);
            throw;
        }
    }

    private DataVoBatchResult WriteInTransaction(Guid sessionId, TransactionContext transaction)
    {
        var pendingInserts = new Dictionary<string, List<Dictionary<string, object?>>>(StringComparer.OrdinalIgnoreCase);
        int inserted = 0, updated = 0, deleted = 0;

        // Deletes first, then updates, then inserts (parents first): the order the auto-commit path writes in, so
        // keys and unique values a batch frees are available to the rows that claim them.
        foreach (TablePlan table in _order)
        {
            if (table.FastPathEligible)
            {
                DemoteTable(table);
            }

            foreach (PendingRow row in table.Rows)
            {
                Dictionary<string, object?>? existing = ReadVisibleRow(table, row.KeyValues, sessionId);
                if (row.IsDelete)
                {
                    if (existing is null)
                    {
                        _unchanged++;
                        continue;
                    }

                    RunStatement(DeleteSql(table), sessionId, KeyParameters(table, row.KeyValues));
                    deleted++;
                }
            }
        }

        foreach (TablePlan table in _order)
        {
            foreach (PendingRow row in table.Rows)
            {
                if (row.IsDelete)
                {
                    continue;
                }

                Dictionary<string, object?>? existing = ReadVisibleRow(table, row.KeyValues, sessionId);
                if (existing is null && row.UpdateOnly)
                {
                    _missing++;
                }
                else if (existing is null)
                {
                    if (!pendingInserts.TryGetValue(table.Name, out List<Dictionary<string, object?>>? rows))
                    {
                        rows = [];
                        pendingInserts[table.Name] = rows;
                    }

                    rows.Add(row.Values!);
                }
                else if (WriteTransactionalUpdate(table, row, existing, sessionId))
                {
                    updated++;
                }
                else
                {
                    _unchanged++;
                }
            }
        }

        long statementTxId = MVCC.MvccCoordinator.ResolveStatementTransactionId(context.Engine, transaction.TransactionId);
        var integrity = new BatchIntegrity(context.Engine, databaseName, InsertService);
        foreach (string table in integrity.ParentsFirst([.. pendingInserts.Keys]))
        {
            inserted += InsertService.InsertRows(databaseName, table, pendingInserts[table], transaction, statementTxId).AcceptedRowCount;
        }

        return new DataVoBatchResult(inserted, updated, deleted, _unchanged, _missing);
    }

    /// <summary>Sets a stored row to the folded values; returns <see langword="false"/> when nothing would change.</summary>
    private bool WriteTransactionalUpdate(TablePlan table, PendingRow row, Dictionary<string, object?> existing, Guid sessionId)
    {
        Dictionary<string, object?> next = row.Reset
            ? table.DefaultRow(row.KeyValues, BindAndNormalize)
            : new Dictionary<string, object?>(existing, StringComparer.OrdinalIgnoreCase);
        foreach (KeyValuePair<string, object?> column in row.Values!)
        {
            next[column.Key] = column.Value;
        }

        if (SameRow(existing, next))
        {
            return false;
        }

        List<string> assigned = [.. table.AllColumns
            .Select(static column => column.Name)
            .Where(name => !table.PrimaryKeys.Contains(name, StringComparer.OrdinalIgnoreCase))];
        if (assigned.Count == 0)
        {
            return false;
        }

        var parameters = new List<DataVoQueryParameter>(assigned.Count + table.PrimaryKeys.Count);
        for (int i = 0; i < assigned.Count; i++)
        {
            parameters.Add(DataVoQueryParameter.FromObject("v" + i, Materialize(next.TryGetValue(assigned[i], out object? value) ? value : null)));
        }

        parameters.AddRange(KeyParameters(table, row.KeyValues));
        string sql = $"UPDATE {table.Name} SET {string.Join(", ", assigned.Select(static (name, i) => $"{name} = @v{i}"))} WHERE {KeyPredicate(table)}";
        RunStatement(sql, sessionId, [.. parameters]);
        return true;
    }

    /// <summary>Reads the row with the given primary key as the transaction sees it, or <see langword="null"/>.</summary>
    private Dictionary<string, object?>? ReadVisibleRow(TablePlan table, Dictionary<string, object?> keyValues, Guid sessionId)
    {
        QueryResult result = RunStatement($"SELECT * FROM {table.Name} WHERE {KeyPredicate(table)}", sessionId, KeyParameters(table, keyValues));
        return result.Data.Count == 0 ? null : new Dictionary<string, object?>(result.Data[0], StringComparer.OrdinalIgnoreCase);
    }

    private static string DeleteSql(TablePlan table) => $"DELETE FROM {table.Name} WHERE {KeyPredicate(table)}";

    private static string KeyPredicate(TablePlan table) =>
        string.Join(" AND ", table.PrimaryKeys.Select(static (key, i) => $"{key} = @k{i}"));

    private static DataVoQueryParameter[] KeyParameters(TablePlan table, Dictionary<string, object?> keyValues) =>
        [.. table.PrimaryKeys.Select((key, i) => DataVoQueryParameter.FromObject("k" + i, Materialize(keyValues[key])))];

    /// <summary>Runs one statement on the session (prepared once per batch) and throws when it fails.</summary>
    private QueryResult RunStatement(string sql, Guid sessionId, DataVoQueryParameter[] parameters)
    {
        if (!_transactionalStatements.TryGetValue(sql, out DataVoPreparedStatement? statement))
        {
            statement = new DataVoPreparedStatement(context.Engine, sql, sessionId);
            _transactionalStatements[sql] = statement;
        }

        QueryResult result = statement.Execute(parameters, sessionId);
        return result.IsError
            ? throw new InvalidOperationException($"ApplyBatch inside a transaction failed: {string.Join(" | ", result.Messages)}")
            : result;
    }
}
