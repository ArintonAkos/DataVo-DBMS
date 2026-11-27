using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using DataVo.Core.Contracts.Results;

namespace DataVo.Core.CompiledQueries;

/// <summary>
/// Compiled updates inside an explicit transaction. The byte-patch fast paths write committed storage directly, so in
/// a transaction a compiled update instead runs as the equivalent parameterized <c>UPDATE</c> on the session: it
/// takes the transaction's row locks, sees its snapshot and buffered writes, and is applied by COMMIT (or discarded
/// by ROLLBACK) like any other statement of the transaction.
/// </summary>
public static partial class DataVoCompiledQuery
{
    private static readonly ConditionalWeakTable<DataVoContext, ConcurrentDictionary<DataVoCompiledQueryPlan, DataVoPreparedStatement>> TransactionalUpdateStatements = new();

    /// <summary>Gets whether the context's session has an open transaction.</summary>
    private static bool InTransaction(DataVoContext context) =>
        context.Engine.TransactionManager.HasActiveTransaction(context.SessionId);

    /// <summary>Runs an update plan as a statement of the session's open transaction.</summary>
    /// <returns>The number of rows the statement updated.</returns>
    /// <exception cref="InvalidOperationException">The statement failed (for example a constraint or MVCC conflict).</exception>
    private static int UpdateInTransaction(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        IReadOnlyList<DataVoCompiledQueryParameter> parameters)
    {
        DataVoPreparedStatement statement = TransactionalUpdateStatements
            .GetOrCreateValue(context)
            .GetOrAdd(plan, static (key, ctx) => new DataVoPreparedStatement(ctx.Engine, TransactionalUpdateSql(key), ctx.SessionId), context);

        var bound = new DataVoQueryParameter[parameters.Count];
        for (int i = 0; i < bound.Length; i++)
        {
            bound[i] = DataVoQueryParameter.FromObject(parameters[i].Name, context.BindValueForWrite(parameters[i].Value));
        }

        QueryResult result = statement.Execute(bound, context.SessionId);
        return result.IsError
            ? throw new InvalidOperationException($"Compiled update inside a transaction failed: {string.Join(" | ", result.Messages)}")
            : Math.Max(0, result.RowsAffected);
    }

    /// <summary>
    /// Runs a batch of primary-key updates as statements of the session's open transaction, under a savepoint so the
    /// batch is all-or-nothing inside the transaction.
    /// </summary>
    private static int UpdateBatchInTransaction(
        DataVoContext context,
        DataVoCompiledQueryPlan plan,
        IReadOnlyList<DataVoFixedWidthUpdateBatchEntry> updates)
    {
        if (plan.Assignments.Count > 2)
        {
            throw new NotSupportedException("Batched fixed-width primary-key updates carry at most two assignment values.");
        }

        string savepoint = "__datavo_update_batch_" + Guid.NewGuid().ToString("N");
        RunTransactionControl(context, $"SAVEPOINT {savepoint}");
        try
        {
            int affected = 0;
            for (int i = 0; i < updates.Count; i++)
            {
                DataVoFixedWidthUpdateBatchEntry update = updates[i];
                affected += UpdateInTransaction(context, plan, PrimaryKeyUpdateParameters(plan, update.PrimaryKey, [update.Value0, update.Value1]));
            }

            RunTransactionControl(context, $"RELEASE SAVEPOINT {savepoint}");
            return affected;
        }
        catch
        {
            RunTransactionControl(context, $"ROLLBACK TO SAVEPOINT {savepoint}");
            RunTransactionControl(context, $"RELEASE SAVEPOINT {savepoint}");
            throw;
        }
    }

    /// <summary>
    /// Names fixed-width primary-key update values after the plan's parameters: assignment values in plan order, then
    /// the primary key (extra values beyond the plan's assignments are ignored).
    /// </summary>
    private static DataVoCompiledQueryParameter[] PrimaryKeyUpdateParameters(
        DataVoCompiledQueryPlan plan,
        DataVoFixedWidthValue primaryKeyValue,
        ReadOnlySpan<DataVoFixedWidthValue> assignmentValues)
    {
        var parameters = new DataVoCompiledQueryParameter[plan.Assignments.Count + 1];
        int index = 0;
        foreach ((_, string parameterName) in plan.Assignments)
        {
            parameters[index] = new DataVoCompiledQueryParameter(parameterName, assignmentValues[index].ToObject());
            index++;
        }

        parameters[index] = new DataVoCompiledQueryParameter(plan.WhereParameterName!, primaryKeyValue.ToObject());
        return parameters;
    }

    private static string TransactionalUpdateSql(DataVoCompiledQueryPlan plan) =>
        $"UPDATE {plan.TableName} SET {string.Join(", ", plan.Assignments.Select(static pair => $"{pair.Key} = @{pair.Value}"))} " +
        $"WHERE {plan.WhereColumn} = @{plan.WhereParameterName}";

    private static void RunTransactionControl(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql)[^1];
        if (result.IsError)
        {
            throw new InvalidOperationException($"{sql} failed: {string.Join(" | ", result.Messages)}");
        }
    }
}
