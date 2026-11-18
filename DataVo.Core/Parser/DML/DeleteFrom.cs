using DataVo.Core.Logging;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DML;
using DataVo.Core.Parser.Actions;
using DataVo.Core.BTree;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Serialization;
using DataVo.Core.Parser.AST;
using DataVo.Core.Models.Statement.Utils;
using DataVo.Core.Transactions;
using DataVo.Core.MVCC;
using DataVo.Core.Runtime.Changes;

namespace DataVo.Core.Parser.DML;

internal class DeleteFrom(DeleteFromStatement ast) : BaseDbAction
{
    private readonly DeleteFromModel _model = DeleteFromModel.FromAst(ast);

    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = GetDatabaseName(session);

            var txContext = Transactions.GetContext(session);
            long statementTxId = MvccCoordinator.ResolveStatementTransactionId(Engine, txContext?.TransactionId);
            if (txContext != null)
            {
                Messages.Add($"Rows affected: {DeleteInTransaction(txContext, databaseName)}");
            }
            else
            {
                List<RowKey> toBeDeleted = _model.WhereStatement.EvaluateWithoutJoin(_model.TableName, databaseName).ToList();

                if (toBeDeleted.Count == 0)
                {
                    Messages.Add("Rows affected: 0");
                    return;
                }

                List<RowKey> rowWriteLocks = Locks.AcquireRowWriteLocks(databaseName, _model.TableName, toBeDeleted);
                ChangeRecorder? recorder = ChangeRecorder.TryCreate(Engine, databaseName);
                try
                {
                    List<RowKey> revalidatedRowIds = RevalidateRowsAfterLock(databaseName, toBeDeleted, rowWriteLocks);
                    foreach (RowKey rowId in revalidatedRowIds)
                    {
                        MvccCoordinator.ValidateCanModifyRow(Engine, databaseName, _model.TableName, rowId, null, "DELETE");
                    }

                    RowDeleter.Delete(Engine, revalidatedRowIds, _model.TableName, databaseName, statementTxId, recorder);

                    Messages.Add($"Rows affected: {revalidatedRowIds.Count}");
                }
                finally
                {
                    Locks.ReleaseRowWriteLocks(databaseName, _model.TableName, rowWriteLocks);
                }

                recorder?.Publish();
            }
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            AddError(ex);
        }
    }

    /// <summary>
    /// Buffers the delete inside an explicit transaction. The WHERE clause reads through the transaction's snapshot
    /// and its own uncommitted writes, so rows it inserted or updated earlier match by their current values. Rows
    /// the transaction inserted are dropped from its insert buffer; stored rows are marked for deletion.
    /// </summary>
    /// <returns>The number of rows deleted.</returns>
    private int DeleteInTransaction(TransactionContext txContext, string databaseName)
    {
        List<RowKey> toBeDeleted;
        using (MvccExecutionScope.PushTransaction(txContext))
        {
            toBeDeleted = _model.WhereStatement.EvaluateWithoutJoin(_model.TableName, databaseName).Distinct().ToList();
        }

        txContext.ExcludeDeleted(_model.TableName, toBeDeleted);
        var buffered = new List<RowKey>();
        foreach (RowKey rowId in toBeDeleted)
        {
            if (TransactionContext.IsBufferedRowKey(rowId))
            {
                buffered.Add(rowId);
                continue;
            }

            MvccCoordinator.ValidateCanModifyRow(Engine, databaseName, _model.TableName, rowId, txContext.Snapshot, "DELETE");
        }

        foreach (RowKey rowId in toBeDeleted)
        {
            if (!TransactionContext.IsBufferedRowKey(rowId))
            {
                txContext.BufferDelete(_model.TableName, rowId);
            }
        }

        txContext.RemoveBufferedInserts(_model.TableName, buffered);
        return toBeDeleted.Count;
    }

    private List<RowKey> RevalidateRowsAfterLock(
        string databaseName,
        IReadOnlyList<RowKey> originalCandidates,
        IReadOnlyList<RowKey> lockCoveredRowIds)
    {
        if (originalCandidates.Count == 0)
        {
            return [];
        }

        HashSet<RowKey> lockCovered = [.. lockCoveredRowIds];
        HashSet<RowKey> originalSet = [.. originalCandidates];

        ExpressionNode whereExpression = _model.WhereStatement.GetExpression()!;
        if (IsTriviallyTrueWhere(whereExpression))
        {
            return [.. originalCandidates];
        }

        return _model.WhereStatement.EvaluateWithoutJoin(_model.TableName, databaseName)
            .Where(rowId => originalSet.Contains(rowId))
            .Where(rowId => rowId <= 0 || lockCovered.Count == 0 || lockCovered.Contains(rowId))
            .ToList();
    }

    private static bool IsTriviallyTrueWhere(ExpressionNode expression)
    {
        if (expression is LiteralNode literal)
        {
            string? value = literal.Value?.ToString();
            return value != null && value.Equals("true", StringComparison.OrdinalIgnoreCase);
        }

        if (expression is ColumnRefNode columnRef)
        {
            return columnRef.Column.Equals("true", StringComparison.OrdinalIgnoreCase);
        }

        if (expression is ResolvedColumnRefNode resolved)
        {
            return resolved.Column.Equals("true", StringComparison.OrdinalIgnoreCase);
        }

        if (expression is BinaryExpressionNode binary
            && binary.Operator == "="
            && binary.Left is LiteralNode leftLiteral
            && binary.Right is LiteralNode rightLiteral)
        {
            return string.Equals(leftLiteral.Value?.ToString(), rightLiteral.Value?.ToString(), StringComparison.Ordinal);
        }

        return false;
    }
}
