using DataVo.Core.Logging;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Transactions;

namespace DataVo.Core.Parser.Transactions;

/// <summary>
/// Executes a <c>ROLLBACK</c> command by discarding all buffered DML operations
/// in the active <see cref="TransactionContext"/>. No changes are applied to disk.
/// </summary>
internal class Rollback : BaseDbAction
{
    /// <summary>
    /// Discards the transaction context for the given session.
    /// If no transaction is active, the error is caught and reported to the caller.
    /// </summary>
    /// <param name="session">The unique session identifier.</param>
    public override void PerformAction(Guid session)
    {
        try
        {
            Transactions.Rollback(session);
            Messages.Add("Transaction rolled back.");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add($"Error: {ex.Message}");
        }
    }
}
