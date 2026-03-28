using DataVo.Core.Logging;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Transactions;

namespace DataVo.Core.Parser.Transactions;

/// <summary>
/// Executes a <c>BEGIN [TRANSACTION]</c> command by opening an explicit transaction
/// scope on the <see cref="TransactionManager"/> for the current session.
/// </summary>
internal class BeginTransaction : BaseDbAction
{
    /// <summary>
    /// Opens a new transaction context for the given session with MVCC support.
    /// Allocates a transaction ID and creates a snapshot for snapshot isolation.
    /// If a transaction is already active, the error is caught and reported to the caller.
    /// </summary>
    /// <param name="session">The unique session identifier.</param>
    public override void PerformAction(Guid session)
    {
        try
        {
            var allocator = Engine.TransactionIdAllocator;
            Transactions.Begin(session, allocator);
            Messages.Add("Transaction started.");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add($"Error: {ex.Message}");
        }
    }
}
