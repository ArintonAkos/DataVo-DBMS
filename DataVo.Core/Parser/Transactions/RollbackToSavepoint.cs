using DataVo.Core.Logging;
using DataVo.Core.Parser.Actions;

namespace DataVo.Core.Parser.Transactions;

/// <summary>
/// Executes a <c>ROLLBACK TO [SAVEPOINT] name</c> command by restoring buffered
/// transaction state to the specified savepoint.
/// </summary>
internal class RollbackToSavepoint(string savepointName) : BaseDbAction
{
    private readonly string _savepointName = savepointName;

    public override void PerformAction(Guid session)
    {
        try
        {
            Transactions.RollbackToSavepoint(session, _savepointName);
            Messages.Add($"Rolled back to savepoint '{_savepointName}'.");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add($"Error: {ex.Message}");
        }
    }
}
