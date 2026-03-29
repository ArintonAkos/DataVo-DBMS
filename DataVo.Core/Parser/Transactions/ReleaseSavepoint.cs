using DataVo.Core.Logging;
using DataVo.Core.Parser.Actions;

namespace DataVo.Core.Parser.Transactions;

/// <summary>
/// Executes a <c>RELEASE [SAVEPOINT] name</c> command by removing a named
/// savepoint from the active transaction.
/// </summary>
internal class ReleaseSavepoint(string savepointName) : BaseDbAction
{
    private readonly string _savepointName = savepointName;

    public override void PerformAction(Guid session)
    {
        try
        {
            Transactions.ReleaseSavepoint(session, _savepointName);
            Messages.Add($"Transaction savepoint '{_savepointName}' released.");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add($"Error: {ex.Message}");
        }
    }
}
