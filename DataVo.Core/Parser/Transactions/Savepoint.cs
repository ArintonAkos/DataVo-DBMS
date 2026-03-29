using DataVo.Core.Logging;
using DataVo.Core.Parser.Actions;

namespace DataVo.Core.Parser.Transactions;

/// <summary>
/// Executes a <c>SAVEPOINT name</c> command by snapshotting buffered transaction
/// state under the given savepoint name.
/// </summary>
internal class Savepoint(string savepointName) : BaseDbAction
{
    private readonly string _savepointName = savepointName;

    public override void PerformAction(Guid session)
    {
        try
        {
            Transactions.Savepoint(session, _savepointName);
            Messages.Add($"Savepoint '{_savepointName}' created.");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add($"Error: {ex.Message}");
        }
    }
}
