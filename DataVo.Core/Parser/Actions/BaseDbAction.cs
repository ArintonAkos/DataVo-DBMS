using DataVo.Core.Contracts;
using DataVo.Core.Contracts.Results;
using DataVo.Core.MongoDB;

namespace DataVo.Core.Parser.Actions;

internal abstract class BaseDbAction : IDbAction
{
    protected DbContext Context;
    protected List<Dictionary<string, dynamic>> Data = new();
    protected List<string> Fields = new();

    protected List<string> Messages = new();

    public BaseDbAction() => Context = DbContext.Instance;

    public QueryResult Perform(Guid session)
    {
        try {
            PerformAction(session);
            return QueryResult.Success(Messages, Data, Fields);
        } catch (Exception ex) {
            return QueryResult.Error(ex.Message);
        }
    }

    /// <summary>
    ///     Do actions on the Messages, Fields, Data fields.
    ///     These values will be returned.
    /// </summary>
    public abstract void PerformAction(Guid session);
}
