using DataVo.Core.Contracts;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine;

namespace DataVo.Core.Parser.Actions;

internal abstract class BaseDbAction : IDbAction
{
    protected StorageContext Context;
    protected List<Dictionary<string, dynamic>> Data = [];
    protected List<string> Fields = [];

    protected List<string> Messages = [];

    public BaseDbAction() => Context = StorageContext.Instance;

    public QueryResult Perform(Guid session)
    {
        try
        {
            PerformAction(session);
            if (Messages.Count > 0 && Messages.Any(m => m.ToLower().Contains("error") || m.ToLower().Contains("exception")))
            {
                return new QueryResult { Messages = Messages, IsError = true, Data = Data, Fields = Fields };
            }
            return QueryResult.Success(Messages, Data, Fields);
        }
        catch (Exception ex)
        {
            return QueryResult.Error(ex.Message);
        }
    }

    /// <summary>
    ///     Do actions on the Messages, Fields, Data fields.
    ///     These values will be returned.
    /// </summary>
    public abstract void PerformAction(Guid session);
}
