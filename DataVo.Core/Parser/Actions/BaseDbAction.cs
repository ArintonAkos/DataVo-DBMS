using DataVo.Core.Contracts;
using DataVo.Core.Contracts.Results;
using DataVo.Core.BTree;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine;
using DataVo.Core.Transactions;

namespace DataVo.Core.Parser.Actions;

internal abstract class BaseDbAction : IDbAction
{
    protected DataVoEngine Engine;
    protected StorageContext Context;
    protected EngineCatalog Catalog;
    protected SessionDatabaseStore Sessions;
    protected TransactionManager Transactions;
    protected LockManager Locks;
    protected IndexManager Indexes;
    protected List<Dictionary<string, dynamic>> Data = [];
    protected List<string> Fields = [];

    protected List<string> Messages = [];

    public BaseDbAction()
    {
        Engine = DataVoEngine.Current();
        Context = Engine.StorageContext;
        Catalog = Engine.Catalog;
        Sessions = Engine.Sessions;
        Transactions = Engine.TransactionManager;
        Locks = Engine.LockManager;
        Indexes = Engine.IndexManager;
    }

    internal void UseEngine(DataVoEngine engine)
    {
        Engine = engine;
        Context = engine.StorageContext;
        Catalog = engine.Catalog;
        Sessions = engine.Sessions;
        Transactions = engine.TransactionManager;
        Locks = engine.LockManager;
        Indexes = engine.IndexManager;
    }

    protected string GetDatabaseName(Guid session)
    {
        return Sessions.Get(session)
            ?? throw new Exception("No database in use!");
    }

    protected void SetDatabaseName(Guid session, string databaseName)
    {
        Sessions.Set(session, databaseName);
    }

    public QueryResult Perform(Guid session)
    {
        try
        {
            PerformAction(session);
            if (Messages.Count > 0 && Messages.Any(m => m.Contains("error", StringComparison.CurrentCultureIgnoreCase) || m.Contains("exception", StringComparison.CurrentCultureIgnoreCase)))
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
