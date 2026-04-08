using DataVo.Core.Contracts;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Exceptions;
using DataVo.Core.Indexing;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Security;
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
    protected List<Dictionary<string, object?>> Data = [];
    protected List<string> Fields = [];

    protected List<string> Messages = [];
    protected bool HasErrors;

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
            ?? throw new BindingException("No database in use!");
    }

    protected void SetDatabaseName(Guid session, string databaseName)
    {
        Sessions.Set(session, databaseName);
    }

    protected void AddError(string message)
    {
        HasErrors = true;
        Messages.Add(message);
    }

    protected void AddError(Exception ex)
    {
        HasErrors = true;
        Messages.Add(ex.ToString());
    }

    protected virtual DatabasePermission RequiredPermission => InferRequiredPermission();

    private DatabasePermission InferRequiredPermission()
    {
        string? namespaceName = GetType().Namespace;
        if (string.IsNullOrWhiteSpace(namespaceName))
        {
            return DatabasePermission.WriteData;
        }

        if (namespaceName.Contains(".Parser.DQL", StringComparison.Ordinal))
        {
            return DatabasePermission.ReadData;
        }

        if (namespaceName.Contains(".Parser.DML", StringComparison.Ordinal))
        {
            return DatabasePermission.WriteData;
        }

        if (namespaceName.Contains(".Parser.DDL", StringComparison.Ordinal))
        {
            return DatabasePermission.ManageSchema;
        }

        if (namespaceName.Contains(".Parser.Transactions", StringComparison.Ordinal))
        {
            return DatabasePermission.ManageTransactions;
        }

        if (namespaceName.Contains(".Parser.Commands", StringComparison.Ordinal))
        {
            return DatabasePermission.ReadData;
        }

        return DatabasePermission.WriteData;
    }

    public QueryResult Perform(Guid session)
    {
        try
        {
            Data.Clear();
            Fields.Clear();
            Messages.Clear();
            HasErrors = false;

            using IDisposable _ = Engine.SessionSecurity.PushAmbientPrincipalForSession(session);
            Engine.SessionSecurity.Authorize(session, RequiredPermission, Engine.Config);

            PerformAction(session);
            if (HasErrors || Messages.Any(m => m.StartsWith("Error:", StringComparison.OrdinalIgnoreCase)))
            {
                return new QueryResult { Messages = Messages, IsError = true, Data = Data, Fields = Fields };
            }

            return QueryResult.Success(Messages, Data, Fields);
        }
        catch (Exception ex)
        {
            return QueryResult.Error(ex.ToString());
        }
    }

    /// <summary>
    ///     Do actions on the Messages, Fields, Data fields.
    ///     These values will be returned.
    /// </summary>
    public abstract void PerformAction(Guid session);
}
