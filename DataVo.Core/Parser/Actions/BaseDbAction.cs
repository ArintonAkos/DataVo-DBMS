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

    /// <summary>Records a failure from an exception; the first recorded failure decides the error code.</summary>
    /// <param name="ex">The exception that failed the statement.</param>
    protected void AddError(Exception ex)
    {
        DataVoError error = DataVoErrorClassifier.Classify(ex);
        HasErrors = true;
        _firstError ??= error;
        Messages.Add(error.Message);
    }

    /// <summary>
    /// Records the failure of a nested statement, keeping its messages, error code, and source position.
    /// </summary>
    /// <param name="failed">The failed result of the nested statement.</param>
    protected void AddError(QueryResult failed)
    {
        HasErrors = true;
        _firstError ??= new DataVoError(
            failed.ErrorCode ?? DataVoErrorCodes.General,
            failed.Messages.FirstOrDefault() ?? "The statement failed.",
            failed.ErrorLine,
            failed.ErrorColumn);
        Messages.AddRange(failed.Messages);
    }

    private DataVoError? _firstError;

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
            _firstError = null;

            using IDisposable _ = Engine.SessionSecurity.PushAmbientPrincipalForSession(session);
            Engine.SessionSecurity.Authorize(session, RequiredPermission, Engine.Config);

            PerformAction(session);
            if (HasErrors)
            {
                return BuildErrorResult();
            }

            return QueryResult.Success(Messages, Data, Fields);
        }
        catch (Exception ex)
        {
            return QueryResult.FromException(ex);
        }
    }

    /// <summary>
    ///     Do actions on the Messages, Fields, Data fields.
    ///     These values will be returned.
    /// </summary>
    public abstract void PerformAction(Guid session);

    // The first recorded error decides the code and position; every error message is kept in order.
    private QueryResult BuildErrorResult()
    {
        DataVoError resolved = _firstError
            ?? new DataVoError(DataVoErrorCodes.General, Messages.FirstOrDefault() ?? "The statement failed.", null, null);
        return new QueryResult
        {
            Messages = Messages,
            IsError = true,
            Data = Data,
            Fields = Fields,
            ErrorCode = resolved.Code,
            ErrorLine = resolved.Line,
            ErrorColumn = resolved.Column,
        };
    }
}
