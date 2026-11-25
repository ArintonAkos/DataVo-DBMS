using System.Collections.Concurrent;
using DataVo.Core;
using DataVo.Core.Contracts.Results;

namespace DataVo.VectorData;

/// <summary>
/// DataVo sessions bound to the vector store's database. A DataVo session carries its current database and open
/// transaction, so each operation leases its own: concurrent operations never share a transaction, and none has to
/// run <c>USE</c>. A lease also caches the statements it prepares, keyed by SQL text, until the store's schema
/// changes (a collection is created or dropped).
/// </summary>
internal sealed class DataVoSessionPool(DataVoContext context, string databaseName)
{
    private const int MaxPooled = 32;
    private const int MaxStatementsPerSession = 128;

    private readonly ConcurrentBag<Lease> _idle = [];
    private int _schemaVersion;

    public DataVoContext Context { get; } = context;

    public string DatabaseName { get; } = databaseName;

    /// <summary>Invalidates every lease's prepared statements (a table was created or dropped).</summary>
    public void SchemaChanged() => Interlocked.Increment(ref _schemaVersion);

    /// <summary>Takes a session bound to the database; dispose the lease to return it.</summary>
    public Lease Rent()
    {
        if (!_idle.TryTake(out Lease? lease))
        {
            lease = new Lease(this, Guid.NewGuid());
            Context.Engine.Sessions.Set(lease.Session, DatabaseName);
        }

        lease.Activate(Volatile.Read(ref _schemaVersion));
        return lease;
    }

    private void Return(Lease lease)
    {
        if (_idle.Count < MaxPooled)
        {
            _idle.Add(lease);
        }
    }

    /// <summary>One leased session and its prepared statements.</summary>
    internal sealed class Lease(DataVoSessionPool pool, Guid session) : IDisposable
    {
        private readonly Dictionary<string, DataVoPreparedStatement> _statements = new(StringComparer.Ordinal);
        private int _schemaVersion = -1;

        public Guid Session { get; } = session;

        public void Activate(int schemaVersion)
        {
            if (schemaVersion != _schemaVersion)
            {
                _statements.Clear();
                _schemaVersion = schemaVersion;
            }
        }

        /// <summary>Runs unparameterized SQL (DDL, BEGIN/COMMIT/ROLLBACK) and returns its last result.</summary>
        public QueryResult Execute(string sql) => pool.Context.Execute(sql, Session)[^1];

        /// <summary>Runs <paramref name="sql"/> as a prepared statement with <paramref name="parameters"/>.</summary>
        public QueryResult Execute(string sql, params DataVoQueryParameter[] parameters)
        {
            if (!_statements.TryGetValue(sql, out DataVoPreparedStatement? statement))
            {
                if (_statements.Count >= MaxStatementsPerSession)
                {
                    _statements.Clear();
                }

                statement = pool.Context.Prepare(sql, Session);
                _statements[sql] = statement;
            }

            return statement.Execute(parameters);
        }

        public void Dispose() => pool.Return(this);
    }
}
