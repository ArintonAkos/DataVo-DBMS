using DataVo.Core.StorageEngine.Config;
using DataVo.Data;

namespace DataVo.EntityFrameworkCore.Infrastructure.Internal;

/// <summary>
/// Opens the DataVo connections used by bridge operations and owns the storage lifecycle behind them.
/// </summary>
/// <remarks>
/// Lsm data directories are shared through the engine registry, so every operation opens its own
/// connection. An in-memory engine only lives as long as a connection to it, so the bridge keeps one
/// connection per in-memory data source open for the life of the process (until it is deleted) and
/// serializes the operations that use it.
/// </remarks>
internal static class DataVoBridgeStorage
{
    private static readonly object Gate = new();
    private static readonly Dictionary<string, InMemoryStore> InMemoryStores = new(StringComparer.Ordinal);

    /// <summary>
    /// Opens a connection for one bridge operation. Dispose the result when the operation completes.
    /// </summary>
    internal static DataVoBridgeConnection Open(string connectionString)
    {
        var builder = new DataVoConnectionStringBuilder(connectionString);
        if (builder.StorageMode != StorageMode.InMemory)
        {
            var connection = new DataVoConnection(connectionString);
            try
            {
                connection.Open();
            }
            catch
            {
                connection.Dispose();
                throw;
            }

            return new DataVoBridgeConnection(connection, store: null);
        }

        string key = GetInMemoryKey(builder);
        while (true)
        {
            InMemoryStore store;
            lock (Gate)
            {
                if (!InMemoryStores.TryGetValue(key, out store!))
                {
                    store = InMemoryStore.Open(connectionString);
                    InMemoryStores[key] = store;
                }
            }

            Monitor.Enter(store.SyncRoot);
            if (!store.IsDropped)
            {
                return new DataVoBridgeConnection(store.Connection, store);
            }

            // Deleted between lookup and acquisition: retry against a fresh store.
            Monitor.Exit(store.SyncRoot);
        }
    }

    /// <summary>
    /// Deletes the storage behind <paramref name="connectionString"/>: the shared in-memory engine or the
    /// Lsm data directory. Returns <see langword="true"/> when something existed and was deleted.
    /// </summary>
    internal static bool EnsureDeleted(string connectionString)
    {
        var builder = new DataVoConnectionStringBuilder(connectionString);
        if (builder.StorageMode == StorageMode.InMemory)
        {
            return DropInMemoryStore(GetInMemoryKey(builder));
        }

        if (string.IsNullOrWhiteSpace(builder.DataSource))
        {
            return false;
        }

        string directoryPath = Path.GetFullPath(builder.DataSource);
        if (!Directory.Exists(directoryPath))
        {
            return false;
        }

        Directory.Delete(directoryPath, recursive: true);
        return true;
    }

    private static bool DropInMemoryStore(string key)
    {
        InMemoryStore? store;
        lock (Gate)
        {
            if (!InMemoryStores.Remove(key, out store))
            {
                return false;
            }
        }

        lock (store.SyncRoot)
        {
            store.Drop();
        }

        return true;
    }

    private static string GetInMemoryKey(DataVoConnectionStringBuilder builder)
        => $"{builder.DataSource}\n{builder.Database}";

    /// <summary>
    /// One process-wide in-memory engine, kept alive by a connection that bridge operations share.
    /// </summary>
    internal sealed class InMemoryStore
    {
        private InMemoryStore(DataVoConnection connection)
        {
            Connection = connection;
        }

        /// <summary>Serializes the bridge operations that share <see cref="Connection"/>.</summary>
        public object SyncRoot { get; } = new();

        /// <summary>The connection that keeps the engine alive.</summary>
        public DataVoConnection Connection { get; }

        /// <summary>Whether the store was deleted; a dropped store must not be used.</summary>
        public bool IsDropped { get; private set; }

        public static InMemoryStore Open(string connectionString)
        {
            var connection = new DataVoConnection(connectionString);
            try
            {
                connection.Open();
            }
            catch
            {
                connection.Dispose();
                throw;
            }

            return new InMemoryStore(connection);
        }

        public void Drop()
        {
            IsDropped = true;
            Connection.Dispose();
        }
    }
}

/// <summary>
/// A connection leased for one bridge operation. Disposing it closes a private connection or releases
/// the shared in-memory connection to the next operation.
/// </summary>
internal sealed class DataVoBridgeConnection : IDisposable
{
    private readonly DataVoBridgeStorage.InMemoryStore? _store;
    private bool _disposed;

    internal DataVoBridgeConnection(DataVoConnection connection, DataVoBridgeStorage.InMemoryStore? store)
    {
        Connection = connection;
        _store = store;
    }

    /// <summary>The open connection to run statements on.</summary>
    public DataVoConnection Connection { get; }

    /// <summary>Creates a command for <paramref name="statement"/> with its parameters bound.</summary>
    public DataVoCommand CreateCommand(DataVoSqlStatement statement)
    {
        var command = Connection.CreateCommand();
        command.CommandText = statement.Sql;
        foreach (var (name, value) in statement.Parameters)
        {
            command.Parameters.Add(new DataVoParameter { ParameterName = name, Value = value ?? DBNull.Value });
        }

        return command;
    }

    /// <summary>Runs a statement and returns the number of affected rows.</summary>
    public int ExecuteNonQuery(DataVoSqlStatement statement)
    {
        using var command = CreateCommand(statement);
        return command.ExecuteNonQuery();
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        if (_store is null)
        {
            Connection.Dispose();
        }
        else
        {
            Monitor.Exit(_store.SyncRoot);
        }
    }
}

/// <summary>
/// A SQL statement with its parameter values, ready to bind to a <see cref="DataVoCommand"/>.
/// </summary>
internal sealed record DataVoSqlStatement(string Sql, IReadOnlyList<(string Name, object? Value)> Parameters)
{
    /// <summary>Creates a statement without parameters.</summary>
    public static DataVoSqlStatement Text(string sql) => new(sql, []);
}

/// <summary>
/// Collects parameter values while SQL text is being built, handing out <c>@pN</c> placeholders.
/// </summary>
internal sealed class DataVoSqlParameterList
{
    private readonly List<(string Name, object? Value)> _parameters = [];

    /// <summary>The parameters collected so far.</summary>
    public IReadOnlyList<(string Name, object? Value)> Parameters => _parameters;

    /// <summary>Adds <paramref name="value"/> (converted for DataVo) and returns its placeholder.</summary>
    public string Add(object? value)
    {
        string name = $"@p{_parameters.Count}";
        _parameters.Add((name, DataVoBridgeValues.ToDataVoValue(value)));
        return name;
    }
}
