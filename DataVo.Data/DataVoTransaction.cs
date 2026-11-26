using System.Data;
using System.Data.Common;

namespace DataVo.Data;

/// <summary>
/// Represents a transaction against a DataVo database.
/// Created via <see cref="DbConnection.BeginTransaction()"/>. Disposing a transaction that was neither committed
/// nor rolled back rolls it back.
/// </summary>
/// <example>
/// <code>
/// using var tx = connection.BeginTransaction();
/// // ... DML operations ...
/// tx.Commit();
/// </code>
/// </example>
public class DataVoTransaction : DbTransaction
{
    private readonly DataVoConnection _connection;
    private readonly IsolationLevel _isolationLevel;
    private bool _completed;

    internal DataVoTransaction(DataVoConnection connection, IsolationLevel isolationLevel)
    {
        _connection = connection;
        _isolationLevel = isolationLevel;
    }

    /// <inheritdoc />
    protected override DbConnection? DbConnection => _completed ? null : _connection;

    /// <summary>
    /// Gets the isolation level the transaction runs at: always <see cref="IsolationLevel.Snapshot"/>.
    /// </summary>
    public override IsolationLevel IsolationLevel => _isolationLevel;

    /// <summary>Gets the connection the transaction was started on, even after it completed.</summary>
    internal DataVoConnection OwningConnection => _connection;

    /// <summary>Gets whether the transaction was committed or rolled back.</summary>
    internal bool IsCompleted => _completed;

#if NET8_0_OR_GREATER
    /// <inheritdoc />
    public override bool SupportsSavepoints => true;
#endif

    /// <summary>
    /// Flushes all buffered operations to the storage engine.
    /// </summary>
    public override void Commit()
    {
        EnsureNotCompleted();
        _connection.ExecuteInternal("COMMIT;");
        _completed = true;
    }

    /// <summary>
    /// Discards all buffered operations without writing to disk.
    /// </summary>
    public override void Rollback()
    {
        EnsureNotCompleted();
        _connection.ExecuteInternal("ROLLBACK;");
        _completed = true;
    }

    /// <inheritdoc />
#if NET8_0_OR_GREATER
    public override void Save(string savepointName)
#else
    public void Save(string savepointName)
#endif
    {
        EnsureNotCompleted();
        string normalizedName = NormalizeSavepointName(savepointName);
        _connection.ExecuteInternal($"SAVEPOINT {normalizedName};");
    }

    /// <inheritdoc />
#if NET8_0_OR_GREATER
    public override void Rollback(string savepointName)
#else
    public void Rollback(string savepointName)
#endif
    {
        EnsureNotCompleted();
        string normalizedName = NormalizeSavepointName(savepointName);
        _connection.ExecuteInternal($"ROLLBACK TO SAVEPOINT {normalizedName};");
    }

    /// <inheritdoc />
#if NET8_0_OR_GREATER
    public override void Release(string savepointName)
#else
    public void Release(string savepointName)
#endif
    {
        EnsureNotCompleted();
        string normalizedName = NormalizeSavepointName(savepointName);
        _connection.ExecuteInternal($"RELEASE SAVEPOINT {normalizedName};");
    }

    /// <summary>
    /// Rolls back the transaction when it is disposed before <see cref="Commit"/> or <see cref="Rollback()"/>, so an
    /// exception inside <c>using var tx = connection.BeginTransaction()</c> never leaves it open on the connection.
    /// </summary>
    protected override void Dispose(bool disposing)
    {
        if (disposing && !_completed)
        {
            _completed = true;
            if (_connection.State == ConnectionState.Open && _connection.HasActiveTransaction)
            {
                _connection.ExecuteInternal("ROLLBACK;");
            }
        }

        base.Dispose(disposing);
    }

    /// <summary>
    /// Maps a requested isolation level to the one DataVo runs: snapshot isolation, which satisfies every level up to
    /// and including <see cref="IsolationLevel.Snapshot"/>.
    /// </summary>
    /// <param name="requested">The level the caller asked for.</param>
    /// <returns><see cref="IsolationLevel.Snapshot"/>.</returns>
    /// <exception cref="NotSupportedException">The level is <see cref="IsolationLevel.Serializable"/> (snapshot
    /// isolation does not prevent write skew) or <see cref="IsolationLevel.Chaos"/>.</exception>
    internal static IsolationLevel ResolveIsolationLevel(IsolationLevel requested) => requested switch
    {
        IsolationLevel.Unspecified
            or IsolationLevel.ReadUncommitted
            or IsolationLevel.ReadCommitted
            or IsolationLevel.RepeatableRead
            or IsolationLevel.Snapshot => IsolationLevel.Snapshot,
        _ => throw new NotSupportedException(
            $"DataVo transactions run under snapshot isolation; IsolationLevel.{requested} is not supported. " +
            "Use Unspecified, ReadCommitted, RepeatableRead or Snapshot."),
    };

    private void EnsureNotCompleted()
    {
        if (_completed)
            throw new InvalidOperationException("This transaction has already been completed.");
    }

    private static string NormalizeSavepointName(string savepointName)
    {
        if (string.IsNullOrWhiteSpace(savepointName))
        {
            throw new ArgumentException("Savepoint name is required.", nameof(savepointName));
        }

        return savepointName;
    }
}
