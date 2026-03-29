using System.Data.Common;

namespace DataVo.Data;

/// <summary>
/// Represents a transaction against a DataVo database.
/// Created via <see cref="DataVoConnection.BeginTransaction()"/>.
/// </summary>
/// <example>
/// <code>
/// using var tx = connection.BeginTransaction();
/// // ... DML operations ...
/// tx.Commit();
/// </code>
/// </example>
public class DataVoTransaction(DataVoConnection connection) : DbTransaction
{
    private bool _completed;

    /// <inheritdoc />
    protected override DbConnection DbConnection => connection;

    /// <inheritdoc />
    public override System.Data.IsolationLevel IsolationLevel => System.Data.IsolationLevel.Serializable;

    /// <summary>
    /// Flushes all buffered operations to the storage engine.
    /// </summary>
    public override void Commit()
    {
        EnsureNotCompleted();
        connection.ExecuteInternal("COMMIT;");
        _completed = true;
    }

    /// <summary>
    /// Discards all buffered operations without writing to disk.
    /// </summary>
    public override void Rollback()
    {
        EnsureNotCompleted();
        connection.ExecuteInternal("ROLLBACK;");
        _completed = true;
    }

    /// <inheritdoc />
    public override void Save(string savepointName)
    {
        EnsureNotCompleted();
        string normalizedName = NormalizeSavepointName(savepointName);
        connection.ExecuteInternal($"SAVEPOINT {normalizedName};");
    }

    /// <inheritdoc />
    public override void Rollback(string savepointName)
    {
        EnsureNotCompleted();
        string normalizedName = NormalizeSavepointName(savepointName);
        connection.ExecuteInternal($"ROLLBACK TO SAVEPOINT {normalizedName};");
    }

    /// <inheritdoc />
    public override void Release(string savepointName)
    {
        EnsureNotCompleted();
        string normalizedName = NormalizeSavepointName(savepointName);
        connection.ExecuteInternal($"RELEASE SAVEPOINT {normalizedName};");
    }

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
