using System.Data.Common;

namespace DataVo.Data;

/// <summary>
/// The ADO.NET provider factory for DataVo: creates connections, commands, parameters and connection-string
/// builders for code that works against <see cref="DbProviderFactory"/> (for example <c>DbProviderFactories</c>
/// registrations and provider-agnostic data layers).
/// </summary>
/// <example>
/// <code>
/// DbProviderFactories.RegisterFactory("DataVo", DataVoFactory.Instance);
/// using DbConnection connection = DataVoFactory.Instance.CreateConnection()!;
/// connection.ConnectionString = "StorageMode=InMemory;DataSource=app";
/// connection.Open();
/// </code>
/// </example>
public sealed class DataVoFactory : DbProviderFactory
{
    /// <summary>The single factory instance, as ADO.NET provider factories expose it.</summary>
    public static readonly DataVoFactory Instance = new();

    private DataVoFactory()
    {
    }

    /// <inheritdoc />
    public override DbConnection CreateConnection() => new DataVoConnection();

    /// <inheritdoc />
    public override DbCommand CreateCommand() => new DataVoCommand();

    /// <inheritdoc />
    public override DbParameter CreateParameter() => new DataVoParameter();

    /// <inheritdoc />
    public override DbConnectionStringBuilder CreateConnectionStringBuilder() => new DataVoConnectionStringBuilder();
}
