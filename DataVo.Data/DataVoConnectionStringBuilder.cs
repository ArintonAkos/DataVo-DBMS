using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Data;

/// <summary>
/// Parses and builds connection strings for DataVo database connections.
/// <para>
/// Supported keys: <c>StorageMode</c> (<c>InMemory</c> | <c>Lsm</c>), <c>DataSource</c> (the data directory in
/// Lsm mode, and the in-memory engine name), and <c>Database</c> (the SQL database selected on open; derived from
/// the last segment of <c>DataSource</c> when omitted).
/// </para>
/// </summary>
/// <example>
/// <code>
/// var builder = new DataVoConnectionStringBuilder("StorageMode=Lsm;DataSource=./mydb");
/// builder.StorageMode // StorageMode.Lsm
/// builder.DataSource  // "./mydb"
/// builder.Database    // "mydb"
/// builder.StorageMode = StorageMode.InMemory;
/// builder.ConnectionString // "StorageMode=InMemory;DataSource=./mydb"
/// </code>
/// </example>
public class DataVoConnectionStringBuilder : DbConnectionStringBuilder
{
    private const string StorageModeKey = "StorageMode";
    private const string DataSourceKey = "DataSource";
    private const string DatabaseKey = "Database";
    private const string DefaultDataSource = "datavo";

    /// <summary>
    /// Initializes an empty builder (InMemory storage, data source <c>datavo</c>).
    /// </summary>
    public DataVoConnectionStringBuilder()
    {
    }

    /// <summary>
    /// Initializes a new builder by parsing a semicolon-delimited connection string.
    /// </summary>
    /// <param name="connectionString">The connection string to parse.</param>
    public DataVoConnectionStringBuilder(string? connectionString)
    {
        ConnectionString = connectionString ?? string.Empty;
    }

    /// <summary>
    /// Gets or sets the storage mode: <see cref="StorageMode.InMemory"/> (the default) or <see cref="StorageMode.Lsm"/>.
    /// </summary>
    /// <exception cref="ArgumentException">The connection string names, or the setter receives, any other storage mode.</exception>
    public StorageMode StorageMode
    {
        get
        {
            if (!TryGetString(StorageModeKey, out string? mode))
            {
                return StorageMode.InMemory;
            }

            if (string.Equals(mode, nameof(StorageMode.InMemory), StringComparison.OrdinalIgnoreCase))
            {
                return StorageMode.InMemory;
            }

            if (string.Equals(mode, nameof(StorageMode.Lsm), StringComparison.OrdinalIgnoreCase))
            {
                return StorageMode.Lsm;
            }

            throw UnknownStorageMode(mode);
        }
        set
        {
            if (value is not (StorageMode.InMemory or StorageMode.Lsm))
            {
                throw UnknownStorageMode(value.ToString());
            }

            this[StorageModeKey] = value.ToString();
        }
    }

    /// <summary>
    /// Gets or sets the data source: the <see cref="DataVoConfig.DiskStoragePath"/> in Lsm mode. Defaults to <c>datavo</c>.
    /// </summary>
    [AllowNull]
    public string DataSource
    {
        get => TryGetString(DataSourceKey, out string? value) && value.Length > 0 ? value : DefaultDataSource;
        set => SetOrRemove(DataSourceKey, value);
    }

    /// <summary>
    /// Gets or sets the SQL database selected when the connection opens. When not set, it is derived from the last
    /// path segment of <see cref="DataSource"/> (<c>./data/mydb</c> becomes <c>mydb</c>), with characters that are not
    /// valid in an identifier replaced by <c>_</c>.
    /// </summary>
    /// <exception cref="ArgumentException">An explicit value is not a valid SQL identifier.</exception>
    [AllowNull]
    public string Database
    {
        get
        {
            if (TryGetString(DatabaseKey, out string? explicitName) && explicitName.Length > 0)
            {
                return IsIdentifier(explicitName)
                    ? explicitName
                    : throw new ArgumentException($"Database '{explicitName}' in the connection string is not a valid identifier.");
            }

            return DeriveDatabaseName(DataSource);
        }
        set => SetOrRemove(DatabaseKey, value);
    }

    /// <summary>
    /// Converts this builder into a <see cref="DataVoConfig"/> suitable for initializing the storage engine.
    /// </summary>
    public DataVoConfig ToConfig()
    {
        return new DataVoConfig
        {
            StorageMode = StorageMode,
            DiskStoragePath = DataSource,
        };
    }

    /// <summary>
    /// Derives a SQL identifier from a data source path: its last segment, with non-identifier characters replaced
    /// by <c>_</c> and a leading <c>_</c> added when it would start with a digit.
    /// </summary>
    /// <param name="dataSource">The data source path or name.</param>
    /// <returns>A valid SQL identifier.</returns>
    internal static string DeriveDatabaseName(string dataSource)
    {
        string trimmed = dataSource.TrimEnd('/', '\\');
        int separator = trimmed.LastIndexOfAny(['/', '\\']);
        string segment = separator >= 0 ? trimmed.Substring(separator + 1) : trimmed;

        var name = new StringBuilder(segment.Length + 1);
        foreach (char c in segment)
        {
            name.Append(char.IsLetterOrDigit(c) || c == '_' ? c : '_');
        }

        if (name.Length == 0 || char.IsDigit(name[0]))
        {
            name.Insert(0, '_');
        }

        return name.ToString();
    }

    private static bool IsIdentifier(string name)
    {
        if (!(char.IsLetter(name[0]) || name[0] == '_'))
        {
            return false;
        }

        foreach (char c in name)
        {
            if (!(char.IsLetterOrDigit(c) || c == '_'))
            {
                return false;
            }
        }

        return true;
    }

    private bool TryGetString(string key, [NotNullWhen(true)] out string? value)
    {
        if (TryGetValue(key, out object? raw) && raw is not null)
        {
            value = raw.ToString()!.Trim();
            return true;
        }

        value = null;
        return false;
    }

    private void SetOrRemove(string key, string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            Remove(key);
        }
        else
        {
            this[key] = value;
        }
    }

    private static ArgumentException UnknownStorageMode(string mode) =>
        new($"Unknown StorageMode '{mode}' in the connection string: use InMemory or Lsm.");
}
