using DataVo.Core.StorageEngine.Config;

namespace DataVo.Data;

/// <summary>
/// Parses and constructs connection strings for DataVo database connections.
/// <para>
/// Supported keys: <c>StorageMode</c> (<c>InMemory</c> | <c>Lsm</c>) and <c>DataSource</c> (the data
/// directory in Lsm mode, and the database name).
/// </para>
/// </summary>
/// <example>
/// <code>
/// var builder = new DataVoConnectionStringBuilder("StorageMode=Lsm;DataSource=./mydb");
/// builder.StorageMode // StorageMode.Lsm
/// builder.DataSource  // "./mydb"
/// </code>
/// </example>
public class DataVoConnectionStringBuilder
{
    private readonly Dictionary<string, string> _properties = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>
    /// Initializes a new builder by parsing a semicolon-delimited connection string.
    /// </summary>
    /// <param name="connectionString">The connection string to parse.</param>
    public DataVoConnectionStringBuilder(string connectionString)
    {
        foreach (string segment in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            int eqIndex = segment.IndexOf('=');
            if (eqIndex <= 0) continue;

            string key = segment[..eqIndex].Trim();
            string value = segment[(eqIndex + 1)..].Trim();
            _properties[key] = value;
        }
    }

    /// <summary>
    /// Gets the storage mode: <see cref="StorageMode.InMemory"/> (the default) or <see cref="StorageMode.Lsm"/>.
    /// </summary>
    /// <exception cref="ArgumentException">The connection string names any other storage mode.</exception>
    public StorageMode StorageMode
    {
        get
        {
            if (!_properties.TryGetValue("StorageMode", out string? mode))
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

            throw new ArgumentException($"Unknown StorageMode '{mode}' in the connection string: use InMemory or Lsm.");
        }
    }

    /// <summary>
    /// Gets the data source: the <see cref="DataVoConfig.DiskStoragePath"/> in Lsm mode, and the database name.
    /// </summary>
    public string DataSource =>
        _properties.TryGetValue("DataSource", out string? ds) ? ds : "datavo";

    /// <summary>
    /// Converts this builder into a <see cref="DataVoConfig"/> suitable for
    /// initializing the storage engine.
    /// </summary>
    public DataVoConfig ToConfig()
    {
        return new DataVoConfig
        {
            StorageMode = StorageMode,
            DiskStoragePath = DataSource,
        };
    }
}
