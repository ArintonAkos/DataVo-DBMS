using DataVo.Core.StorageEngine.Config;

namespace DataVo.Data;

/// <summary>
/// Parses and constructs connection strings for DataVo database connections.
/// <para>
/// Supported keys: <c>StorageMode</c> (InMemory | Disk), <c>DataSource</c> (path for Disk mode),
/// <c>WalEnabled</c> (true | false).
/// </para>
/// </summary>
/// <example>
/// <code>
/// var builder = new DataVoConnectionStringBuilder("StorageMode=Disk;DataSource=./mydb");
/// builder.StorageMode // StorageMode.Disk
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
    /// Gets the storage mode. Defaults to <see cref="StorageMode.InMemory"/>.
    /// </summary>
    public StorageMode StorageMode =>
        _properties.TryGetValue("StorageMode", out string? mode) &&
        Enum.TryParse<StorageMode>(mode, ignoreCase: true, out var parsed)
            ? parsed
            : StorageMode.InMemory;

    /// <summary>
    /// Gets the data source path. Required for Disk mode, used as both
    /// the <see cref="DataVoConfig.DiskStoragePath"/> and the database name.
    /// </summary>
    public string DataSource =>
        _properties.TryGetValue("DataSource", out string? ds) ? ds : "datavo";

    /// <summary>
    /// Gets whether Write-Ahead Logging is enabled. Defaults to <c>null</c>
    /// (auto-determined by config: true for Disk, false for InMemory).
    /// </summary>
    public bool? WalEnabled =>
        _properties.TryGetValue("WalEnabled", out string? val) && bool.TryParse(val, out bool result)
            ? result
            : null;

    /// <summary>
    /// Converts this builder into a <see cref="DataVoConfig"/> suitable for
    /// initializing the storage engine.
    /// </summary>
    public DataVoConfig ToConfig()
    {
        var config = new DataVoConfig
        {
            StorageMode = StorageMode,
            DiskStoragePath = DataSource,
        };

        if (WalEnabled.HasValue)
            config.WalEnabled = WalEnabled.Value;

        return config;
    }
}
