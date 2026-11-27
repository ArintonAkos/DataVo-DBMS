using System.Runtime.InteropServices.JavaScript;
using System.Runtime.Versioning;
using System.Text.Json;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Indexing.HNSW;
using DataVo.Core.Parser;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Browser;

/// <summary>
/// JavaScript-facing entry points of the DataVo WebAssembly runtime. Every SQL call answers with a JSON
/// <c>QueryResult[]</c>; failures are results with <c>IsError</c> set, never a different shape.
/// </summary>
[SupportedOSPlatform("browser")]
public partial class DataVoInterop
{
    public static void Main() { }

    private static DataVoEngine? _engine;
    private static readonly Guid _session = Guid.NewGuid();

    // What OPFS/localStorage currently holds, so read-only or failed calls skip the catalog write.
    private static long _persistedCatalogGeneration = -1;
    private static string _persistedDatabase = string.Empty;

    [JSImport("globalThis.DataVoStorage.readCatalog")]
    internal static partial string? ReadCatalogState();

    [JSImport("globalThis.DataVoStorage.writeCatalog")]
    internal static partial void WriteCatalogState(string xml);

    [JSImport("globalThis.DataVoStorage.readSelectedDatabase")]
    internal static partial string? ReadSelectedDatabase();

    [JSImport("globalThis.DataVoStorage.writeSelectedDatabase")]
    internal static partial void WriteSelectedDatabase(string databaseName);

    [JSImport("globalThis.DataVoStorage.clearAllStorage")]
    internal static partial void ClearAllStorage();

    [JSImport("globalThis.DataVoStorage.getStorageBackendKind")]
    internal static partial string? GetStorageBackendKind();

    [JSImport("globalThis.DataVoStorage.getStorageCapabilities")]
    internal static partial string? GetStorageCapabilities();

    [JSImport("globalThis.DataVoStorage.getRuntimeFlags")]
    internal static partial string? GetRuntimeFlags();

    /// <summary>Starts the engine over browser storage and restores the persisted catalog and indexes.</summary>
    [JSExport]
    public static void Initialize()
    {
        if (_engine != null) return;

        BrowserRuntimeFlags.ForceVectorFallback = ResolveForceVectorFallback();

        var config = new DataVoConfig
        {
            StorageMode = StorageMode.Custom,
            CustomStorageEngine = new BrowserStorageEngine()
        };
        _engine = DataVoEngine.Initialize(config);

        var catalogState = ReadCatalogState();
        if (!string.IsNullOrWhiteSpace(catalogState))
        {
            _engine.Catalog.LoadState(catalogState);
        }

        _engine.RebuildIndexesFromStorage();

        var selectedDatabase = ReadSelectedDatabase();
        if (!string.IsNullOrWhiteSpace(selectedDatabase))
        {
            _engine.Sessions.Set(_session, selectedDatabase);
        }

        // Storage already holds exactly this state.
        MarkPersisted();
    }

    /// <summary>Runs a SQL batch and returns its results as a JSON <c>QueryResult[]</c>.</summary>
    [JSExport]
    public static string ExecuteSql(string sql)
    {
        if (_engine == null)
        {
            return SerializeResults([QueryResult.Error("Engine not initialized. Call Initialize() first.")]);
        }

        try
        {
            return SerializeResults(new QueryEngine(sql, _session, _engine).Parse());
        }
        catch (Exception ex)
        {
            return SerializeResults([QueryResult.FromException(ex)]);
        }
        finally
        {
            PersistBrowserStateIfChanged();
        }
    }

    /// <summary>Wipes all DataVo browser storage and stops the engine.</summary>
    [JSExport]
    public static void ResetStorage()
    {
        ClearAllStorage();
        _engine?.Dispose();
        _engine = null;
        _persistedCatalogGeneration = -1;
        _persistedDatabase = string.Empty;
    }

    /// <summary>Returns the storage backend capabilities reported by the JS host, as JSON.</summary>
    [JSExport]
    public static string RuntimeCapabilities()
    {
        string backendKind = "unknown";
        string? capabilitiesJson = null;

        try
        {
            backendKind = GetStorageBackendKind() ?? "unknown";
        }
        catch
        {
            // Keep unknown fallback for hosts without the capability API.
        }

        try
        {
            capabilitiesJson = GetStorageCapabilities();
        }
        catch
        {
            // Optional API - continue with minimum capability payload.
        }

        if (!string.IsNullOrWhiteSpace(capabilitiesJson))
        {
            return capabilitiesJson!;
        }

        var fallback = new BrowserRuntimeCapabilities(backendKind, _engine != null, BrowserRuntimeFlags.ForceVectorFallback);
        return JsonSerializer.Serialize(fallback, BrowserJsonContext.Default.BrowserRuntimeCapabilities);
    }

    private static string SerializeResults(List<QueryResult> results) =>
        JsonSerializer.Serialize(results, BrowserJsonContext.Default.ListQueryResult);

    private static bool ResolveForceVectorFallback()
    {
        try
        {
            string? raw = GetRuntimeFlags();
            if (!string.IsNullOrWhiteSpace(raw)
                && JsonSerializer.Deserialize(raw, BrowserJsonContext.Default.BrowserRuntimeFlagsPayload) is { ForceVectorFallback: bool value })
            {
                return value;
            }
        }
        catch
        {
            // Keep safe default for constrained browser runtimes.
        }

        return true;
    }

    /// <summary>
    /// Writes the catalog and selected database only when a statement changed them (DDL, USE, database
    /// create/drop); the catalog generation advances on every schema change.
    /// </summary>
    private static void PersistBrowserStateIfChanged()
    {
        if (_engine == null)
        {
            return;
        }

        if (_engine.Catalog.Generation != _persistedCatalogGeneration)
        {
            WriteCatalogState(_engine.Catalog.ExportState());
        }

        string selectedDatabase = _engine.Sessions.Get(_session) ?? string.Empty;
        if (!string.Equals(selectedDatabase, _persistedDatabase, StringComparison.Ordinal))
        {
            WriteSelectedDatabase(selectedDatabase);
        }

        MarkPersisted();
    }

    private static void MarkPersisted()
    {
        _persistedCatalogGeneration = _engine!.Catalog.Generation;
        _persistedDatabase = _engine.Sessions.Get(_session) ?? string.Empty;
    }
}
