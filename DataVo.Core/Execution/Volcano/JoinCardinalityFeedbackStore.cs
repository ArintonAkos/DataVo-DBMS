using System.Collections.Concurrent;
using System.Text.Json;
using DataVo.Core.Logging;
using DataVo.Core.Serialization;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Core.Execution.Volcano;

/// <summary>
/// Observed join output sizes, learned per join edge and blended into the Volcano planner's estimates. Each
/// engine owns one store, so independent engines never see each other's statistics; when persistence is
/// enabled the store loads its file on first use and rewrites it after every update.
/// </summary>
/// <param name="config">The owning engine's configuration.</param>
internal sealed class JoinCardinalityFeedbackStore(DataVoConfig config)
{
    private const double Alpha = 0.35d;

    private readonly object _sync = new();
    private readonly ConcurrentDictionary<string, double> _observedRows = new(StringComparer.OrdinalIgnoreCase);
    private string? _loadedPath;
    private bool _loaded;

    /// <summary>Gets the number of learned join edges.</summary>
    public int Count => _observedRows.Count;

    /// <summary>Looks up the learned output size of a join edge.</summary>
    /// <param name="edgeKey">The canonical join edge key.</param>
    /// <param name="learnedRows">The smoothed observed row count, at least 1.</param>
    /// <returns><see langword="false"/> when feedback is disabled or nothing was learned for the edge.</returns>
    public bool TryGet(string edgeKey, out long learnedRows)
    {
        learnedRows = 0;
        if (!config.EnableVolcanoJoinCardinalityFeedback)
        {
            return false;
        }

        EnsureLoaded();
        if (!_observedRows.TryGetValue(edgeKey, out double value))
        {
            return false;
        }

        learnedRows = Math.Max(1L, (long)Math.Round(value));
        return true;
    }

    /// <summary>
    /// Folds observed output sizes into the learned values (exponential smoothing), trims the store to its
    /// configured size, and persists it when enabled.
    /// </summary>
    /// <param name="observations">Each join edge and the rows it emitted.</param>
    public void Record(IEnumerable<(string EdgeKey, long ObservedRows)> observations)
    {
        if (!config.EnableVolcanoJoinCardinalityFeedback)
        {
            return;
        }

        EnsureLoaded();
        foreach ((string edgeKey, long observedRows) in observations)
        {
            double observed = Math.Max(1L, observedRows);
            _observedRows.AddOrUpdate(
                edgeKey,
                observed,
                (_, existing) => (existing * (1d - Alpha)) + (observed * Alpha));
        }

        lock (_sync)
        {
            Trim();
            Persist();
        }
    }

    private void EnsureLoaded()
    {
        string? path = PersistencePath();
        if (path is null)
        {
            return;
        }

        lock (_sync)
        {
            if (_loaded && string.Equals(_loadedPath, path, StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            _observedRows.Clear();
            try
            {
                if (File.Exists(path)
                    && JsonSerializer.Deserialize(File.ReadAllText(path), DataVoJsonContext.Default.StringDoubleMap) is { } loaded)
                {
                    foreach (KeyValuePair<string, double> entry in loaded)
                    {
                        if (!string.IsNullOrWhiteSpace(entry.Key) && entry.Value > 0)
                        {
                            _observedRows[entry.Key] = entry.Value;
                        }
                    }
                }
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or JsonException)
            {
                Logger.Info($"Planner: unable to load join feedback persistence: {ex.Message}");
            }

            _loadedPath = path;
            _loaded = true;
        }
    }

    // Keeps the edges with the largest outputs once the store exceeds its configured size. Caller holds _sync.
    private void Trim()
    {
        int maxEntries = Math.Max(16, config.VolcanoJoinCardinalityFeedbackMaxEntries);
        if (_observedRows.Count <= maxEntries)
        {
            return;
        }

        KeyValuePair<string, double>[] keep = [.. _observedRows
            .OrderByDescending(entry => entry.Value)
            .Take(maxEntries)];

        _observedRows.Clear();
        foreach (KeyValuePair<string, double> entry in keep)
        {
            _observedRows[entry.Key] = entry.Value;
        }
    }

    // Caller holds _sync.
    private void Persist()
    {
        string? path = PersistencePath();
        if (path is null)
        {
            return;
        }

        try
        {
            string directory = Path.GetDirectoryName(path) ?? string.Empty;
            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(directory);
            }

            var snapshot = _observedRows.ToDictionary(entry => entry.Key, entry => entry.Value, StringComparer.OrdinalIgnoreCase);
            File.WriteAllText(path, JsonSerializer.Serialize(snapshot, DataVoJsonContext.Default.StringDoubleMap));
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            Logger.Info($"Planner: unable to persist join feedback: {ex.Message}");
        }
    }

    private string? PersistencePath() =>
        config.EnableVolcanoJoinCardinalityFeedbackPersistence
        && !string.IsNullOrWhiteSpace(config.VolcanoJoinCardinalityFeedbackPersistenceFile)
            ? config.VolcanoJoinCardinalityFeedbackPersistenceFile
            : null;
}
