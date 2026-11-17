using System.Globalization;

namespace DataVo.Core.Models.Catalog;

/// <summary>
/// The graph parameters of an HNSW index given in <c>CREATE INDEX ... USING HNSW WITH (...)</c>. A value left
/// <see langword="null"/> uses the index's default. The catalog stores them, so an index rebuilt from its
/// table's rows is built the way it was created.
/// </summary>
/// <example>
/// <code>
/// CREATE INDEX vx ON Docs (Emb) USING HNSW WITH (m = 24, ef_construction = 128, enable_diversity_heuristic = false);
/// </code>
/// </example>
public sealed class HnswIndexOptions
{
    /// <summary>The smallest and largest accepted <see cref="M"/>.</summary>
    internal const int MinM = 2, MaxM = 128;

    /// <summary>The smallest and largest accepted <see cref="EfConstruction"/> and <see cref="EfSearch"/>.</summary>
    internal const int MinEf = 1, MaxEf = 4096;

    /// <summary>Gets or sets the links each node keeps per layer (twice as many on the bottom layer); option <c>m</c>.</summary>
    public int? M { get; set; }

    /// <summary>Gets or sets the candidates examined when a vector is inserted; option <c>ef_construction</c>.</summary>
    public int? EfConstruction { get; set; }

    /// <summary>Gets or sets the smallest candidate list of a search; option <c>ef_search</c>.</summary>
    public int? EfSearch { get; set; }

    /// <summary>
    /// Gets or sets whether a node prefers neighbors that are not close to one it already links to, which keeps
    /// links between clusters; option <c>enable_diversity_heuristic</c>.
    /// </summary>
    public bool? EnableDiversityHeuristic { get; set; }

    /// <summary>Gets whether no option is set.</summary>
    public bool IsEmpty => M is null && EfConstruction is null && EfSearch is null && EnableDiversityHeuristic is null;

    /// <summary>The option names accepted by <see cref="TrySet"/>, for error messages.</summary>
    internal const string OptionNames = "m, ef_construction, ef_search, enable_diversity_heuristic";

    /// <summary>
    /// Sets the option called <paramref name="name"/> (case-insensitive, underscores optional) from its SQL text.
    /// </summary>
    /// <param name="name">The option name as written, such as <c>ef_construction</c> or <c>EfConstruction</c>.</param>
    /// <param name="value">The option value as written.</param>
    /// <param name="error">Why the value was rejected; null when it was accepted or the name is not an HNSW option.</param>
    /// <returns><see langword="true"/> when the name is an HNSW option and the value was accepted.</returns>
    public bool TrySet(string name, string value, out string? error)
    {
        error = null;
        switch (name.Replace("_", string.Empty, StringComparison.Ordinal).ToLowerInvariant())
        {
            case "m":
                M = ParseInt(name, value, MinM, MaxM, out error);
                return error is null;
            case "efconstruction":
                EfConstruction = ParseInt(name, value, MinEf, MaxEf, out error);
                return error is null;
            case "efsearch":
                EfSearch = ParseInt(name, value, MinEf, MaxEf, out error);
                return error is null;
            case "enablediversityheuristic":
                if (bool.TryParse(value, out bool enabled))
                {
                    EnableDiversityHeuristic = enabled;
                    return true;
                }

                error = $"Index option {name} must be true or false, not '{value}'.";
                return false;
            default:
                return false;
        }
    }

    /// <summary>Returns a copy of these options.</summary>
    public HnswIndexOptions Clone() => (HnswIndexOptions)MemberwiseClone();

    /// <summary>Writes the set options into the parameters an HNSW index factory reads.</summary>
    internal void CopyTo(Dictionary<string, object> parameters)
    {
        if (M is { } m)
        {
            parameters["m"] = m;
        }

        if (EfConstruction is { } efConstruction)
        {
            parameters["efConstruction"] = efConstruction;
        }

        if (EfSearch is { } efSearch)
        {
            parameters["efSearch"] = efSearch;
        }

        if (EnableDiversityHeuristic is { } diversity)
        {
            parameters["enableDiversityHeuristic"] = diversity;
        }
    }

    private static int? ParseInt(string name, string value, int min, int max, out string? error)
    {
        if (int.TryParse(value, NumberStyles.Integer, CultureInfo.InvariantCulture, out int parsed) && parsed >= min && parsed <= max)
        {
            error = null;
            return parsed;
        }

        error = $"Index option {name} must be a whole number from {min} to {max}, not '{value}'.";
        return null;
    }
}
