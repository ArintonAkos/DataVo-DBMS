using System.Xml.Serialization;
using System.ComponentModel;

namespace DataVo.Core.Models.Catalog;

/// <summary>
/// Represents an index file definition associated with a specific table.
/// </summary>
/// <example>
/// <code>
/// var index = new IndexFile { IndexFileName = "idx_name", AttributeNames = ["Name"] };
/// </code>
/// </example>
[Serializable]
[XmlRoot("IndexFile")]
public class IndexFile
{
    /// <summary>Gets or sets the logical name of the index.</summary>
    [XmlAttribute("IndexName")] public required string IndexFileName { get; set; }

    /// <summary>Gets or sets the list of column names that are indexed.</summary>
    [XmlArray("IndexAttributes")]
    [XmlArrayItem("IAttribute")]
    public required List<string> AttributeNames { get; set; }

    /// <summary>Gets or sets the logical index implementation kind (for example BTREE or HNSW).</summary>
    [XmlAttribute("IndexKind")]
    [DefaultValue("BTREE")]
    public string IndexKind { get; set; } = "BTREE";

    /// <summary>
    /// Gets or sets the distance metric of a vector index: <c>cosine</c> or <c>euclidean</c>.
    /// <see langword="null"/> means the default, cosine. Ignored by other index kinds.
    /// </summary>
    [XmlAttribute("Metric")]
    public string? Metric { get; set; }

    /// <summary>Gets the metric a vector index uses, with the default applied.</summary>
    [XmlIgnore]
    public string EffectiveMetric => Metric ?? CosineMetric;

    /// <summary>
    /// Gets whether this is a columnar index: a copy of the listed columns laid out for scans and aggregates.
    /// It holds no keys, so it never serves a row lookup.
    /// </summary>
    [XmlIgnore]
    public bool IsColumnar => string.Equals(IndexKind, ColumnarKind, StringComparison.OrdinalIgnoreCase);

    internal const string ColumnarKind = "COLUMNAR";
    internal const string CosineMetric = "cosine";
    internal const string EuclideanMetric = "euclidean";

    /// <summary>Maps a metric name to its stored form, or returns <see langword="null"/> when it is not one.</summary>
    /// <param name="metric">A metric name, such as cosine, euclidean or l2.</param>
    /// <returns><c>cosine</c>, <c>euclidean</c>, or <see langword="null"/>.</returns>
    public static string? NormalizeMetric(string? metric) => metric?.Trim().ToLowerInvariant() switch
    {
        "cosine" => CosineMetric,
        "euclidean" or "l2" => EuclideanMetric,
        _ => null,
    };
}