namespace DataVo.Core.Annotations;

/// <summary>
/// Maps a class or record to a table. <c>DataVo.Generators</c> emits typed schema, lookup, insert and update
/// methods for the model at compile time, with no reflection at run time.
/// </summary>
/// <param name="tableName">The table the model is stored in.</param>
/// <example>
/// <code>
/// [DataVoTable("players")]
/// public sealed class Player
/// {
///     [PrimaryKey] public int Id { get; set; }
///     [Indexed] public int Level { get; set; }
///     public string Name { get; set; } = "";
/// }
///
/// context.EnsurePlayerTable();
/// context.Insert(new Player { Id = 1, Level = 5, Name = "Ann" });
/// Player? player = context.GetPlayerById(1);
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, AllowMultiple = false, Inherited = false)]
public sealed class DataVoTableAttribute(string tableName) : Attribute
{
    /// <summary>Gets the table name.</summary>
    public string TableName { get; } = tableName;
}

/// <summary>Marks the property that is the table's primary key. A model has exactly one.</summary>
/// <example>
/// <code>
/// [PrimaryKey] public int Id { get; set; }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Parameter, AllowMultiple = false, Inherited = false)]
public sealed class PrimaryKeyAttribute : Attribute;

/// <summary>Creates a secondary index on the property's column when the table is ensured.</summary>
/// <example>
/// <code>
/// [Indexed] public int Level { get; set; }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Parameter, AllowMultiple = false, Inherited = false)]
public sealed class IndexedAttribute : Attribute;

/// <summary>
/// Adds the property's column to the table's columnar index when the table is ensured. Counts, sums, minimums
/// and maximums whose condition and value read only such columns are then computed from dense runs of each
/// column, many values at a time, instead of row by row. Applies to <see cref="int"/>, <see cref="long"/>,
/// <see cref="float"/>, <see cref="double"/>, <see cref="bool"/> and date columns.
/// </summary>
/// <remarks>
/// The index is a second copy of the marked columns, kept in memory and updated on every insert and delete.
/// Mark the columns analytical queries filter and aggregate on, not every column.
/// </remarks>
/// <example>
/// <code>
/// [Columnar] public int Level { get; set; }
/// [Columnar] public long Gold { get; set; }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Parameter, AllowMultiple = false, Inherited = false)]
public sealed class ColumnarAttribute : Attribute;

/// <summary>
/// Creates a Euclidean HNSW index on a <see cref="System.Numerics.Vector3"/> position property when the table is
/// ensured, so <c>DataVoTable&lt;T&gt;.NearestNeighbors</c> is answered from the index instead of a scan.
/// </summary>
/// <example>
/// <code>
/// [SpatialIndex] public Vector3 Position { get; set; }
/// </code>
/// </example>
[AttributeUsage(AttributeTargets.Property | AttributeTargets.Parameter, AllowMultiple = false, Inherited = false)]
public sealed class SpatialIndexAttribute : Attribute;
