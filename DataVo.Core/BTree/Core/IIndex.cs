namespace DataVo.Core.BTree.Core;

/// <summary>
/// Represents a B-Tree index that maps string keys to physical row IDs.
///
/// <para><b>Key format:</b></para>
/// <para>
/// A key is always a <c>string</c> derived from one or more column values in a row.
/// </para>
/// <list type="bullet">
///   <item>
///     <b>Single-column index</b> (e.g., index on <c>Name</c>):
///     the key is the column value itself → <c>"Alice"</c>
///   </item>
///   <item>
///     <b>Composite index</b> (e.g., index on <c>(Name, Age)</c>):
///     the key is the column values joined with <c>##</c> → <c>"Alice##30"</c>
///   </item>
///   <item>
///     <b>Primary key index</b> (<c>_PK_TableName</c>):
///     the key is the stringified row ID → <c>"1"</c>
///   </item>
///   <item>
///     <b>Unique key index</b> (<c>_UK_ColumnName</c>):
///     the key is the column value → <c>"alice@example.com"</c>
///   </item>
/// </list>
///
/// <para><b>Row ID:</b></para>
/// <para>
/// A <c>long</c> representing the physical position/identifier of the row in the storage engine.
/// Multiple keys can map to the same row ID (e.g., a row appears in several indexes),
/// and a single key can map to multiple row IDs (e.g., <c>"Alice"</c> → <c>[1, 5, 12]</c>).
/// </para>
///
/// <para>
/// Implementations (e.g., <c>JsonBTreeIndex</c>, <c>BinaryBTreeIndex</c>, <c>BinaryBPlusTreeIndex</c>)
/// define the on-disk layout, node splitting strategy, and serialization format.
/// </para>
/// </summary>
public interface IIndex
{
    /// <summary>
    /// Inserts a key → rowId mapping into the index.
    /// If the key already exists, the rowId is appended to its value list.
    /// </summary>
    /// <example>
    /// index.Insert("Alice", 1);  // key "Alice" now maps to [1]
    /// index.Insert("Alice", 5);  // key "Alice" now maps to [1, 5]
    /// index.Insert("Bob##25", 3); // composite key maps to [3]
    /// </example>
    void Insert(string key, long rowId);

    /// <summary>
    /// Removes all occurrences of the given row IDs across every key in the index.
    /// If removing a row ID leaves a key with an empty value list, that key is removed entirely.
    /// </summary>
    /// <example>
    /// // Given: "Alice" → [1, 5], "Bob" → [1, 3]
    /// index.DeleteValues([1, 3]);
    /// // Result: "Alice" → [5], "Bob" is removed
    /// </example>
    void DeleteValues(List<long> rowIds);

    /// <summary>
    /// Returns all row IDs associated with the given key.
    /// Returns an empty list if the key does not exist.
    /// </summary>
    /// <example>
    /// // Given: "Alice" → [1, 5]
    /// index.Search("Alice");   // returns [1, 5]
    /// index.Search("Unknown"); // returns []
    /// </example>
    List<long> Search(string key);

    /// <summary>
    /// Checks whether any key in the index maps to the given row ID.
    /// Used to verify if a row is referenced by this index (e.g., for constraint checks).
    /// </summary>
    /// <example>
    /// // Given: "Alice" → [1, 5]
    /// index.ContainsValue(1);  // true
    /// index.ContainsValue(99); // false
    /// </example>
    bool ContainsValue(long rowId);

    /// <summary>
    /// Persists the index to disk at the specified file path.
    /// The serialization format is implementation-specific.
    /// </summary>
    void Save(string filePath);
}
