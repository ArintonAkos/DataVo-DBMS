namespace DataVo.Core.BTree.Core;

/// <summary>
/// Identifies the concrete on-disk index implementation to use when creating or loading an index.
/// </summary>
public enum IndexType
{
    /// <summary>
    /// JSON-serialized in-memory B-Tree implementation.
    /// </summary>
    JsonBTree,

    /// <summary>
    /// Fixed-page binary B-Tree implementation.
    /// </summary>
    BinaryBTree,

    /// <summary>
    /// Fixed-page binary B+Tree implementation.
    /// </summary>
    BinaryBPlusTree
}
