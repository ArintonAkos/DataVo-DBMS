using DataVo.Core.BTree.Core;

namespace DataVo.Core.BTree.Binary;

/// <summary>
/// Implements <see cref="IIndex"/> using a classic disk-backed binary B-Tree.
/// </summary>
/// <remarks>
/// In this variant, keys and row IDs may appear in both internal and leaf pages.
/// The index is persisted through <see cref="DiskPager"/>, which stores each page in a fixed-size 4 KB block.
/// </remarks>
public class BinaryBTreeIndex : IIndex, IDisposable
{
    private DiskPager _pager = null!;

    /// <summary>
    /// Initializes a new instance of the <see cref="BinaryBTreeIndex"/> class.
    /// </summary>
    public BinaryBTreeIndex() { }

    /// <summary>
    /// Opens an index file and initializes its root page if the file is new.
    /// </summary>
    /// <param name="filePath">The path to the backing <c>.btree</c> file.</param>
    public void Load(string filePath)
    {
        _pager = new DiskPager(filePath);
        if (_pager.RootPageId == -1)
        {
            var root = _pager.AllocatePage();
            root.IsLeaf = true;
            root.NumKeys = 0;
            _pager.RootPageId = root.PageId;
            _pager.WritePage(root);
            _pager.WriteMetadata();
        }
    }

    /// <summary>
    /// Creates and loads a <see cref="BinaryBTreeIndex"/> from the specified file.
    /// </summary>
    /// <param name="filePath">The path to the backing file.</param>
    /// <returns>A loaded <see cref="BinaryBTreeIndex"/> instance.</returns>
    public static BinaryBTreeIndex LoadFile(string filePath)
    {
        var index = new BinaryBTreeIndex();
        index.Load(filePath);
        return index;
    }

    /// <summary>
    /// Persists index metadata to disk.
    /// </summary>
    /// <param name="filePath">The file path to initialize if the index has not yet been loaded.</param>
    public void Save(string filePath)
    {
        if (_pager == null)
        {
            Load(filePath);
        }
        else
        {
            _pager.WriteMetadata();
        }
    }

    /// <summary>
    /// Inserts a key-to-row mapping into the B-Tree.
    /// </summary>
    /// <param name="key">The logical key to index.</param>
    /// <param name="rowId">The row ID associated with the key.</param>
    /// <exception cref="Exception">Thrown when the index has not been loaded.</exception>
    public void Insert(string key, long rowId)
    {
        if (_pager == null) throw new Exception("Index not loaded");

        BTreePage root = _pager.ReadPage(_pager.RootPageId);

        if (root.NumKeys == BTreePage.MaxKeys)
        {
            BTreePage newRoot = _pager.AllocatePage();
            newRoot.IsLeaf = false;
            newRoot.NumKeys = 0;
            newRoot.Children[0] = root.PageId;

            _pager.RootPageId = newRoot.PageId;
            _pager.WriteMetadata();

            newRoot.SplitChild(0, root, _pager);
            newRoot.InsertNonFull(key, rowId, _pager);
        }
        else
        {
            root.InsertNonFull(key, rowId, _pager);
        }
    }

    /// <summary>
    /// Returns all row IDs associated with the specified key.
    /// </summary>
    /// <param name="key">The logical key to search for.</param>
    /// <returns>A list of matching row IDs, excluding tombstoned zero values.</returns>
    /// <exception cref="Exception">Thrown when the index has not been loaded.</exception>
    public List<long> Search(string key)
    {
        if (_pager == null) throw new Exception("Index not loaded");

        var results = new List<long>();
        SearchInternal(_pager.RootPageId, key, results);
        return results;
    }

    /// <summary>
    /// Recursively searches the subtree rooted at the specified page and appends matching row IDs to the result list.
    /// </summary>
    /// <param name="pageId">The page ID to search.</param>
    /// <param name="key">The logical key to locate.</param>
    /// <param name="results">The destination list for matching row IDs.</param>
    private void SearchInternal(int pageId, string key, List<long> results)
    {
        BTreePage node = _pager.ReadPage(pageId);
        int i = 0;

        while (i < node.NumKeys && string.Compare(key, node.Keys[i], StringComparison.Ordinal) > 0)
        {
            i++;
        }

        int matchIdx = i;
        while (matchIdx < node.NumKeys && string.Compare(key, node.Keys[matchIdx], StringComparison.Ordinal) == 0)
        {
            if (node.Values[matchIdx] != 0)
            {
                results.Add(node.Values[matchIdx]);
            }

            if (!node.IsLeaf)
            {
                SearchInternal(node.Children[matchIdx], key, results);
            }
            matchIdx++;
        }

        if (!node.IsLeaf)
        {
            SearchInternal(node.Children[matchIdx], key, results);
        }
    }

    /// <summary>
    /// Tombstones every occurrence of the specified row IDs in the index.
    /// </summary>
    /// <param name="rowIds">The row IDs to remove.</param>
    /// <exception cref="Exception">Thrown when the index has not been loaded.</exception>
    /// <remarks>
    /// This implementation performs logical deletion by replacing matching row IDs with the sentinel value <c>0</c>.
    /// It does not rebalance or compact the tree.
    /// </remarks>
    public void DeleteValues(List<long> rowIds)
    {
        if (_pager == null) throw new Exception("Index not loaded");
        var idsSet = new HashSet<long>(rowIds);

        // Tombstone deletion algorithm for performance and simplicity in advanced disk I/O
        for (int i = 1; i < _pager.NumPages; i++)
        {
            var page = _pager.ReadPage(i);
            bool pageChanged = false;

            for (int k = 0; k < page.NumKeys; k++)
            {
                if (idsSet.Contains(page.Values[k]))
                {
                    page.Values[k] = 0; // Tombstone
                    pageChanged = true;
                }
            }
            if (pageChanged)
            {
                _pager.WritePage(page);
            }
        }
    }

    /// <summary>
    /// Determines whether the specified row ID appears anywhere in the index.
    /// </summary>
    /// <param name="rowId">The row ID to search for.</param>
    /// <returns><see langword="true"/> if the row ID is present; otherwise, <see langword="false"/>.</returns>
    /// <exception cref="Exception">Thrown when the index has not been loaded.</exception>
    public bool ContainsValue(long rowId)
    {
        if (_pager == null) throw new Exception("Index not loaded");

        for (int i = 1; i < _pager.NumPages; i++)
        {
            var page = _pager.ReadPage(i);
            for (int k = 0; k < page.NumKeys; k++)
            {
                if (page.Values[k] == rowId) return true;
            }
        }
        return false;
    }

    /// <summary>
    /// Releases the underlying pager and any associated file handles.
    /// </summary>
    public void Dispose()
    {
        _pager?.Dispose();
    }
}
