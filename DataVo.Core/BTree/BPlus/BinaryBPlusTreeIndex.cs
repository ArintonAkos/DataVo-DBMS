using DataVo.Core.BTree.Core;
using System.Collections.Concurrent;
using System.Threading;

namespace DataVo.Core.BTree.BPlus;

/// <summary>
/// Implements <see cref="IIndex"/> using a disk-backed binary B+Tree.
/// </summary>
/// <remarks>
/// All row IDs are stored in leaf pages, and leaf pages are linked through <see cref="BPlusTreePage.NextPageId"/>.
/// This layout supports efficient exact-match lookups and sequential/range-style leaf scanning.
/// </remarks>
public class BinaryBPlusTreeIndex : IIndex, IDisposable
{
    private BPlusDiskPager _pager = null!;
    private readonly object _writerGate = new();
    private readonly ConcurrentDictionary<int, ReaderWriterLockSlim> _pageLatches = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="BinaryBPlusTreeIndex"/> class.
    /// </summary>
    public BinaryBPlusTreeIndex() { }

    /// <summary>
    /// Inserts a logical key-to-row mapping into the B+Tree.
    /// </summary>
    /// <param name="key">The logical key to insert.</param>
    /// <param name="rowId">The row ID associated with the key.</param>
    public void Insert(string key, long rowId)
    {
        byte[] encodedKey = IndexKeyEncoder.Encode(key);

        lock (_writerGate)
        {
            if (_pager.RootPageId == -1)
            {
                var root = _pager.AllocatePage();
                EnsurePageLatch(root.PageId);

                using (EnterWriteLatch(root.PageId))
                {
                    root.IsLeaf = true;
                    root.NextPageId = -1;
                    root.Keys[0] = encodedKey;
                    root.SetValue(0, rowId);
                    root.NumKeys = 1;

                    _pager.RootPageId = root.PageId;
                    _pager.WritePage(root);
                    _pager.WriteMetadata();
                }

                return;
            }

            int rootPageId = _pager.RootPageId;
            EnsurePageLatch(rootPageId);

            IDisposable? rootLatch = EnterWriteLatch(rootPageId);
            BPlusTreePage rootPage = _pager.ReadPage(rootPageId);

            if (rootPage.NumKeys == BPlusTreePage.MaxKeys)
            {
                var newRoot = _pager.AllocatePage();
                EnsurePageLatch(newRoot.PageId);

                using (EnterWriteLatch(newRoot.PageId))
                {
                    newRoot.IsLeaf = false;
                    newRoot.Children[0] = rootPage.PageId;

                    SplitChild(newRoot, 0, rootPage);

                    _pager.RootPageId = newRoot.PageId;
                    _pager.WritePage(newRoot);
                    _pager.WriteMetadata();
                }

                rootLatch.Dispose();
                rootLatch = EnterWriteLatch(newRoot.PageId);
                rootPage = _pager.ReadPage(newRoot.PageId);
            }

            InsertNonFullWithLatchCrabbing(rootPage, rootLatch, encodedKey, rowId);
        }
    }

    /// <summary>
    /// Inserts a key/value pair into a page that is known not to be full.
    /// </summary>
    /// <param name="node">The page that will receive the insertion.</param>
    /// <param name="nodeWriteLatch">Write latch currently held for <paramref name="node"/>.</param>
    /// <param name="key">The encoded key to insert.</param>
    /// <param name="value">The row ID associated with the key.</param>
    private void InsertNonFullWithLatchCrabbing(BPlusTreePage node, IDisposable nodeWriteLatch, byte[] key, long value)
    {
        BPlusTreePage current = node;
        IDisposable currentLatch = nodeWriteLatch;

        try
        {
            while (true)
            {
                if (current.IsLeaf)
                {
                    int insertIdx = current.FindIndex(key);
                    // Shift right to make room
                    for (int j = current.NumKeys - 1; j >= insertIdx; j--)
                    {
                        current.Keys[j + 1] = current.Keys[j];
                        current.SetValue(j + 1, current.GetValue(j));
                    }

                    current.Keys[insertIdx] = key;
                    current.SetValue(insertIdx, value);
                    current.NumKeys++;
                    _pager.WritePage(current);
                    return;
                }

                int i = current.FindIndex(key);
                if (i < current.NumKeys && IndexKeyEncoder.CompareKeys(current.Keys[i], key) == 0)
                {
                    i++; // For B+Tree, internal node keys are <= right child's min
                }

                int childPageId = current.Children[i];
                EnsurePageLatch(childPageId);
                IDisposable childLatch = EnterWriteLatch(childPageId);
                BPlusTreePage child = _pager.ReadPage(childPageId);

                if (child.NumKeys == BPlusTreePage.MaxKeys)
                {
                    SplitChild(current, i, child);

                    childLatch.Dispose();

                    if (IndexKeyEncoder.CompareKeys(key, current.Keys[i]) >= 0)
                    {
                        i++;
                    }

                    childPageId = current.Children[i];
                    EnsurePageLatch(childPageId);
                    childLatch = EnterWriteLatch(childPageId);
                    child = _pager.ReadPage(childPageId);
                }

                currentLatch.Dispose();
                currentLatch = childLatch;
                current = child;
            }
        }
        finally
        {
            currentLatch.Dispose();
        }
    }

    /// <summary>
    /// Splits a full child page and updates the parent routing page.
    /// </summary>
    /// <param name="parent">The parent page that will receive the promoted routing key.</param>
    /// <param name="i">The child slot to split.</param>
    /// <param name="child">The full child page.</param>
    private void SplitChild(BPlusTreePage parent, int i, BPlusTreePage child)
    {
        var newNode = _pager.AllocatePage();
        EnsurePageLatch(newNode.PageId);
        newNode.IsLeaf = child.IsLeaf;

        int t = BPlusTreePage.T;

        if (child.IsLeaf)
        {
            // Leaf Split: newNode gets the upper half, including the median key
            newNode.NumKeys = BPlusTreePage.MaxKeys - t;
            for (int j = 0; j < newNode.NumKeys; j++)
            {
                newNode.Keys[j] = child.Keys[j + t];
                newNode.SetValue(j, child.GetValue(j + t));
            }

            newNode.NextPageId = child.NextPageId;
            child.NextPageId = newNode.PageId; // Linked list for sequential scans
            child.NumKeys = t;

            // Push up the lowest key of newNode as the routing key
            byte[] routingKey = newNode.Keys[0];

            for (int j = parent.NumKeys - 1; j >= i; j--)
            {
                parent.Keys[j + 1] = parent.Keys[j];
                parent.Children[j + 2] = parent.Children[j + 1];
            }
            parent.Keys[i] = routingKey;
            parent.Children[i + 1] = newNode.PageId;
            parent.NumKeys++;
        }
        else
        {
            // Internal Split: Median key pushed UP, not to new right node
            newNode.NumKeys = BPlusTreePage.MaxKeys - t - 1;
            for (int j = 0; j < newNode.NumKeys; j++)
            {
                newNode.Keys[j] = child.Keys[j + t + 1];
            }
            for (int j = 0; j <= newNode.NumKeys; j++)
            {
                newNode.Children[j] = child.Children[j + t + 1];
            }

            byte[] medianKey = child.Keys[t];
            child.NumKeys = t;

            for (int j = parent.NumKeys - 1; j >= i; j--)
            {
                parent.Keys[j + 1] = parent.Keys[j];
                parent.Children[j + 2] = parent.Children[j + 1];
            }
            parent.Keys[i] = medianKey;
            parent.Children[i + 1] = newNode.PageId;
            parent.NumKeys++;
        }

        _pager.WritePage(child);
        _pager.WritePage(newNode);
        _pager.WritePage(parent);
    }

    /// <summary>
    /// Returns all row IDs associated with the specified key.
    /// </summary>
    /// <param name="key">The logical key to search for.</param>
    /// <returns>A list of matching row IDs, excluding tombstoned zero values.</returns>
    public List<long> Search(string key)
    {
        var results = new List<long>();
        if (_pager.RootPageId == -1) return results;

        byte[] encodedKey = IndexKeyEncoder.Encode(key);
        int rootPageId = _pager.RootPageId;
        EnsurePageLatch(rootPageId);

        IDisposable? currentLatch = EnterReadLatch(rootPageId);
        BPlusTreePage current = _pager.ReadPage(rootPageId);

        try
        {
            // Traverse to the first leaf where key could exist using read-latch coupling.
            while (!current.IsLeaf)
            {
                int i = current.FindIndex(encodedKey);
                int childPageId = current.Children[i];

                EnsurePageLatch(childPageId);
                IDisposable childLatch = EnterReadLatch(childPageId);
                BPlusTreePage child = _pager.ReadPage(childPageId);

                currentLatch.Dispose();
                currentLatch = childLatch;
                current = child;
            }

            // Scan linearly across linked leaves using latch crabbing across sibling leaves.
            bool stop = false;
            while (!stop)
            {
                for (int i = 0; i < current.NumKeys; i++)
                {
                    int cmp = IndexKeyEncoder.CompareKeys(current.Keys[i], encodedKey);
                    if (cmp == 0)
                    {
                        long val = current.GetValue(i);
                        if (val != 0) // 0 = empty/tombstone sentinel
                        {
                            results.Add(val);
                        }
                    }
                    else if (cmp > 0)
                    {
                        // Sorted: any key > target means we're done
                        stop = true;
                        break;
                    }
                }

                if (stop || current.NextPageId == -1)
                {
                    break;
                }

                int nextLeafPageId = current.NextPageId;
                EnsurePageLatch(nextLeafPageId);
                IDisposable nextLeafLatch = EnterReadLatch(nextLeafPageId);
                BPlusTreePage nextLeaf = _pager.ReadPage(nextLeafPageId);

                currentLatch.Dispose();
                currentLatch = nextLeafLatch;
                current = nextLeaf;
            }
        }
        finally
        {
            currentLatch.Dispose();
        }

        return results;
    }

    /// <summary>
    /// Determines whether the specified row ID appears in any leaf page.
    /// </summary>
    /// <param name="key">The row ID to search for.</param>
    /// <returns><see langword="true"/> if the row ID is present; otherwise, <see langword="false"/>.</returns>
    public bool ContainsValue(long key)
    {
        for (int i = 1; i < _pager.NumPages; i++)
        {
            EnsurePageLatch(i);
            using var pageLatch = EnterReadLatch(i);
            var page = _pager.ReadPage(i);
            if (!page.IsLeaf) continue;

            for (int k = 0; k < page.NumKeys; k++)
            {
                if (page.GetValue(k) == key) return true;
            }
        }
        return false;
    }

    /// <summary>
    /// Placeholder for key-specific deletion. This operation is currently not implemented.
    /// </summary>
    /// <param name="key">The logical key to delete from.</param>
    /// <param name="value">The specific row ID to remove.</param>
    public void Delete(string key, long value) { }

    /// <summary>
    /// Tombstones every occurrence of the specified row IDs in leaf pages.
    /// </summary>
    /// <param name="valuesToDelete">The row IDs to remove.</param>
    /// <remarks>
    /// This implementation performs logical deletion by writing the sentinel value <c>0</c>
    /// instead of rebalancing or compacting the tree.
    /// </remarks>
    public void DeleteValues(List<long> valuesToDelete)
    {
        if (_pager == null) return;
        var idsSet = new HashSet<long>(valuesToDelete);

        lock (_writerGate)
        {
            for (int i = 1; i < _pager.NumPages; i++)
            {
                EnsurePageLatch(i);
                using var pageLatch = EnterWriteLatch(i);

                var page = _pager.ReadPage(i);
                if (!page.IsLeaf) continue;

                bool pageChanged = false;
                for (int k = 0; k < page.NumKeys; k++)
                {
                    if (idsSet.Contains(page.GetValue(k)))
                    {
                        page.SetValue(k, 0); // Tombstone with sentinel 0
                        pageChanged = true;
                    }
                }
                if (pageChanged)
                {
                    _pager.WritePage(page);
                }
            }
        }
    }

    /// <summary>
    /// Persists pager metadata to disk.
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
    /// Opens the B+Tree file through a <see cref="BPlusDiskPager"/>.
    /// </summary>
    /// <param name="filePath">The path to the backing file.</param>
    public void Load(string filePath)
    {
        _pager = new BPlusDiskPager(filePath);
        _pageLatches.Clear();
    }

    /// <summary>
    /// Creates and loads a <see cref="BinaryBPlusTreeIndex"/> from the specified file.
    /// </summary>
    /// <param name="filePath">The path to the backing file.</param>
    /// <returns>A loaded <see cref="BinaryBPlusTreeIndex"/> instance.</returns>
    public static BinaryBPlusTreeIndex LoadFile(string filePath)
    {
        var index = new BinaryBPlusTreeIndex();
        index._pager = new BPlusDiskPager(filePath);
        return index;
    }

    /// <summary>
    /// Releases the underlying pager and any associated file handles.
    /// </summary>
    public void Dispose()
    {
        _pager?.Dispose();
        _pager = null!;

        foreach (ReaderWriterLockSlim latch in _pageLatches.Values)
        {
            latch.Dispose();
        }

        _pageLatches.Clear();
    }

    private void EnsurePageLatch(int pageId)
    {
        _pageLatches.GetOrAdd(pageId, _ => new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion));
    }

    private IDisposable EnterReadLatch(int pageId)
    {
        ReaderWriterLockSlim pageLatch = _pageLatches.GetOrAdd(pageId, _ => new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion));
        pageLatch.EnterReadLock();
        return new PageLatchScope(pageLatch, isWrite: false);
    }

    private IDisposable EnterWriteLatch(int pageId)
    {
        ReaderWriterLockSlim pageLatch = _pageLatches.GetOrAdd(pageId, _ => new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion));
        pageLatch.EnterWriteLock();
        return new PageLatchScope(pageLatch, isWrite: true);
    }

    private sealed class PageLatchScope : IDisposable
    {
        private readonly ReaderWriterLockSlim _latch;
        private readonly bool _isWrite;
        private bool _disposed;

        public PageLatchScope(ReaderWriterLockSlim latch, bool isWrite)
        {
            _latch = latch;
            _isWrite = isWrite;
        }

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            if (_isWrite)
            {
                _latch.ExitWriteLock();
            }
            else
            {
                _latch.ExitReadLock();
            }

            _disposed = true;
        }
    }
}
