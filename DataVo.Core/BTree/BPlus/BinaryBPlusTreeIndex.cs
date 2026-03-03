using DataVo.Core.BTree.Core;

namespace DataVo.Core.BTree.BPlus;

public class BinaryBPlusTreeIndex : IIndex
{
    private BPlusDiskPager _pager = null!;

    public BinaryBPlusTreeIndex() { }

    public void Insert(string key, long rowId)
    {
        byte[] encodedKey = IndexKeyEncoder.Encode(key);

        if (_pager.RootPageId == -1)
        {
            var root = _pager.AllocatePage();
            root.IsLeaf = true;
            root.NextPageId = -1;
            root.Keys[0] = encodedKey;
            root.SetValue(0, rowId);
            root.NumKeys = 1;

            _pager.RootPageId = root.PageId;
            _pager.WritePage(root);
            _pager.WriteMetadata();
            return;
        }

        var rootPage = _pager.ReadPage(_pager.RootPageId);
        if (rootPage.NumKeys == BPlusTreePage.MaxKeys)
        {
            var newRoot = _pager.AllocatePage();
            newRoot.IsLeaf = false;
            newRoot.Children[0] = rootPage.PageId;

            SplitChild(newRoot, 0, rootPage);

            _pager.RootPageId = newRoot.PageId;
            _pager.WritePage(newRoot);
            _pager.WriteMetadata();

            InsertNonFull(newRoot, encodedKey, rowId);
        }
        else
        {
            InsertNonFull(rootPage, encodedKey, rowId);
        }
    }

    private void InsertNonFull(BPlusTreePage node, byte[] key, long value)
    {
        if (node.IsLeaf)
        {
            int insertIdx = node.FindIndex(key);
            // Shift right to make room
            for (int j = node.NumKeys - 1; j >= insertIdx; j--)
            {
                node.Keys[j + 1] = node.Keys[j];
                node.SetValue(j + 1, node.GetValue(j));
            }
            node.Keys[insertIdx] = key;
            node.SetValue(insertIdx, value);
            node.NumKeys++;
            _pager.WritePage(node);
        }
        else
        {
            int i = node.FindIndex(key);
            if (i < node.NumKeys && IndexKeyEncoder.CompareKeys(node.Keys[i], key) == 0)
            {
                i++; // For B+Tree, internal node keys are <= right child's min
            }

            var child = _pager.ReadPage(node.Children[i]);

            if (child.NumKeys == BPlusTreePage.MaxKeys)
            {
                SplitChild(node, i, child);
                if (IndexKeyEncoder.CompareKeys(key, node.Keys[i]) >= 0)
                {
                    i++;
                }
                child = _pager.ReadPage(node.Children[i]);
            }
            InsertNonFull(child, key, value);
        }
    }

    private void SplitChild(BPlusTreePage parent, int i, BPlusTreePage child)
    {
        var newNode = _pager.AllocatePage();
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

    public List<long> Search(string key)
    {
        var results = new List<long>();
        if (_pager.RootPageId == -1) return results;

        byte[] encodedKey = IndexKeyEncoder.Encode(key);
        var current = _pager.ReadPage(_pager.RootPageId);

        // Traverse to the FIRST LEAF where key COULD exist
        while (!current.IsLeaf)
        {
            int i = current.FindIndex(encodedKey);
            current = _pager.ReadPage(current.Children[i]);
        }

        // Scan linearly across linked leaves
        bool stop = false;
        while (current != null && !stop)
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

            if (!stop && current.NextPageId != -1)
            {
                current = _pager.ReadPage(current.NextPageId);
            }
            else
            {
                current = null!;
            }
        }

        return results;
    }

    public bool ContainsValue(long key)
    {
        for (int i = 1; i < _pager.NumPages; i++)
        {
            var page = _pager.ReadPage(i);
            if (!page.IsLeaf) continue;

            for (int k = 0; k < page.NumKeys; k++)
            {
                if (page.GetValue(k) == key) return true;
            }
        }
        return false;
    }

    public void Delete(string key, long value) { }

    public void DeleteValues(List<long> valuesToDelete)
    {
        if (_pager == null) return;
        var idsSet = new HashSet<long>(valuesToDelete);

        for (int i = 1; i < _pager.NumPages; i++)
        {
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

    public void Load(string filePath)
    {
        _pager = new BPlusDiskPager(filePath);
    }

    public static BinaryBPlusTreeIndex LoadFile(string filePath)
    {
        var index = new BinaryBPlusTreeIndex();
        index._pager = new BPlusDiskPager(filePath);
        return index;
    }

    public void Dispose()
    {
        _pager?.Dispose();
        _pager = null!;
    }
}
