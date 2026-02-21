using System;
using System.Collections.Generic;
using System.Linq;
using Server.Server.BTree.Core;

namespace Server.Server.BTree.Binary;

public class BinaryBTreeIndex : IIndex, IDisposable
{
    private DiskPager _pager = null!;

    public BinaryBTreeIndex() { }

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

    public static BinaryBTreeIndex LoadFile(string filePath)
    {
        var index = new BinaryBTreeIndex();
        index.Load(filePath);
        return index;
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

    public void Insert(string key, string rowId)
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

    public List<string> Search(string key)
    {
        if (_pager == null) throw new Exception("Index not loaded");
        
        var results = new List<string>();
        SearchInternal(_pager.RootPageId, key, results);
        return results;
    }

    private void SearchInternal(int pageId, string key, List<string> results)
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
            if (!string.IsNullOrEmpty(node.Values[matchIdx]))
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

    public void DeleteValues(List<string> rowIds)
    {
        if (_pager == null) throw new Exception("Index not loaded");
        var idsSet = new HashSet<string>(rowIds);

        // Tombstone deletion algorithm for performance and simplicity in advanced disk I/O
        for (int i = 1; i < _pager.NumPages; i++)
        {
            var page = _pager.ReadPage(i);
            bool pageChanged = false;
            
            for (int k = 0; k < page.NumKeys; k++)
            {
                if (idsSet.Contains(page.Values[k]))
                {
                    page.Values[k] = ""; // Tombstone
                    pageChanged = true;
                }
            }
            if (pageChanged)
            {
                _pager.WritePage(page);
            }
        }
    }

    public bool ContainsValue(string rowId)
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

    public void Dispose()
    {
        _pager?.Dispose();
    }
}
