using System;
using System.Collections.Generic;
using System.IO;
using Server.Server.BTree.Core;

namespace Server.Server.BTree.BPlus;

public class BinaryBPlusTreeIndex : IIndex
{
    private BPlusDiskPager _pager = null!;

    public BinaryBPlusTreeIndex() { }

    public void Insert(string key, string rowId)
    {
        if (!int.TryParse(key, out int intKey)) return; 
        
        if (_pager.RootPageId == -1)
        {
            var root = _pager.AllocatePage();
            root.IsLeaf = true;
            root.NextPageId = -1;
            root.Keys[0] = intKey;
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
            
            InsertNonFull(newRoot, intKey, rowId);
        }
        else
        {
            InsertNonFull(rootPage, intKey, rowId);
        }
    }

    private void InsertNonFull(BPlusTreePage node, int key, string value)
    {
        int i = node.NumKeys - 1;

        if (node.IsLeaf)
        {
            // Find location and shift right
            while (i >= 0 && key < node.Keys[i])
            {
                node.Keys[i + 1] = node.Keys[i];
                node.SetValue(i + 1, node.GetValue(i));
                i--;
            }
            node.Keys[i + 1] = key;
            node.SetValue(i + 1, value);
            node.NumKeys++;
            _pager.WritePage(node);
        }
        else
        {
            // Find child
            while (i >= 0 && key < node.Keys[i])
            {
                i--;
            }
            i++;

            var child = _pager.ReadPage(node.Children[i]);

            if (child.NumKeys == BPlusTreePage.MaxKeys)
            {
                SplitChild(node, i, child);
                if (key >= node.Keys[i]) // For B+Tree, internal node keys are <= right child's min
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
            // Leaf Split: newNode gets the upper half, including the median key (it acts as routing key up above)
            // child will keep the lower half. 
            newNode.NumKeys = BPlusTreePage.MaxKeys - t; // 112 - 56 = 56
            for (int j = 0; j < newNode.NumKeys; j++)
            {
                newNode.Keys[j] = child.Keys[j + t];
                newNode.SetValue(j, child.GetValue(j + t));
            }
            
            newNode.NextPageId = child.NextPageId;
            child.NextPageId = newNode.PageId; // Linked Array connection! Fast sequential scans!
            child.NumKeys = t;
            
            // Push up the lowest key of newNode as the routing key
            int routingKey = newNode.Keys[0];
            
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
            // Internal Split: Similar to standard B-Tree. Median key is pushed UP and NOT to new right node.
            newNode.NumKeys = BPlusTreePage.MaxKeys - t - 1; // 112 - 56 - 1 = 55
            for (int j = 0; j < newNode.NumKeys; j++)
            {
                newNode.Keys[j] = child.Keys[j + t + 1];
            }
            for (int j = 0; j <= newNode.NumKeys; j++)
            {
                newNode.Children[j] = child.Children[j + t + 1];
            }
            
            int medianKey = child.Keys[t];
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

    public List<string> Search(string key)
    {
        var results = new List<string>();
        if (_pager.RootPageId == -1 || !int.TryParse(key, out int intKey)) return results;

        var current = _pager.ReadPage(_pager.RootPageId);
        
        // Traverse to the FIRST LEAF where key COULD exist
        while (!current.IsLeaf)
        {
            int i = 0;
            // Go to the left child if intKey == current.Keys[i] to ensure we find the very first duplicate occurrence
            while (i < current.NumKeys && intKey > current.Keys[i])
            {
                i++;
            }
            current = _pager.ReadPage(current.Children[i]);
        }

        // We are at a leaf. Now scan linearly using the highly optimized contiguous Linked Arrays.
        bool stop = false;
        while (current != null && !stop)
        {
            for (int i = 0; i < current.NumKeys; i++)
            {
                if (current.Keys[i] == intKey)
                {
                    results.Add(current.GetValue(i));
                }
                else if (current.Keys[i] > intKey)
                {
                    // Since it's sorted, any key > target means we are completely done.
                    // This avoids fetching more pages.
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

    public bool ContainsValue(string key)
    {
         return Search(key).Count > 0;
    }

    public void Delete(string key, string value) { }
    public void DeleteValues(List<string> valuesToDelete) { }

    public void Save(string filePath)
    {
        _pager?.Dispose();
        _pager = null!;
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
