using System.Text;
using DataVo.Core.BTree.BPlus;
using DataVo.Core.BTree.Core;

namespace DataVo.Core.BTree;

/// <summary>
/// Debug utility for inspecting B-Tree / B+Tree index contents.
/// Call from the VS Code debug watch window or Immediate Window:
///   BTreeDumpUtility.DumpIndex("_PK_Users", "Users", "MyDB")
///   BTreeDumpUtility.DumpBPlusTreeFile("databases/MyDB/Users__PK_Users_index.btree")
/// </summary>
public static class BTreeDumpUtility
{
    /// <summary>
    /// Dumps a B+Tree index via the IndexManager singleton (must be loaded/cached).
    /// Returns a formatted string you can inspect in the debugger.
    /// </summary>
    public static string DumpIndex(string indexName, string tableName, string databaseName)
    {
        var sb = new StringBuilder();
        
        try
        {
            bool hasKey = IndexManager.Instance.IndexContainsKey("__probe__", indexName, tableName, databaseName);
            sb.AppendLine($"=== Index: {indexName} on {tableName} (db: {databaseName}) ===");
            sb.AppendLine($"  Index loaded: YES (probe returned {hasKey})");
        }
        catch (Exception ex)
        {
            sb.AppendLine($"=== Index: {indexName} on {tableName} (db: {databaseName}) ===");
            sb.AppendLine($"  ERROR loading index: {ex.Message}");
            return sb.ToString();
        }

        // Try to dump as B+Tree file
        string filePath = Path.Combine("databases", databaseName, $"{tableName}_{indexName}_index.btree");
        if (File.Exists(filePath))
        {
            sb.Append(DumpBPlusTreeFile(filePath));
        }
        else
        {
            sb.AppendLine($"  File not found: {filePath}");
        }

        return sb.ToString();
    }

    /// <summary>
    /// Dumps the raw contents of a B+Tree .btree file.
    /// Shows page layout, keys, values (row IDs), and linked-list structure.
    /// </summary>
    public static string DumpBPlusTreeFile(string filePath)
    {
        var sb = new StringBuilder();

        if (!File.Exists(filePath))
        {
            sb.AppendLine($"File not found: {filePath}");
            return sb.ToString();
        }

        using var pager = new BPlusDiskPager(filePath);
        sb.AppendLine($"  File: {filePath} ({new FileInfo(filePath).Length:N0} bytes)");
        sb.AppendLine($"  RootPageId: {pager.RootPageId}");
        sb.AppendLine($"  NumPages: {pager.NumPages}");
        sb.AppendLine();

        for (int pageId = 1; pageId < pager.NumPages; pageId++)
        {
            var page = pager.ReadPage(pageId);
            string nodeType = page.IsLeaf ? "LEAF" : "INTERNAL";
            sb.AppendLine($"  Page {pageId} [{nodeType}] NumKeys={page.NumKeys} NextPageId={page.NextPageId}");

            for (int i = 0; i < page.NumKeys; i++)
            {
                if (page.IsLeaf)
                {
                    long val = page.GetValue(i);
                    string flag = val == 0 ? " ZERO (sentinel/empty)" : "";
                    sb.AppendLine($"    [{i}] Key={page.Keys[i]}  → RowId={val}{flag}");
                }
                else
                {
                    sb.AppendLine($"    [{i}] Key={page.Keys[i]}  Child[{i}]={page.Children[i]}");
                }
            }

            if (!page.IsLeaf)
            {
                sb.AppendLine($"    Child[{page.NumKeys}]={page.Children[page.NumKeys]}");
            }

            sb.AppendLine();
        }

        return sb.ToString();
    }
}
