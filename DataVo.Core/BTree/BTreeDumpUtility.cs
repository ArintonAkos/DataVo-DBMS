using System.Text;
using DataVo.Core.BTree.BPlus;
using DataVo.Core.BTree.Core;

namespace DataVo.Core.BTree;

/// <summary>
/// Provides debugger-oriented helpers for inspecting the on-disk contents of B-Tree and B+Tree indexes.
/// </summary>
/// <remarks>
/// <para>
/// This type is intended for diagnostics and troubleshooting rather than normal query execution.
/// The output is formatted as plain text so it can be copied directly from a debugger watch window,
/// Immediate Window, or log output.
/// </para>
/// <para>
/// Typical usage:
/// <code>
/// BTreeDumpUtility.DumpIndex("_PK_Users", "Users", "MyDB");
/// BTreeDumpUtility.DumpBPlusTreeFile("databases/MyDB/Users__PK_Users_index.btree");
/// </code>
/// </para>
/// </remarks>
public static class BTreeDumpUtility
{
    /// <summary>
    /// Loads an index through <see cref="IndexManager"/> and returns a formatted textual dump of its backing B+Tree file.
    /// </summary>
    /// <param name="indexName">The logical index name, such as <c>_PK_Users</c> or a user-defined index name.</param>
    /// <param name="tableName">The table that owns the index.</param>
    /// <param name="databaseName">The database containing the table and index file.</param>
    /// <returns>
    /// A human-readable dump containing the header information and, when the file is available,
    /// the page-by-page contents of the associated B+Tree file.
    /// </returns>
    /// <remarks>
    /// This method intentionally traps index-loading failures and returns the error message inside
    /// the formatted output instead of throwing, which makes it more convenient to use while debugging.
    /// </remarks>
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
    /// Reads a raw <c>.btree</c> file and produces a page-by-page textual representation of its contents.
    /// </summary>
    /// <param name="filePath">The path to the B+Tree file to inspect.</param>
    /// <returns>
    /// A formatted string that includes file metadata, each page's type, keys, row IDs,
    /// child pointers, and the leaf-page linked-list information.
    /// </returns>
    /// <remarks>
    /// Missing files are reported in the returned text instead of causing an exception.
    /// </remarks>
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
                string keyDisplay = FormatKey(page.Keys[i]);

                if (page.IsLeaf)
                {
                    long val = page.GetValue(i);
                    string flag = val == 0 ? " ⚠️ ZERO (sentinel/empty)" : "";
                    sb.AppendLine($"    [{i}] Key={keyDisplay}  → RowId={val}{flag}");
                }
                else
                {
                    sb.AppendLine($"    [{i}] Key={keyDisplay}  Child[{i}]={page.Children[i]}");
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

    /// <summary>
    /// Formats an encoded key for display by attempting to decode simple integer keys first,
    /// then falling back to a hexadecimal representation.
    /// </summary>
    /// <param name="key">The encoded key bytes read from a B+Tree page.</param>
    /// <returns>
    /// A readable representation such as <c>INT(42)</c>, <c>[empty]</c>, or a hex byte sequence.
    /// </returns>
    private static string FormatKey(byte[] key)
    {
        if (IndexKeyEncoder.IsEmptyKey(key))
            return "[empty]";

        // Try to decode as a sign-flipped int (first 4 bytes)
        if (key.Length >= 4)
        {
            uint raw = (uint)(key[0] << 24 | key[1] << 16 | key[2] << 8 | key[3]);
            int intVal = (int)(raw ^ unchecked((uint)int.MinValue)); // reverse sign-flip

            // Check if remaining bytes are zero (pure int key)
            bool isSimpleInt = true;
            for (int i = 4; i < key.Length; i++)
            {
                if (key[i] != 0) { isSimpleInt = false; break; }
            }

            if (isSimpleInt)
                return $"INT({intVal})";
        }

        // Show hex of non-zero bytes
        int lastNonZero = key.Length - 1;
        while (lastNonZero >= 0 && key[lastNonZero] == 0) lastNonZero--;

        string hex = BitConverter.ToString(key, 0, lastNonZero + 1).Replace("-", " ");
        return $"[{hex}]";
    }
}
