using System.Runtime.InteropServices.JavaScript;
using DataVo.Core.StorageEngine;

namespace DataVo.Browser;

/// <summary>
/// A WebAssembly specific storage engine that heavily relies on JSInterop to 
/// read and write byte arrays to an underlying persistent Browser API (like IndexedDB).
/// </summary>
public partial class BrowserStorageEngine : IStorageEngine
{
    // Note: IndexedDB is fundamentally asynchronous, but DataVo's current IStorageEngine
    // is fully synchronous. In WASM, synchronous JSInterop holds the WASM execution thread,
    // which is acceptable for client-side synchronous DB execution, provided the JS implementations 
    // utilize synchronous storage (like localStorage) OR the WASM payload is booted with Web Workers.
    // However, JSInterop doesn't support async calls blocking WASM thread natively without Asyncify/Promises.
    // For this generalized implementation, we will map standard synchronous calls.
    // (If using IndexedDB, you'll need a synchronous proxy in JS or run WASM in a Worker that bridges to IDB synchronously via Atomics, 
    // or just use localStorage for smaller datasets if IDB sync is unavailable).

    [JSImport("globalThis.DataVoStorage.insertRow")]
    internal static partial string InsertRowJS(string databaseName, string tableName, byte[] rowBytes);

    [JSImport("globalThis.DataVoStorage.readRow")]
    internal static partial byte[]? ReadRowJS(string databaseName, string tableName, string rowId);

    [JSImport("globalThis.DataVoStorage.readAllRows")]
    internal static partial string ReadAllRowsJS(string databaseName, string tableName); // Returns JSON holding arrays of [rowId, bytesBase64]

    [JSImport("globalThis.DataVoStorage.deleteRow")]
    internal static partial void DeleteRowJS(string databaseName, string tableName, string rowId);

    [JSImport("globalThis.DataVoStorage.dropTable")]
    internal static partial void DropTableJS(string databaseName, string tableName);

    [JSImport("globalThis.DataVoStorage.dropDatabase")]
    internal static partial void DropDatabaseJS(string databaseName);

    public long InsertRow(string databaseName, string tableName, byte[] rowBytes)
    {
        string idStr = InsertRowJS(databaseName, tableName, rowBytes);
        return long.Parse(idStr);
    }

    public List<long> InsertRows(string databaseName, string tableName, List<byte[]> rowsBytes)
    {
        var ids = new List<long>(rowsBytes.Count);
        foreach (var row in rowsBytes)
        {
            ids.Add(InsertRow(databaseName, tableName, row));
        }
        return ids;
    }

    public byte[] ReadRow(string databaseName, string tableName, long rowId)
    {
        var bytes = ReadRowJS(databaseName, tableName, rowId.ToString());
        if (bytes == null) throw new Exception($"Row {rowId} not found in {databaseName}.{tableName}");
        return bytes;
    }

    public IEnumerable<(long RowId, byte[] RawRow)> ReadAllRows(string databaseName, string tableName)
    {
        string json = ReadAllRowsJS(databaseName, tableName);
        if (string.IsNullOrEmpty(json) || json == "[]") yield break;

        // Manual parsing of the simple JSON array format: [["rowId","base64"],["rowId","base64"]]
        // This avoids System.Text.Json reflection which is disabled in trimmed WASM builds.
        foreach (var entry in ParseRowEntries(json))
        {
            long rowId = long.Parse(entry.Id);
            byte[] rawRow = Convert.FromBase64String(entry.Data);
            yield return (rowId, rawRow);
        }
    }

    /// <summary>
    /// Parses a JSON array of [id, base64] tuples without using reflection-based JSON deserialization.
    /// </summary>
    private static IEnumerable<(string Id, string Data)> ParseRowEntries(string json)
    {
        // Strip outer brackets
        var trimmed = json.Trim();
        if (trimmed.Length < 2) yield break;
        trimmed = trimmed[1..^1]; // remove outer [ ]

        int depth = 0;
        int start = -1;

        for (int i = 0; i < trimmed.Length; i++)
        {
            char c = trimmed[i];
            if (c == '[')
            {
                if (depth == 0) start = i + 1;
                depth++;
            }
            else if (c == ']')
            {
                depth--;
                if (depth == 0 && start >= 0)
                {
                    var inner = trimmed[start..i];
                    var parts = inner.Split(',', 2);
                    if (parts.Length == 2)
                    {
                        var id = parts[0].Trim().Trim('"');
                        var data = parts[1].Trim().Trim('"');
                        yield return (id, data);
                    }
                    start = -1;
                }
            }
        }
    }

    public void DeleteRow(string databaseName, string tableName, long rowId)
    {
        DeleteRowJS(databaseName, tableName, rowId.ToString());
    }

    public void DropTable(string databaseName, string tableName)
    {
        DropTableJS(databaseName, tableName);
    }

    public void DropDatabase(string databaseName)
    {
        DropDatabaseJS(databaseName);
    }

    public List<(long NewRowId, byte[] RawRow)> CompactTable(string databaseName, string tableName)
    {
        // For BrowserStorage, compaction is largely delegated to the JS engine or handled here
        // by reading all, dropping, and rewriting.
        var allRows = ReadAllRows(databaseName, tableName).ToList();
        DropTable(databaseName, tableName);

        var rewritten = new List<(long NewRowId, byte[] RawRow)>();
        foreach (var row in allRows)
        {
            long newId = InsertRow(databaseName, tableName, row.RawRow);
            rewritten.Add((newId, row.RawRow));
        }
        return rewritten;
    }
}
