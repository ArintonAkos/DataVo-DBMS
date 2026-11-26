using DataVo.Core.StorageEngine.Backends.Abstractions;

namespace DataVo.Core.StorageEngine.Backends;

/// <summary>
/// WASM backend slot wrapping a supplied OPFS-backed engine in browser runtimes.
/// </summary>
/// <remarks>
/// Without a supplied engine, <see cref="Create"/> returns an <see cref="InMemoryStorageBackend"/> reporting
/// <c>"Wasm"</c>, so the fallback keeps every in-memory capability instead of only the base row API.
/// </remarks>
internal sealed class WasmStorageBackend : IStorageBackend
{
    private readonly IStorageEngine _inner;

    private WasmStorageBackend(IStorageEngine wasmEngine)
    {
        _inner = wasmEngine;
    }

    /// <summary>Creates the backend for <see cref="Config.StorageMode.Wasm"/>.</summary>
    /// <param name="wasmEngine">The OPFS-backed engine, or null to fall back to in-memory storage.</param>
    public static IStorageEngine Create(IStorageEngine? wasmEngine) =>
        wasmEngine is null ? new InMemoryStorageBackend(backendKind: "Wasm") : new WasmStorageBackend(wasmEngine);

    public string BackendKind => "Wasm";

    public RowKey InsertRow(string databaseName, string tableName, byte[] rowBytes) => _inner.InsertRow(databaseName, tableName, rowBytes);
    public List<RowKey> InsertRows(string databaseName, string tableName, List<byte[]> rowsBytes) => _inner.InsertRows(databaseName, tableName, rowsBytes);
    public byte[] ReadRow(string databaseName, string tableName, RowKey rowId) => _inner.ReadRow(databaseName, tableName, rowId);
    public IEnumerable<(RowKey RowId, byte[] RawRow)> ReadAllRows(string databaseName, string tableName) => _inner.ReadAllRows(databaseName, tableName);
    public void DeleteRow(string databaseName, string tableName, RowKey rowId) => _inner.DeleteRow(databaseName, tableName, rowId);
    public void DropTable(string databaseName, string tableName) => _inner.DropTable(databaseName, tableName);
    public void DropDatabase(string databaseName) => _inner.DropDatabase(databaseName);
    public List<(RowKey NewRowId, byte[] RawRow)> CompactTable(string databaseName, string tableName) => _inner.CompactTable(databaseName, tableName);
}
