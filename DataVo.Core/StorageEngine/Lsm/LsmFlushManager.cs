namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>Freezes mutable MemTables and synchronously materializes them as SSTable byte images.</summary>
public sealed class LsmFlushManager
{
    /// <summary>
    /// Rejects empty tables without freezing them; otherwise freezes <paramref name="memTable"/> and
    /// synchronously consumes its ref-struct iterator through <see cref="SsTableWriter.Write(MemTable)"/>
    /// before returning the copied SSTable bytes.
    /// </summary>
    public byte[] FreezeAndFlush(MemTable memTable)
    {
        ArgumentNullException.ThrowIfNull(memTable);
        if (memTable.Count == 0)
        {
            throw new InvalidOperationException("Cannot flush an empty MemTable.");
        }

        memTable.Freeze();
        return SsTableWriter.Write(memTable);
    }
}
