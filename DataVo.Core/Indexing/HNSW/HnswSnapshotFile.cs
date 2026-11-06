using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Core.Indexing.HNSW;

/// <summary>
/// The on-disk HNSW snapshot of an LSM table: magic, format version, the commit LSN the graph reflects
/// (the watermark), the indexed column, the graph, and a CRC32C over all of it.
/// </summary>
/// <remarks>
/// The rows stay the source of truth. A snapshot only saves the rebuild: rows whose version sequence is
/// above the watermark are re-indexed from the table on open, and rows the table no longer has are
/// removed. Any snapshot that fails to load is ignored and the index is rebuilt from the rows.
/// </remarks>
internal static class HnswSnapshotFile
{
    public const string DirectoryName = ".vec";
    public const string Extension = ".hnsw";

    private const long Magic = 0x31_57_53_4E_48_4F_56_44; // "DVOHNSW1" little-endian
    // 2: nodes are keyed by 16-byte row keys (version 1 held 8-byte row ids; such files are rebuilt).
    private const int FormatVersion = 2;
    private const int MaxColumnBytes = 4096;
    private const long MaxPlausibleWatermark = 1L << 62;

    /// <summary>Snapshots use raw little-endian arrays; other hosts always rebuild.</summary>
    public static bool IsSupported => BitConverter.IsLittleEndian;

    /// <summary>Writes the snapshot atomically (temp file, fsync, rename, directory fsync).</summary>
    public static void Write(string path, HNSWIndex index, long watermark, string column)
    {
        LsmDurableFileOperations.WriteFileAtomically(
            path,
            overwrite: true,
            stream =>
            {
                var writer = new ChecksummedWriter(stream);
                writer.WriteInt64(Magic);
                writer.WriteInt32(FormatVersion);
                writer.WriteInt64(watermark);
                writer.WriteString(column);
                index.WriteSnapshot(writer);
                writer.Finish();
            },
            crashHook: null,
            LsmCrashPoint.AfterSstableTempFileFsyncBeforeRename,
            LsmCrashPoint.AfterSstableRenameBeforeDirectoryFsync,
            LsmCrashPoint.AfterSstableDirectoryFsyncBeforeManifest);
    }

    /// <summary>
    /// Loads a snapshot of <paramref name="column"/>. Returns <see langword="false"/> when the file is
    /// missing, damaged, from another format version, or indexes a different column.
    /// </summary>
    public static bool TryRead(string path, string column, out HNSWIndex? index, out long watermark)
    {
        index = null;
        watermark = 0;
        if (!IsSupported || !File.Exists(path))
        {
            return false;
        }

        try
        {
            using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 1 << 20, FileOptions.SequentialScan);
            var reader = new ChecksummedReader(stream);
            if (reader.ReadInt64() != Magic || reader.ReadInt32() != FormatVersion)
            {
                return false;
            }

            long savedWatermark = reader.ReadInt64();
            if (savedWatermark is < 0 or > MaxPlausibleWatermark || !string.Equals(reader.ReadString(MaxColumnBytes), column, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            HNSWIndex loaded = HNSWIndex.ReadSnapshot(reader);
            reader.VerifyEnd();
            index = loaded;
            watermark = savedWatermark;
            return true;
        }
        catch (Exception ex) when (ex is IOException or InvalidDataException or UnauthorizedAccessException or OverflowException or ArgumentException)
        {
            return false;
        }
    }

    /// <summary>
    /// Reads only the watermark of every snapshot in a table directory. The storage engine raises its
    /// LSN floor to these values: a row written after a snapshot must get a sequence above the snapshot's
    /// watermark, or opening the snapshot would treat that row as already indexed.
    /// </summary>
    public static long MaxWatermark(string tableDirectory)
    {
        string directory = Path.Combine(tableDirectory, DirectoryName);
        if (!IsSupported || !Directory.Exists(directory))
        {
            return 0;
        }

        long max = 0;
        foreach (string path in Directory.EnumerateFiles(directory, "*" + Extension))
        {
            try
            {
                using var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize: 64);
                var reader = new ChecksummedReader(stream);
                if (reader.ReadInt64() == Magic && reader.ReadInt32() == FormatVersion)
                {
                    // A damaged header must not push LSNs toward overflow; TryRead rejects such a file.
                    long watermark = reader.ReadInt64();
                    if (watermark is > 0 and <= MaxPlausibleWatermark)
                    {
                        max = Math.Max(max, watermark);
                    }
                }
            }
            catch (IOException)
            {
                // Unreadable: TryRead rejects it too, so its watermark is never relied on.
            }
        }

        return max;
    }

    /// <summary>Deletes a snapshot if present.</summary>
    public static void Delete(string path)
    {
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}
