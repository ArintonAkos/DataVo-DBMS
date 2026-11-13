using System.Buffers.Binary;
using DataVo.Core.Indexing.Columnar;
using DataVo.Core.Transactions;

namespace DataVo.Core.StorageEngine.Lsm;

/// <summary>
/// The <c>COLUMNAR</c> file of a table's directory: the <see cref="ColumnarSpec"/> the table's SSTables are
/// written with, followed by its CRC32C. It is what lets a flush that runs right after a restart, before any
/// query has asked for the columnar index, still write a columnar segment.
/// </summary>
internal static class LsmColumnarSpecFile
{
    private const string FileName = "COLUMNAR";

    /// <summary>
    /// The spec stored for the table in <paramref name="tableDirectory"/>, or null when there is none or the
    /// file is damaged. A lost spec only costs a rebuild: the next query that wants the index stores it again.
    /// </summary>
    public static ColumnarSpec? TryLoad(string tableDirectory)
    {
        string path = Path.Combine(tableDirectory, FileName);
        if (!File.Exists(path))
        {
            return null;
        }

        try
        {
            byte[] bytes = File.ReadAllBytes(path);
            if (bytes.Length <= sizeof(uint)
                || WalCrc32C.HashToUInt32(bytes.AsSpan(0, bytes.Length - sizeof(uint))) != BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(bytes.Length - sizeof(uint))))
            {
                return null;
            }

            return ColumnarSpec.ReadFrom(bytes.AsSpan(0, bytes.Length - sizeof(uint)));
        }
        catch (Exception ex) when (ex is IOException or InvalidDataException)
        {
            return null;
        }
    }

    /// <summary>Stores <paramref name="spec"/> for the table, replacing any it had, durably and atomically.</summary>
    public static void Save(string tableDirectory, ColumnarSpec spec)
    {
        var bytes = new byte[spec.EncodedLength + sizeof(uint)];
        spec.WriteTo(bytes);
        BinaryPrimitives.WriteUInt32LittleEndian(bytes.AsSpan(spec.EncodedLength), WalCrc32C.HashToUInt32(bytes.AsSpan(0, spec.EncodedLength)));
        LsmDurableFileOperations.WriteFileAtomically(
            Path.Combine(tableDirectory, FileName),
            overwrite: true,
            stream => stream.Write(bytes, 0, bytes.Length),
            crashHook: null,
            LsmCrashPoint.AfterSstableTempFileFsyncBeforeRename,
            LsmCrashPoint.AfterSstableRenameBeforeDirectoryFsync,
            LsmCrashPoint.AfterSstableDirectoryFsyncBeforeManifest);
    }

    /// <summary>Removes the table's stored spec, if it has one.</summary>
    public static void Delete(string tableDirectory)
    {
        string path = Path.Combine(tableDirectory, FileName);
        if (File.Exists(path))
        {
            File.Delete(path);
        }
    }
}
