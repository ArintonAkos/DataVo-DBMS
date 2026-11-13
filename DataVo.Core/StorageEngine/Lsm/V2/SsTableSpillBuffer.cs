using System.Buffers;

namespace DataVo.Core.StorageEngine.Lsm.V2;

/// <summary>Where an SSTable writer may put what it must keep until <c>Finish</c>, once that outgrows memory.</summary>
/// <param name="Directory">The directory of the temporary files (the table's own).</param>
/// <param name="MemoryLimit">The bytes one buffer keeps in memory before it moves to a file.</param>
internal sealed record SsTableSpillOptions(string Directory, long MemoryLimit)
{
    /// <summary>The file name extension of spill files; any left behind by a crash can be deleted.</summary>
    public const string Extension = ".spill";

    /// <summary>The memory limit compaction uses unless told otherwise.</summary>
    public const long DefaultMemoryLimit = 4L * 1024 * 1024;

    /// <summary>
    /// Deletes the spill files a crash left in <paramref name="directory"/>. Call it when a table is opened, before
    /// anything can write to it; a file still held open elsewhere is left alone.
    /// </summary>
    /// <returns>The number of files deleted.</returns>
    public static int DeleteLeftovers(string directory)
    {
        int deleted = 0;
        foreach (string path in System.IO.Directory.EnumerateFiles(directory, "*" + Extension))
        {
            try
            {
                File.Delete(path);
                deleted++;
            }
            catch (IOException)
            {
            }
            catch (UnauthorizedAccessException)
            {
            }
        }

        return deleted;
    }
}

/// <summary>
/// An append-only byte buffer read back once, front to back: in pooled pages up to a limit, then in a temporary
/// file that is deleted when the buffer is disposed (or, after a crash, by the next open of the table).
/// </summary>
/// <remarks>
/// Without <see cref="SsTableSpillOptions"/> it never leaves memory: a flush writes a bounded MemTable and pays
/// nothing extra; a compaction can write any number of rows and stays within the limit.
/// </remarks>
internal sealed class SsTableSpillBuffer : IDisposable
{
    private const int PageSize = 64 * 1024;

    private readonly SsTableSpillOptions? _options;
    private readonly List<byte[]> _pages = [];
    private int _lastPageUsed = PageSize;
    private FileStream? _file;
    private int _readPage;
    private int _readOffset;

    /// <summary>Creates a buffer that spills under <paramref name="options"/>, or never when it is null.</summary>
    public SsTableSpillBuffer(SsTableSpillOptions? options)
    {
        _options = options;
    }

    /// <summary>The bytes written.</summary>
    public long Length { get; private set; }

    /// <summary>Whether the bytes are in a file rather than memory.</summary>
    public bool Spilled => _file is not null;

    /// <summary>Appends <paramref name="bytes"/>.</summary>
    public void Write(ReadOnlySpan<byte> bytes)
    {
        if (_file is null && _options is { } options && Length + bytes.Length > options.MemoryLimit)
        {
            MoveToFile(options);
        }

        Length += bytes.Length;
        if (_file is not null)
        {
            _file.Write(bytes);
            return;
        }

        while (!bytes.IsEmpty)
        {
            if (_lastPageUsed == PageSize)
            {
                _pages.Add(ArrayPool<byte>.Shared.Rent(PageSize));
                _lastPageUsed = 0;
            }

            int count = Math.Min(bytes.Length, PageSize - _lastPageUsed);
            bytes[..count].CopyTo(_pages[^1].AsSpan(_lastPageUsed));
            _lastPageUsed += count;
            bytes = bytes[count..];
        }
    }

    /// <summary>Writes every byte, in order, to <paramref name="output"/>.</summary>
    public void CopyTo(Stream output)
    {
        Rewind();
        byte[] buffer = ArrayPool<byte>.Shared.Rent(PageSize);
        try
        {
            int read;
            while ((read = Read(buffer)) > 0)
            {
                output.Write(buffer, 0, read);
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>Starts reading from the first byte.</summary>
    public void Rewind()
    {
        _readPage = 0;
        _readOffset = 0;
        if (_file is not null)
        {
            _file.Flush();
            _file.Position = 0;
        }
    }

    /// <summary>Reads the next bytes into <paramref name="destination"/>; returns how many (0 at the end).</summary>
    public int Read(Span<byte> destination)
    {
        if (_file is not null)
        {
            int total = 0;
            while (total < destination.Length)
            {
                int read = _file.Read(destination[total..]);
                if (read == 0)
                {
                    break;
                }

                total += read;
            }

            return total;
        }

        int copied = 0;
        while (copied < destination.Length && _readPage < _pages.Count)
        {
            int used = _readPage == _pages.Count - 1 ? _lastPageUsed : PageSize;
            int count = Math.Min(destination.Length - copied, used - _readOffset);
            _pages[_readPage].AsSpan(_readOffset, count).CopyTo(destination[copied..]);
            copied += count;
            _readOffset += count;
            if (_readOffset == used)
            {
                _readPage++;
                _readOffset = 0;
            }
        }

        return copied;
    }

    /// <summary>Returns the pages and deletes the file.</summary>
    public void Dispose()
    {
        ReturnPages();
        _file?.Dispose();
        _file = null;
    }

    private void MoveToFile(SsTableSpillOptions options)
    {
        string path = Path.Combine(options.Directory, Guid.NewGuid().ToString("N") + SsTableSpillOptions.Extension);
        _file = new FileStream(path, FileMode.CreateNew, FileAccess.ReadWrite, FileShare.None, PageSize, FileOptions.DeleteOnClose | FileOptions.SequentialScan);
        for (int i = 0; i < _pages.Count; i++)
        {
            _file.Write(_pages[i], 0, i == _pages.Count - 1 ? _lastPageUsed : PageSize);
        }

        ReturnPages();
    }

    private void ReturnPages()
    {
        foreach (byte[] page in _pages)
        {
            ArrayPool<byte>.Shared.Return(page);
        }

        _pages.Clear();
        _lastPageUsed = PageSize;
    }
}
