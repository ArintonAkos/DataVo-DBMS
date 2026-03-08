using System.IO.MemoryMappedFiles;

namespace DataVo.Core.BTree.Binary;

/// <summary>
/// Provides low-level page allocation and page I/O for binary B-Tree index files.
/// </summary>
/// <remarks>
/// Page <c>0</c> is reserved for metadata. All tree pages begin at page <c>1</c>.
/// A memory-mapped file is used so the operating system can manage page caching efficiently.
/// </remarks>
public class DiskPager : IDisposable
{
    private readonly FileStream _fs;
    private MemoryMappedFile? _mmf;
    private MemoryMappedViewAccessor? _accessor;
    private const long InitialCapacity = 10L * 1024 * 1024; // 10MB init map

    /// <summary>
    /// Gets or sets the page ID of the tree root.
    /// </summary>
    public int RootPageId { get; set; }

    /// <summary>
    /// Gets the total number of allocated pages, including the metadata page.
    /// </summary>
    public int NumPages { get; private set; }

    /// <summary>
    /// Initializes a pager for the specified B-Tree file.
    /// </summary>
    /// <param name="filePath">The path to the backing index file.</param>
    public DiskPager(string filePath)
    {
        string? directory = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
        }

        bool isNew = !File.Exists(filePath);
        _fs = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);

        if (isNew || _fs.Length < BTreePage.PageSize)
        {
            _fs.SetLength(Math.Max(_fs.Length, InitialCapacity));
            isNew = true;
        }

        // leaveOpen = true ensures MMF.Dispose doesn't close our _fs
        _mmf = MemoryMappedFile.CreateFromFile(_fs, null, 0, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, leaveOpen: true);
        _accessor = _mmf.CreateViewAccessor();

        if (isNew)
        {
            RootPageId = -1;
            NumPages = 1; // Page 0 is metadata
            WriteMetadata();
        }
        else
        {
            ReadMetadata();
        }
    }

    /// <summary>
    /// Reads pager metadata from page <c>0</c>.
    /// </summary>
    public void ReadMetadata()
    {
        byte[] meta = new byte[BTreePage.PageSize];
        _accessor!.ReadArray(0, meta, 0, BTreePage.PageSize);
        RootPageId = BitConverter.ToInt32(meta, 0);
        NumPages = BitConverter.ToInt32(meta, 4);
    }

    /// <summary>
    /// Writes pager metadata to page <c>0</c>.
    /// </summary>
    public void WriteMetadata()
    {
        byte[] meta = new byte[BTreePage.PageSize];
        byte[] rootBytes = BitConverter.GetBytes(RootPageId);
        byte[] numBytes = BitConverter.GetBytes(NumPages);
        Array.Copy(rootBytes, 0, meta, 0, 4);
        Array.Copy(numBytes, 0, meta, 4, 4);
        _accessor!.WriteArray(0, meta, 0, BTreePage.PageSize);
    }

    /// <summary>
    /// Allocates a new empty page, writes it to disk, and updates metadata.
    /// </summary>
    /// <returns>The newly allocated page.</returns>
    public BTreePage AllocatePage()
    {
        int pageId = NumPages++;

        long requiredOffset = (long)NumPages * BTreePage.PageSize;
        if (requiredOffset >= _accessor!.Capacity)
        {
            // Expand mapping by 10MB
            GrowMap(requiredOffset + InitialCapacity);
        }

        var page = new BTreePage { PageId = pageId };
        WritePage(page);
        WriteMetadata();
        return page;
    }

    /// <summary>
    /// Expands the memory-mapped file to the specified capacity.
    /// </summary>
    /// <param name="newCapacity">The desired file capacity in bytes.</param>
    private void GrowMap(long newCapacity)
    {
        _accessor?.Flush();
        _accessor?.Dispose();
        _mmf?.Dispose();

        _fs.SetLength(newCapacity);
        _mmf = MemoryMappedFile.CreateFromFile(_fs, null, 0, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, leaveOpen: true);
        _accessor = _mmf.CreateViewAccessor();
    }

    /// <summary>
    /// Serializes and writes a page to its fixed page slot.
    /// </summary>
    /// <param name="page">The page to write.</param>
    public void WritePage(BTreePage page)
    {
        long offset = (long)page.PageId * BTreePage.PageSize;
        _accessor!.WriteArray(offset, page.Serialize(), 0, BTreePage.PageSize);
    }

    /// <summary>
    /// Reads and deserializes a page from the specified page ID.
    /// </summary>
    /// <param name="pageId">The page ID to read.</param>
    /// <returns>The deserialized <see cref="BTreePage"/>.</returns>
    public BTreePage ReadPage(int pageId)
    {
        long offset = (long)pageId * BTreePage.PageSize;
        byte[] data = new byte[BTreePage.PageSize];
        _accessor!.ReadArray(offset, data, 0, BTreePage.PageSize);
        return BTreePage.Deserialize(data);
    }

    /// <summary>
    /// Flushes metadata and releases all memory-mapped resources.
    /// </summary>
    public void Dispose()
    {
        WriteMetadata();
        _accessor?.Flush();
        _accessor?.Dispose();
        _mmf?.Dispose();
        _fs.Dispose();
    }
}
