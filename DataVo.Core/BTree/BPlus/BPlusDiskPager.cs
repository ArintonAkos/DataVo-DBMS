using System.IO.MemoryMappedFiles;

namespace DataVo.Core.BTree.BPlus;

/// <summary>
/// Provides low-level page allocation and page I/O for binary B+Tree index files.
/// </summary>
/// <remarks>
/// Page <c>0</c> stores pager metadata. All B+Tree pages begin at page <c>1</c>.
/// The file is memory-mapped so the operating system can efficiently cache and flush pages.
/// </remarks>
public class BPlusDiskPager : IDisposable
{
    private readonly FileStream _fs;
    private MemoryMappedFile? _mmf;
    private MemoryMappedViewAccessor? _accessor;
    private const long InitialCapacity = 10L * 1024 * 1024; // 10MB init map

    /// <summary>
    /// Gets or sets the page ID of the B+Tree root.
    /// </summary>
    public int RootPageId { get; set; }

    /// <summary>
    /// Gets the total number of allocated pages, including the metadata page.
    /// </summary>
    public int NumPages { get; private set; }

    /// <summary>
    /// Initializes a pager for the specified B+Tree file.
    /// </summary>
    /// <param name="filePath">The path to the backing <c>.btree</c> file.</param>
    public BPlusDiskPager(string filePath)
    {
        string? directory = Path.GetDirectoryName(filePath);
        if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
        {
            Directory.CreateDirectory(directory);
        }

        bool isNew = !File.Exists(filePath);
        _fs = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);

        if (isNew || _fs.Length < BPlusTreePage.PageSize)
        {
            _fs.SetLength(Math.Max(_fs.Length, InitialCapacity));
            isNew = true;
        }

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
        byte[] meta = new byte[BPlusTreePage.PageSize];
        _accessor!.ReadArray(0, meta, 0, BPlusTreePage.PageSize);
        RootPageId = BitConverter.ToInt32(meta, 0);
        NumPages = BitConverter.ToInt32(meta, 4);
    }

    /// <summary>
    /// Writes pager metadata to page <c>0</c>.
    /// </summary>
    public void WriteMetadata()
    {
        byte[] meta = new byte[BPlusTreePage.PageSize];
        byte[] rootBytes = BitConverter.GetBytes(RootPageId);
        byte[] numBytes = BitConverter.GetBytes(NumPages);
        Array.Copy(rootBytes, 0, meta, 0, 4);
        Array.Copy(numBytes, 0, meta, 4, 4);
        _accessor!.WriteArray(0, meta, 0, BPlusTreePage.PageSize);
    }

    /// <summary>
    /// Allocates a new empty page, writes it to disk, and updates metadata.
    /// </summary>
    /// <returns>The newly allocated page.</returns>
    public BPlusTreePage AllocatePage()
    {
        int pageId = NumPages++;

        long requiredOffset = (long)NumPages * BPlusTreePage.PageSize;
        if (requiredOffset >= _accessor!.Capacity)
        {
            // Expand mapping by 10MB
            GrowMap(requiredOffset + InitialCapacity);
        }

        var page = new BPlusTreePage { PageId = pageId };
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
    /// Serializes and writes a page to disk.
    /// </summary>
    /// <param name="page">The page to write.</param>
    public void WritePage(BPlusTreePage page)
    {
        long offset = (long)page.PageId * BPlusTreePage.PageSize;
        _accessor!.WriteArray(offset, page.Serialize(), 0, BPlusTreePage.PageSize);
    }

    /// <summary>
    /// Reads and deserializes a page from the specified page ID.
    /// </summary>
    /// <param name="pageId">The page ID to read.</param>
    /// <returns>The deserialized <see cref="BPlusTreePage"/>.</returns>
    public BPlusTreePage ReadPage(int pageId)
    {
        long offset = (long)pageId * BPlusTreePage.PageSize;
        byte[] data = new byte[BPlusTreePage.PageSize];
        _accessor!.ReadArray(offset, data, 0, BPlusTreePage.PageSize);
        return BPlusTreePage.Deserialize(data);
    }

    /// <summary>
    /// Flushes metadata and releases all file-mapping resources.
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
