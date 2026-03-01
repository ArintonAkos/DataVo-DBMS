using System.IO.MemoryMappedFiles;

namespace DataVo.Core.BTree.Binary;

/// <summary>
/// Handles translation between contiguous 4KB Blocks and BTreePage models.
/// Upgraded to MemoryMappedFile for Zero-Copy OS level Page Caching and False-Sharing mitigation.
/// </summary>
public class DiskPager : IDisposable
{
    private readonly FileStream _fs;
    private MemoryMappedFile? _mmf;
    private MemoryMappedViewAccessor? _accessor;
    private const long InitialCapacity = 10L * 1024 * 1024; // 10MB init map

    public int RootPageId { get; set; }
    public int NumPages { get; private set; }

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

    public void ReadMetadata()
    {
        byte[] meta = new byte[BTreePage.PageSize];
        _accessor!.ReadArray(0, meta, 0, BTreePage.PageSize);
        RootPageId = BitConverter.ToInt32(meta, 0);
        NumPages = BitConverter.ToInt32(meta, 4);
    }

    public void WriteMetadata()
    {
        byte[] meta = new byte[BTreePage.PageSize];
        byte[] rootBytes = BitConverter.GetBytes(RootPageId);
        byte[] numBytes = BitConverter.GetBytes(NumPages);
        Array.Copy(rootBytes, 0, meta, 0, 4);
        Array.Copy(numBytes, 0, meta, 4, 4);
        _accessor!.WriteArray(0, meta, 0, BTreePage.PageSize);
    }

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

    private void GrowMap(long newCapacity)
    {
        _accessor?.Flush();
        _accessor?.Dispose();
        _mmf?.Dispose();

        _fs.SetLength(newCapacity);
        _mmf = MemoryMappedFile.CreateFromFile(_fs, null, 0, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, leaveOpen: true);
        _accessor = _mmf.CreateViewAccessor();
    }

    public void WritePage(BTreePage page)
    {
        long offset = (long)page.PageId * BTreePage.PageSize;
        _accessor!.WriteArray(offset, page.Serialize(), 0, BTreePage.PageSize);
    }

    public BTreePage ReadPage(int pageId)
    {
        long offset = (long)pageId * BTreePage.PageSize;
        byte[] data = new byte[BTreePage.PageSize];
        _accessor!.ReadArray(offset, data, 0, BTreePage.PageSize);
        return BTreePage.Deserialize(data);
    }

    public void Dispose()
    {
        WriteMetadata();
        _accessor?.Flush();
        _accessor?.Dispose();
        _mmf?.Dispose();
        _fs.Dispose();
    }
}
