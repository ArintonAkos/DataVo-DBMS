using System.IO.MemoryMappedFiles;

namespace DataVo.Core.BTree.BPlus;

/// <summary>
/// Handles translation between contiguous 4KB Blocks and BPlusTreePage models.
/// Uses MemoryMappedFile for Zero-Copy OS level Page Caching and False-Sharing mitigation.
/// </summary>
public class BPlusDiskPager : IDisposable
{
    private readonly FileStream _fs;
    private MemoryMappedFile? _mmf;
    private MemoryMappedViewAccessor? _accessor;
    private const long InitialCapacity = 10L * 1024 * 1024; // 10MB init map

    public int RootPageId { get; set; }
    public int NumPages { get; private set; }

    public BPlusDiskPager(string filePath)
    {
        bool isNew = !File.Exists(filePath);
        _fs = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);

        if (isNew)
        {
            _fs.SetLength(InitialCapacity);
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

    public void ReadMetadata()
    {
        byte[] meta = new byte[BPlusTreePage.PageSize];
        _accessor!.ReadArray(0, meta, 0, BPlusTreePage.PageSize);
        RootPageId = BitConverter.ToInt32(meta, 0);
        NumPages = BitConverter.ToInt32(meta, 4);
    }

    public void WriteMetadata()
    {
        byte[] meta = new byte[BPlusTreePage.PageSize];
        byte[] rootBytes = BitConverter.GetBytes(RootPageId);
        byte[] numBytes = BitConverter.GetBytes(NumPages);
        Array.Copy(rootBytes, 0, meta, 0, 4);
        Array.Copy(numBytes, 0, meta, 4, 4);
        _accessor!.WriteArray(0, meta, 0, BPlusTreePage.PageSize);
    }

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

    private void GrowMap(long newCapacity)
    {
        _accessor?.Flush();
        _accessor?.Dispose();
        _mmf?.Dispose();

        _fs.SetLength(newCapacity);
        _mmf = MemoryMappedFile.CreateFromFile(_fs, null, 0, MemoryMappedFileAccess.ReadWrite, HandleInheritability.None, leaveOpen: true);
        _accessor = _mmf.CreateViewAccessor();
    }

    public void WritePage(BPlusTreePage page)
    {
        long offset = (long)page.PageId * BPlusTreePage.PageSize;
        _accessor!.WriteArray(offset, page.Serialize(), 0, BPlusTreePage.PageSize);
    }

    public BPlusTreePage ReadPage(int pageId)
    {
        long offset = (long)pageId * BPlusTreePage.PageSize;
        byte[] data = new byte[BPlusTreePage.PageSize];
        _accessor!.ReadArray(offset, data, 0, BPlusTreePage.PageSize);
        return BPlusTreePage.Deserialize(data);
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
