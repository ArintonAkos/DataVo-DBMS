using DataVo.Core.BTree;

namespace DataVo.Tests.BTree;

public class BTreePersistenceTests
{
    private string GetTempFilePath()
    {
        string dir = Path.Combine(Path.GetTempPath(), "datavo_btree_tests");
        Directory.CreateDirectory(dir);
        return Path.Combine(dir, $"test_index_{Guid.NewGuid()}.btree");
    }

    [Fact]
    public void SaveAndLoad_PreservesAllData()
    {
        string filePath = GetTempFilePath();
        try
        {
            var index = new JsonBTreeIndex(3);
            index.Insert("alice", "row1");
            index.Insert("bob", "row2");
            index.Insert("alice", "row3"); // duplicate key

            index.Save(filePath);

            var loaded = JsonBTreeIndex.Load(filePath);

            Assert.Equal(2, loaded.Search("alice").Count);
            Assert.Contains("row1", loaded.Search("alice"));
            Assert.Contains("row3", loaded.Search("alice"));
            Assert.Single(loaded.Search("bob"));
            Assert.Equal("row2", loaded.Search("bob")[0]);
        }
        finally
        {
            if (File.Exists(filePath)) File.Delete(filePath);
        }
    }

    [Fact]
    public void SaveAndLoad_LargeIndex_SurvivesRoundTrip()
    {
        string filePath = GetTempFilePath();
        try
        {
            var index = new JsonBTreeIndex(2); // small degree forces many nodes
            for (int i = 0; i < 200; i++)
            {
                index.Insert($"key_{i:D4}", $"row_{i}");
            }

            index.Save(filePath);
            var loaded = JsonBTreeIndex.Load(filePath);

            for (int i = 0; i < 200; i++)
            {
                var result = loaded.Search($"key_{i:D4}");
                Assert.Single(result);
                Assert.Equal($"row_{i}", result[0]);
            }
        }
        finally
        {
            if (File.Exists(filePath)) File.Delete(filePath);
        }
    }

    [Fact]
    public void Load_NonExistentFile_ThrowsFileNotFoundException()
    {
        Assert.Throws<FileNotFoundException>(() =>
        {
            JsonBTreeIndex.Load("/tmp/does_not_exist.btree");
        });
    }

    [Fact]
    public void Save_CreatesDirectoryIfNotExists()
    {
        string dir = Path.Combine(Path.GetTempPath(), "datavo_btree_tests", $"subdir_{Guid.NewGuid()}");
        string filePath = Path.Combine(dir, "test.btree");
        try
        {
            var index = new JsonBTreeIndex(3);
            index.Insert("test", "value");
            index.Save(filePath);

            Assert.True(File.Exists(filePath));

            var loaded = JsonBTreeIndex.Load(filePath);
            Assert.Single(loaded.Search("test"));
        }
        finally
        {
            if (Directory.Exists(dir)) Directory.Delete(dir, recursive: true);
        }
    }

    [Fact]
    public void SaveAndLoad_EmptyIndex_Works()
    {
        string filePath = GetTempFilePath();
        try
        {
            var index = new JsonBTreeIndex(3);
            index.Save(filePath);

            var loaded = JsonBTreeIndex.Load(filePath);
            Assert.Empty(loaded.Search("anything"));
        }
        finally
        {
            if (File.Exists(filePath)) File.Delete(filePath);
        }
    }
}
