using DataVo.Core.BTree.BPlus;
using System.Collections.Concurrent;

namespace DataVo.Tests.BTree;

public class BinaryBPlusTreeIndexTests
{
    [Fact]
    public void InsertAndSearch_RoundTrip_ReturnsInsertedRowId()
    {
        string path = BuildTempIndexPath();

        using var index = new BinaryBPlusTreeIndex();
        index.Load(path);

        index.Insert("alpha", 11);
        index.Insert("beta", 22);
        index.Insert("gamma", 33);

        Assert.Contains(11, index.Search("alpha"));
        Assert.Contains(22, index.Search("beta"));
        Assert.Contains(33, index.Search("gamma"));

        File.Delete(path);
    }

    [Fact]
    public async Task ConcurrentSearchAndInsert_LatchCrabbing_DoesNotThrowAndPreservesRows()
    {
        string path = BuildTempIndexPath();

        using var index = new BinaryBPlusTreeIndex();
        index.Load(path);

        for (int i = 0; i < 200; i++)
        {
            index.Insert($"seed-{i}", i + 1);
        }

        var exceptions = new ConcurrentQueue<Exception>();

        Task writer = Task.Run(() =>
        {
            try
            {
                for (int i = 200; i < 400; i++)
                {
                    index.Insert($"seed-{i}", i + 1);
                }
            }
            catch (Exception ex)
            {
                exceptions.Enqueue(ex);
            }
        });

        Task[] readers = Enumerable.Range(0, 4)
            .Select(_ => Task.Run(() =>
            {
                var rnd = new Random(Environment.TickCount ^ Thread.CurrentThread.ManagedThreadId);
                try
                {
                    for (int i = 0; i < 1500; i++)
                    {
                        int id = rnd.Next(0, 400);
                        index.Search($"seed-{id}");
                    }
                }
                catch (Exception ex)
                {
                    exceptions.Enqueue(ex);
                }
            }))
            .ToArray();

        await Task.WhenAll(readers.Append(writer));

        Assert.True(exceptions.IsEmpty, string.Join(Environment.NewLine, exceptions.Select(e => e.ToString())));

        for (int i = 0; i < 400; i++)
        {
            List<long> rowIds = index.Search($"seed-{i}");
            Assert.Contains(i + 1, rowIds);
        }

        File.Delete(path);
    }

    private static string BuildTempIndexPath()
    {
        string dir = Path.Combine(Path.GetTempPath(), "datavo-bplus-tests");
        Directory.CreateDirectory(dir);
        return Path.Combine(dir, $"bplus_{Guid.NewGuid():N}.btree");
    }
}
