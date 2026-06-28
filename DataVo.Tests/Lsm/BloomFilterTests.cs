using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

public class BloomFilterTests
{
    private static byte[] Key(int i)
    {
        var buf = new byte[8];
        InternalKey.EncodeInt64UserKey(buf, i);
        return buf;
    }

    [Fact]
    public void Add_Then_MightContain_HasNoFalseNegatives()
    {
        BloomFilter filter = BloomFilter.Create(expectedKeys: 1000);
        for (int i = 0; i < 1000; i++)
        {
            filter.Add(Key(i));
        }

        for (int i = 0; i < 1000; i++)
        {
            Assert.True(filter.MightContain(Key(i)), $"false negative for key {i}");
        }
    }

    [Fact]
    public void MightContain_FalsePositiveRate_IsNearTarget()
    {
        const int n = 10_000;
        BloomFilter filter = BloomFilter.Create(expectedKeys: n, bitsPerKey: 10);
        for (int i = 0; i < n; i++)
        {
            filter.Add(Key(i));
        }

        int falsePositives = 0;
        for (int i = n; i < 2 * n; i++) // keys that were never added
        {
            if (filter.MightContain(Key(i)))
            {
                falsePositives++;
            }
        }

        double fpr = (double)falsePositives / n;
        // 10 bits/key targets ~1%. Allow generous headroom for hash variance.
        Assert.True(fpr < 0.03, $"false-positive rate {fpr:P2} exceeds 3%");
    }

    [Fact]
    public void ToBytes_FromBytes_RoundTripsMembership()
    {
        BloomFilter source = BloomFilter.Create(expectedKeys: 500);
        for (int i = 0; i < 500; i++)
        {
            source.Add(Key(i));
        }

        BloomFilter reloaded = BloomFilter.FromBytes(source.ToBytes());

        Assert.Equal(source.BitCount, reloaded.BitCount);
        Assert.Equal(source.NumProbes, reloaded.NumProbes);
        for (int i = 0; i < 500; i++)
        {
            Assert.True(reloaded.MightContain(Key(i)));
        }
    }

    [Fact]
    public void MightContain_IsAllocationFree()
    {
        BloomFilter filter = BloomFilter.Create(expectedKeys: 1000);
        for (int i = 0; i < 1000; i++)
        {
            filter.Add(Key(i));
        }

        byte[] probe = Key(7);
        for (int i = 0; i < 200; i++)
        {
            _ = filter.MightContain(probe); // warm
        }

        const int n = 50_000;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < n; i++)
        {
            _ = filter.MightContain(probe);
        }
        long perOp = (GC.GetAllocatedBytesForCurrentThread() - before) / n;

        Assert.True(perOp == 0, $"MightContain allocated {perOp} B/op (expected 0)");
    }
}
