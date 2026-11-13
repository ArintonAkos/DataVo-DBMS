using DataVo.Core.Memory.Flat;

namespace DataVo.Tests.Memory;

public class FlatMemoryTests
{
    [Fact]
    public void Chunk_IsAlignedZeroedAndTypedByStride()
    {
        var chunk = new FlatChunk(sizeof(long));

        Assert.True(chunk.IsAligned);
        Assert.Equal(FlatChunk.Slots, chunk.As<long>().Length);
        Assert.Equal(FlatChunk.Slots * sizeof(long), chunk.Bytes.Length);
        Assert.True(chunk.Bytes.IndexOfAnyExcept((byte)0) < 0);
        Assert.Throws<InvalidOperationException>(() => chunk.As<int>());
    }

    [Fact]
    public void Chunk_SlotAndTypedViewsShareMemory()
    {
        var chunk = new FlatChunk(sizeof(int));
        chunk.As<int>()[7] = 0x01020304;

        Assert.Equal(new byte[] { 4, 3, 2, 1 }, chunk.Slot(7).ToArray());
    }

    [Fact]
    public void Chunk_HoldsPackedRecordsOfAnyStride()
    {
        var chunk = new FlatChunk(45);
        chunk.Slot(FlatChunk.Slots - 1).Fill(0xAB);

        Assert.Equal(45, chunk.Slot(3).Length);
        Assert.Equal(0, chunk.Slot(FlatChunk.Slots - 2)[44]);
        Assert.Equal(0xAB, chunk.Bytes[^1]);
    }

    [Fact]
    public void Directory_GrowsWithoutMovingExistingChunks()
    {
        var directory = new ChunkDirectory(sizeof(int));
        directory.EnsureSlots(1);
        FlatChunk first = directory.Chunk(0);
        first.As<int>()[0] = 42;

        directory.EnsureSlots(FlatChunk.Slots * 9 + 1);

        Assert.Equal(FlatChunk.Slots * 10, directory.Capacity);
        Assert.Same(first, directory.Chunk(0));
        Assert.Same(directory.Chunk(9), directory.ChunkOf(FlatChunk.Slots * 9));
        Assert.Equal(42, directory.ChunkOf(0).As<int>()[0]);
    }

    [Fact]
    public void SlotList_PublishesRecordsInOrder()
    {
        var list = new FlatSlotList(12);
        Assert.False(list.TryRead(0, out _));

        for (int i = 0; i < FlatChunk.Slots + 5; i++)
        {
            Span<byte> slot = list.Next();
            BitConverter.TryWriteBytes(slot, i);
            Assert.False(list.TryRead(i, out _)); // written but not yet published
            Assert.Equal(i, list.Publish());
        }

        Assert.Equal(FlatChunk.Slots + 5, list.Count);
        Assert.True(list.TryRead(FlatChunk.Slots + 4, out ReadOnlySpan<byte> last));
        Assert.Equal(FlatChunk.Slots + 4, BitConverter.ToInt32(last));
        Assert.False(list.TryRead(-1, out _));
    }

    [Fact]
    public void Directory_SmallFirstChunkGrowsByCopying_UntilItIsFullSize()
    {
        var directory = new ChunkDirectory(sizeof(int), firstChunkSlots: 16);
        directory.EnsureSlots(1);
        FlatChunk small = directory.Chunk(0);
        small.As<int>()[15] = 77;
        Assert.Equal(16, small.Capacity);
        Assert.Equal(16, directory.Capacity);

        directory.EnsureSlots(17);
        FlatChunk grown = directory.Chunk(0);
        Assert.NotSame(small, grown);
        Assert.Equal(64, grown.Capacity);
        Assert.Equal(77, grown.As<int>()[15]);
        Assert.Equal(0, grown.As<int>()[16]);

        directory.EnsureSlots(FlatChunk.Slots + 1);
        Assert.Equal(FlatChunk.Slots, directory.Chunk(0).Capacity);
        Assert.Equal(77, directory.Chunk(0).As<int>()[15]);
        Assert.Equal(FlatChunk.Slots * 2, directory.Capacity);
        FlatChunk full = directory.Chunk(0);
        directory.EnsureSlots(FlatChunk.Slots * 3);
        Assert.Same(full, directory.Chunk(0));
    }

    [Fact]
    public void ReferenceDirectory_GivesEachSlotItsOwnRun_AndKeepsReferencesWhenItGrows()
    {
        var references = new ReferenceDirectory(perSlot: 2, firstChunkSlots: 4);
        references.EnsureSlots(4);
        references.ChunkOf(3)[references.BaseOf(3) + 1] = "last of the small chunk";
        Assert.Equal(8, references.ChunkOf(0).Length);
        Assert.Equal(6, references.BaseOf(3));

        references.EnsureSlots(FlatChunk.Slots + 2);
        Assert.Equal("last of the small chunk", references.ChunkOf(3)[7]);
        Assert.Equal(FlatChunk.Slots * 2, references.ChunkOf(FlatChunk.Slots + 1).Length);
        Assert.Equal(2, references.BaseOf(FlatChunk.Slots + 1));
        Assert.NotSame(references.ChunkOf(0), references.ChunkOf(FlatChunk.Slots));
    }

    [Fact]
    public void SlotList_EditReachesAPublishedRecord_AndNothingElse()
    {
        var list = new FlatSlotList(4, firstChunkSlots: 2);
        for (int i = 0; i < 5; i++)
        {
            BitConverter.TryWriteBytes(list.Next(), i);
            list.Publish();
        }

        list.Edit(3)[0] = 0xFF;

        Assert.True(list.TryRead(3, out ReadOnlySpan<byte> record));
        Assert.Equal(0xFF, record[0]);
        Assert.Throws<ArgumentOutOfRangeException>(() => list.Edit(5));
    }

    [Fact]
    public void SlotList_ReaderSeesOnlyWholeRecordsWhileAWriterAppends()
    {
        const int Total = 200_000;
        var list = new FlatSlotList(sizeof(long) * 2);
        var writer = new Thread(() =>
        {
            for (long i = 0; i < Total; i++)
            {
                Span<byte> slot = list.Next();
                BitConverter.TryWriteBytes(slot, i);
                BitConverter.TryWriteBytes(slot.Slice(sizeof(long)), ~i);
                list.Publish();
            }
        });

        writer.Start();
        int seen = 0;
        while (seen < Total)
        {
            int count = list.Count;
            for (; seen < count; seen++)
            {
                Assert.True(list.TryRead(seen, out ReadOnlySpan<byte> record));
                Assert.Equal(seen, BitConverter.ToInt64(record));
                Assert.Equal(~(long)seen, BitConverter.ToInt64(record.Slice(sizeof(long))));
            }
        }

        writer.Join();
    }

    [Fact]
    public void Bitmask_SetClearTestAndCount()
    {
        Span<ulong> words = stackalloc ulong[Bitmask.WordsPerChunk];
        Bitmask.Set(words, 0);
        Bitmask.Set(words, 63);
        Bitmask.Set(words, 64);
        Bitmask.Set(words, FlatChunk.Slots - 1);

        Assert.True(Bitmask.Test(words, 63));
        Assert.False(Bitmask.Test(words, 62));
        Assert.Equal(4, Bitmask.Count(words));

        Bitmask.Clear(words, 63);
        Assert.False(Bitmask.Test(words, 63));
        Assert.Equal(3, Bitmask.Count(words));
    }

    [Fact]
    public void Bitmask_CombinesWordByWord()
    {
        ulong[] a = [0b1100, ulong.MaxValue];
        ulong[] b = [0b1010, 1];

        ulong[] and = [.. a];
        Bitmask.And(and, b);
        ulong[] or = [.. a];
        Bitmask.Or(or, b);
        ulong[] andNot = [.. a];
        Bitmask.AndNot(andNot, b);
        ulong[] not = [.. b];
        Bitmask.Not(not);

        Assert.Equal(new ulong[] { 0b1000, 1 }, and);
        Assert.Equal(new ulong[] { 0b1110, ulong.MaxValue }, or);
        Assert.Equal(new ulong[] { 0b0100, ulong.MaxValue - 1 }, andNot);
        Assert.Equal(new ulong[] { ~0b1010UL, ~1UL }, not);
    }

    [Theory]
    [InlineData(0, 0)]
    [InlineData(1, 1)]
    [InlineData(64, 64)]
    [InlineData(70, 70)]
    [InlineData(128, 128)]
    [InlineData(500, 192)]
    public void Bitmask_TrimKeepsOnlyTheLeadingBits(int bits, int expected)
    {
        ulong[] words = [ulong.MaxValue, ulong.MaxValue, ulong.MaxValue];
        Bitmask.TrimTo(words, bits);

        Assert.Equal(expected, Bitmask.Count(words));
        Assert.Equal(Math.Min(bits, 192) > 0, Bitmask.Test(words, 0));
    }

    [Fact]
    public void Bitmask_WordHelpersMatchANaiveCount()
    {
        var random = new Random(7);
        for (int i = 0; i < 1000; i++)
        {
            ulong word = (ulong)random.NextInt64() | ((ulong)random.Next(2) << 63);
            int naive = 0;
            int lowest = -1;
            for (int bit = 0; bit < 64; bit++)
            {
                if ((word >> bit & 1) != 0)
                {
                    naive++;
                    lowest = lowest < 0 ? bit : lowest;
                }
            }

            Assert.Equal(naive, Bitmask.PopCount(word));
            if (word != 0)
            {
                Assert.Equal(lowest, Bitmask.LowestSet(word));
            }
        }
    }

    [Fact]
    public void BitmaskDirectory_AddressesBitsAcrossChunks()
    {
        var mask = new BitmaskDirectory();
        int far = FlatChunk.Slots * 70 + 129; // past the first chunk of words
        mask.EnsureSlots(far + 1);
        mask.Set(5);
        mask.Set(far);

        Assert.True(mask.Test(5));
        Assert.True(mask.Test(far));
        Assert.False(mask.Test(far - 1));
        Assert.Equal(1, Bitmask.Count(mask.Words(0)));
        Assert.Equal(1, Bitmask.Count(mask.Words(70)));
        Assert.True(Bitmask.Test(mask.Words(70), 129));

        mask.Clear(far);
        Assert.Equal(0, Bitmask.Count(mask.Words(70)));
    }
}
