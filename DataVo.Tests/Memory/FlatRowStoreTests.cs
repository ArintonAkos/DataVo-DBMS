using DataVo.Core.Memory.Flat;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Memory;

namespace DataVo.Tests.Memory;

public class FlatRowStoreTests
{
    // Id INT, Gold BIGINT, Name VARCHAR, Tag VARCHAR
    private static readonly CellType[] Columns = [CellType.Int32, CellType.Int64, CellType.String, CellType.String];

    private static FlatRowStore NewStore() => new(RowLayout.Create(Columns));

    private static CellValue[] Row(int id) =>
        [CellValue.From(id), CellValue.From(~(long)id), CellValue.From("n" + id), id % 5 == 0 ? CellValue.Null : CellValue.From("t" + id)];

    /// <summary>A row that cannot be packed: its INT column holds a BIGINT cell.</summary>
    private static CellValue[] OddRow(int id) =>
        [CellValue.From((long)id), CellValue.From(~(long)id), CellValue.From("n" + id), CellValue.From("t" + id)];

    private static void AssertIsRow(int id, ReadOnlySpan<CellValue> cells)
    {
        long readId = cells[0].Type == CellType.Int64 ? cells[0].AsInt64() : cells[0].AsInt32();
        Assert.Equal(id, readId);
        Assert.Equal(~(long)id, cells[1].AsInt64());
        Assert.Equal("n" + id, cells[2].AsString());
    }

    [Fact]
    public void Add_NumbersRowsInOrder_AndReadsThemBack_AcrossChunks()
    {
        FlatRowStore store = NewStore();
        int total = FlatChunk.Slots * 2 + 37;
        for (int id = 0; id < total; id++)
        {
            Assert.Equal(id, store.Add(Row(id)));
        }

        Assert.Equal(total, store.Count);
        var cells = new CellValue[Columns.Length];
        for (int id = 0; id < total; id++)
        {
            Assert.True(store.TryDecode(id, cells));
            FlatRowCodecTests.AssertSameCells(Row(id), cells);
        }

        Assert.False(store.TryDecode(total, cells));
        Assert.False(store.TryDecode(-1, cells));
    }

    [Fact]
    public void Add_CopiesTheCells_SoTheCallerCanReuseItsBuffer()
    {
        FlatRowStore store = NewStore();
        CellValue[] buffer = Row(1);
        store.Add(buffer);
        Row(2).CopyTo(buffer, 0);
        store.Add(buffer);

        var cells = new CellValue[Columns.Length];
        Assert.True(store.TryDecode(0, cells));
        AssertIsRow(1, cells);
    }

    [Fact]
    public void TryRead_LendsThePackedRecordAndReferences()
    {
        FlatRowStore store = NewStore();
        store.Add(Row(0));
        store.Add(Row(1));

        Assert.True(store.TryRead(1, out FlatRow row));
        Assert.False(row.IsKeptApart);
        RowLayout layout = store.Layout;
        Assert.Equal(1, FlatRowCodec.ReadInt32(row.Record, layout[0].Position));
        Assert.Equal("n1", FlatRowCodec.ReadString(row.References, row.ReferenceBase, layout[2].Position));
        Assert.Equal("t1", FlatRowCodec.ReadString(row.References, row.ReferenceBase, layout[3].Position));
    }

    [Fact]
    public void Remove_HidesTheRow_LetsGoOfItsReferences_AndKeepsOtherNumbers()
    {
        FlatRowStore store = NewStore();
        for (int id = 0; id < 3; id++)
        {
            store.Add(Row(id + 1));
        }

        Assert.True(store.TryRead(1, out FlatRow before));
        object?[] references = before.References;
        int referenceBase = before.ReferenceBase;

        Assert.True(store.Remove(1));
        Assert.False(store.Remove(1));
        Assert.False(store.Remove(99));

        Assert.False(store.IsLive(1));
        Assert.False(store.TryRead(1, out _));
        Assert.Null(references[referenceBase]);
        Assert.Null(references[referenceBase + 1]);
        Assert.Equal(3, store.Count);

        var cells = new CellValue[Columns.Length];
        Assert.True(store.TryDecode(2, cells));
        AssertIsRow(3, cells);
        Assert.Equal(3, store.Add(Row(4)));
    }

    [Fact]
    public void AddEmpty_UsesUpANumberWithoutARow()
    {
        FlatRowStore store = NewStore();
        store.Add(Row(0));
        Assert.Equal(1, store.AddEmpty());
        Assert.Equal(2, store.Add(Row(2)));

        Assert.False(store.IsLive(1));
        Assert.False(store.Remove(1));
        Assert.True(store.IsLive(2));
    }

    [Fact]
    public void RowThatCannotBePacked_IsKeptApart_ReadBackExactly_AndDroppedOnRemove()
    {
        FlatRowStore store = NewStore();
        store.Add(Row(0));
        int odd = store.Add(OddRow(1));
        store.Add(Row(2));

        Assert.Equal(1, store.KeptApartCount);
        Assert.True(store.TryRead(odd, out FlatRow row));
        Assert.True(row.IsKeptApart);
        var cells = new CellValue[Columns.Length];
        Assert.True(store.TryDecode(odd, cells));
        FlatRowCodecTests.AssertSameCells(OddRow(1), cells);

        Assert.True(store.Remove(odd));
        Assert.Equal(0, store.KeptApartCount);
        Assert.False(store.TryDecode(odd, cells));
        Assert.True(store.TryDecode(2, cells));
        AssertIsRow(2, cells);
    }

    [Fact]
    public void Clone_HasTheSameRowsUnderTheSameNumbers_AndThenGoesItsOwnWay()
    {
        FlatRowStore store = NewStore();
        for (int id = 0; id < 100; id++)
        {
            store.Add(id % 7 == 3 ? OddRow(id) : Row(id));
        }

        store.Remove(10); // a packed row
        store.Remove(17); // a row kept apart
        store.AddEmpty();

        FlatRowStore copy = store.Clone();
        store.Remove(20);
        store.Remove(24);
        copy.Remove(30);
        store.Add(Row(101));

        Assert.Equal(101, copy.Count);
        Assert.Equal(store.KeptApartCount + 1, copy.KeptApartCount); // row 24 is still in the copy
        var cells = new CellValue[Columns.Length];
        for (int id = 0; id < 100; id++)
        {
            bool expected = id is not (10 or 17 or 30);
            Assert.Equal(expected, copy.TryDecode(id, cells));
            if (expected)
            {
                FlatRowCodecTests.AssertSameCells(id % 7 == 3 ? OddRow(id) : Row(id), cells);
            }
        }

        Assert.False(copy.IsLive(100));
        Assert.True(store.IsLive(30));
    }

    [Fact]
    public void LayoutWithoutReferences_NeedsNoReferenceArray()
    {
        var store = new FlatRowStore(RowLayout.Create([CellType.Int32, CellType.Double]));
        store.Add([CellValue.From(4), CellValue.Null]);

        Assert.True(store.TryRead(0, out FlatRow row));
        Assert.Empty(row.References);
        var cells = new CellValue[2];
        Assert.True(store.TryDecode(0, cells));
        Assert.Equal(4, cells[0].AsInt32());
        Assert.True(cells[1].IsNull);
        Assert.True(store.Remove(0));
    }

    /// <summary>
    /// One writer adds rows, some of them kept apart, and removes rows behind itself, while readers decode rows
    /// without a lock. A reader must get a row whole or not at all: never a row with a string already let go,
    /// and never another row's values. The store starts with a small first chunk, so the readers also run
    /// across its replacement by larger copies.
    /// </summary>
    [Theory]
    [InlineData(300, 40)]      // many small stores: the first chunk is replaced again and again
    [InlineData(150_000, 1)]   // one large store: many chunks
    public void Readers_SeeWholeRowsOrNothing_WhileAWriterAddsAndRemoves(int rows, int stores)
    {
        for (int round = 0; round < stores; round++)
        {
            FlatRowStore store = NewStore();
            var removed = new bool[rows];
            Exception? failure = null;
            bool done = false;

            var readers = new Thread[2];
            for (int r = 0; r < readers.Length; r++)
            {
                int seed = r + 1;
                readers[r] = new Thread(() =>
                {
                    try
                    {
                        ReadUntilDone(store, seed, ref done);
                    }
                    catch (Exception e)
                    {
                        Volatile.Write(ref failure, e);
                    }
                });
                readers[r].Start();
            }

            for (int id = 0; id < rows; id++)
            {
                store.Add(id % 97 == 11 ? OddRow(id) : Row(id));
                int behind = id - (id % 13);
                if (id % 3 == 0 && !removed[behind])
                {
                    Assert.True(store.Remove(behind));
                    removed[behind] = true;
                }
            }

            Volatile.Write(ref done, true);
            foreach (Thread reader in readers)
            {
                reader.Join();
            }

            Assert.Null(failure);
            AssertFinalState(store, removed);
        }
    }

    private static void ReadUntilDone(FlatRowStore store, int seed, ref bool done)
    {
        var random = new Random(seed);
        var cells = new CellValue[Columns.Length];
        while (!Volatile.Read(ref done))
        {
            int count = store.Count;
            if (count == 0)
            {
                continue;
            }

            // Mostly the newest rows, where adding, removing and chunk replacement are happening.
            int id = random.Next(4) == 0 ? random.Next(count) : Math.Max(0, count - 1 - random.Next(32));
            if (store.TryDecode(id, cells))
            {
                AssertIsRow(id, cells);
                Assert.Equal(id % 5 == 0 && id % 97 != 11, cells[3].IsNull);
            }
        }
    }

    private static void AssertFinalState(FlatRowStore store, bool[] removed)
    {
        var cells = new CellValue[Columns.Length];
        int keptApart = 0;
        for (int id = 0; id < removed.Length; id++)
        {
            Assert.Equal(!removed[id], store.TryDecode(id, cells));
            if (!removed[id])
            {
                AssertIsRow(id, cells);
                keptApart += id % 97 == 11 ? 1 : 0;
            }
        }

        Assert.Equal(keptApart, store.KeptApartCount);
    }
}
