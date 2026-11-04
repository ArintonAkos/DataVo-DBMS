using DataVo.Core.Indexing;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.BTree;

public sealed class IndexManagerGuidFastLaneTests
{
    [Fact]
    public void GuidEqualityComparer_MatchesGuidEqualityAndHashingForDeterministicValues()
    {
        var random = new Random(0x5A17);
        var values = new List<Guid>
        {
            Guid.Empty,
            Guid.Parse("00112233-4455-6677-8899-aabbccddeeff")
        };

        for (int i = 0; i < 32; i++)
        {
            var bytes = new byte[16];
            random.NextBytes(bytes);
            values.Add(new Guid(bytes));
        }

        foreach (Guid left in values)
        {
            Assert.Equal(left.GetHashCode(), GuidSimdEqualityComparer.Instance.GetHashCode(left));
            foreach (Guid right in values)
            {
                Assert.Equal(left.Equals(right), GuidSimdEqualityComparer.Instance.Equals(left, right));
            }
        }
    }

    [Fact]
    public void GuidPrimaryKeyFastLane_LooksUpRowId()
    {
        var manager = CreateInMemoryManager();
        Guid key = Guid.Parse("dd27ec9b-4230-4db9-84a6-7d83d842a9fc");

        manager.InsertGuidPrimaryKeys([(key, 42L)], "_PK_Sessions", "Sessions", "Db");

        Assert.True(manager.HasGuidPrimaryKeyFastLane("_PK_Sessions", "Sessions", "Db"));
        Assert.True(manager.TryLookupGuidPrimaryKey(key, "_PK_Sessions", "Sessions", "Db", out long rowId));
        Assert.Equal(42L, rowId);
    }

    [Fact]
    public void GuidSecondaryFastLane_ReturnsAllRowsForKey()
    {
        var manager = CreateInMemoryManager();
        Guid tenant = Guid.Parse("723af1d7-7402-445c-b260-ceb48fc230db");

        manager.InsertGuidIndexEntries([(tenant, 10L), (tenant, 11L)], "IX_Sessions_Tenant", "Sessions", "Db");

        Assert.True(manager.HasGuidIndexFastLane("IX_Sessions_Tenant", "Sessions", "Db"));
        Assert.True(manager.TryLookupGuidIndex(tenant, "IX_Sessions_Tenant", "Sessions", "Db", out IReadOnlyList<long> rowIds));
        Assert.Equal([10L, 11L], rowIds);
    }

    [Fact]
    public void FilterUsingIndex_UsesGuidFastLaneForCanonicalStringKey()
    {
        var manager = CreateInMemoryManager();
        Guid key = Guid.Parse("6c0be708-6d7e-4f9b-b448-6a24b0f37e81");
        manager.InsertGuidPrimaryKeys([(key, 7L)], "_PK_Sessions", "Sessions", "Db");

        IReadOnlyList<long> rowIds = manager.FilterUsingIndex(key.ToString("D"), "_PK_Sessions", "Sessions", "Db");

        Assert.Equal([7L], rowIds);
    }

    // These tests cover the process-local lanes alone. A persistent manager also mirrors every lane write
    // into the authoritative scalar index, which requires the index to exist first.
    private static IndexManager CreateInMemoryManager() =>
        new(new DataVoConfig { StorageMode = StorageMode.InMemory }, engineStorageRoot: null);
}
