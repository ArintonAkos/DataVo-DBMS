using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

/// <summary>
/// Directories written before the shared commit log kept a WAL per table. Rows that only reached that
/// WAL (never flushed) must survive the first open with the commit-log engine.
/// </summary>
public sealed class LsmLegacyWalMigrationTests
{
    [Fact]
    public void UnflushedRowsInLegacyTableWal_AreMigrated_OnFirstOpen()
    {
        string root = TestStorageDirectories.Create("lsm_legacy_wal");
        string tableDirectory = Path.Combine(root, "db", "users");
        Directory.CreateDirectory(tableDirectory);

        // Simulate the old engine: a WAL-backed table whose rows were acknowledged but never flushed.
        var manifest = new LsmManifest(Path.Combine(tableDirectory, "MANIFEST"));
        var legacy = new LsmTable(tableDirectory, manifest, Path.Combine(tableDirectory, "active.wal"), LsmWalDurabilityMode.StrictFsync);
        Span<byte> key = stackalloc byte[sizeof(long)];
        for (long rowId = 1; rowId <= 3; rowId++)
        {
            InternalKey.EncodeInt64UserKey(key, rowId);
            legacy.Put(key, (ulong)rowId, [(byte)rowId, 42]);
        }

        legacy.Dispose(); // no flush: the rows exist only in active.wal
        Assert.NotEmpty(Directory.EnumerateFiles(tableDirectory, "active*.wal"));

        using (var engine = new LsmStorageEngine(root))
        {
            Assert.Equal([2, 42], engine.ReadRow("db", "users", 2));
            Assert.Equal(3, engine.ReadAllRows("db", "users").Count());
            Assert.Empty(Directory.EnumerateFiles(tableDirectory, "active*.wal"));

            // New writes continue above every migrated sequence number and row id.
            Assert.Equal(4, engine.InsertRow("db", "users", [4, 42]));
        }

        using var reopened = new LsmStorageEngine(root);
        Assert.Equal(4, reopened.ReadAllRows("db", "users").Count());
        Assert.Equal([4, 42], reopened.ReadRow("db", "users", 4));
    }
}
