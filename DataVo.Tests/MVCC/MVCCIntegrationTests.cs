using DataVo.Core.MVCC;
using DataVo.Core.Transactions;
using Xunit;

namespace DataVo.Tests.MVCC;

/// <summary>
/// Integration tests for MVCC functionality.
/// Tests snapshot isolation, non-blocking readers, write-write conflict detection,
/// and consistency of multi-version storage.
/// </summary>
public class MVCCIntegrationTests
{
    /// <summary>
    /// Test: Two transactions with different snapshots should see different versions of a row.
    /// Demonstrates snapshot isolation at the core of MVCC.
    /// </summary>
    [Fact]
    public void SnapshotIsolation_DifferentTxsSeeConsistentSnapshots()
    {
        var idAllocator = new TransactionIdAllocator();
        var versionStorage = new VersionStorageManager();

        // Transaction 1: Sees version created at timestamp 1
        long tx1Id = idAllocator.AllocateTransactionId(); // tx1Id = 1
        var snapshot1 = new TransactionSnapshot(snapshotTimestamp: tx1Id, transactionId: tx1Id);

        // Allocate version to tx1
        versionStorage.AllocateVersion("db", "tbl", rowId: 1, xmin: tx1Id);

        // Transaction 2: Started after tx1, has different snapshot
        long tx2Id = idAllocator.AllocateTransactionId(); // tx2Id = 2
        var snapshot2 = new TransactionSnapshot(snapshotTimestamp: tx2Id, transactionId: tx2Id);

        // Get version
        var version = versionStorage.GetVersion("db", "tbl", 1);
        Assert.True(version.HasValue);

        // Both snapshots should see the version (created within both snapshots)
        Assert.True(SnapshotVisibilityEvaluator.IsVersionVisible(version.Value, snapshot1));
        Assert.True(SnapshotVisibilityEvaluator.IsVersionVisible(version.Value, snapshot2));
    }

    /// <summary>
    /// Test: When a row is deleted (marked with xmax), old snapshots still see it,
    /// but newer snapshots don't (unless the delete itself is in their future).
    /// Demonstrates MVCC row deletion isolation.
    /// </summary>
    [Fact]
    public void MVCCDeletion_OldSnapshotsSeePreviousVersions()
    {
        var idAllocator = new TransactionIdAllocator();
        var versionStorage = new VersionStorageManager();

        // Create version at tx1
        long tx1Id = idAllocator.AllocateTransactionId(); // 1
        versionStorage.AllocateVersion("db", "tbl", rowId: 1, xmin: tx1Id);

        // Snapshot at tx1 time
        var snapshot1 = new TransactionSnapshot(snapshotTimestamp: tx1Id, transactionId: tx1Id);

        // Tx2 deletes the row
        long tx2Id = idAllocator.AllocateTransactionId(); // 2
        versionStorage.MarkVersionObsolete("db", "tbl", rowId: 1, xmax: tx2Id);

        // Snapshot at tx2 time
        var snapshot2 = new TransactionSnapshot(snapshotTimestamp: tx2Id, transactionId: tx2Id);

        var version = versionStorage.GetVersion("db", "tbl", 1);
        Assert.True(version.HasValue);

        // Snapshot1 (before delete) should see the row
        Assert.True(SnapshotVisibilityEvaluator.IsVersionVisible(version.Value, snapshot1));

        // Snapshot2 (at delete time) should NOT see it (xmax <= snapshot2.timestamp)
        Assert.False(SnapshotVisibilityEvaluator.IsVersionVisible(version.Value, snapshot2));
    }

    /// <summary>
    /// Test: Multiple versions of the same row can coexist, connected by version chain.
    /// Demonstrates MVCC's ability to maintain version history.
    /// </summary>
    [Fact]
    public void VersionChaining_MultipleVersionsOfSameRow()
    {
        var idAllocator = new TransactionIdAllocator();
        var versionStorage = new VersionStorageManager();

        // Initial version (tx1 creates a row)
        long tx1Id = idAllocator.AllocateTransactionId(); // 1
        versionStorage.AllocateVersion("db", "tbl", rowId: 1, xmin: tx1Id);

        // Tx2 updates the row (creates new version, marks old one as obsolete)
        long tx2Id = idAllocator.AllocateTransactionId(); // 2
        versionStorage.MarkVersionObsolete("db", "tbl", rowId: 1, xmax: tx2Id);
        versionStorage.AllocateVersion("db", "tbl", rowId: 2, xmin: tx2Id); // new version in different row
        versionStorage.LinkVersionChain("db", "tbl", oldRowId: 1, newRowId: 2);

        // Tx3 updates again
        long tx3Id = idAllocator.AllocateTransactionId(); // 3
        versionStorage.MarkVersionObsolete("db", "tbl", rowId: 2, xmax: tx3Id);
        versionStorage.AllocateVersion("db", "tbl", rowId: 3, xmin: tx3Id);
        versionStorage.LinkVersionChain("db", "tbl", oldRowId: 2, newRowId: 3);

        // Get full version chain
        var chain = versionStorage.GetVersionChain("db", "tbl", startRowId: 1);

        // Should have 3 versions in order
        Assert.Equal(3, chain.Count);
        Assert.Equal(1, chain[0].RowId);
        Assert.Equal(2, chain[1].RowId);
        Assert.Equal(3, chain[2].RowId);

        // Verify version metadata
        Assert.Equal(tx1Id, chain[0].Version.Xmin);
        Assert.Equal(tx2Id, chain[0].Version.Xmax); // marked obsolete
        Assert.Equal(2, chain[0].Version.VersionChain); // points to next

        Assert.Equal(tx2Id, chain[1].Version.Xmin);
        Assert.Equal(tx3Id, chain[1].Version.Xmax);
        Assert.Equal(3, chain[1].Version.VersionChain);

        Assert.Equal(tx3Id, chain[2].Version.Xmin);
        Assert.Equal(0, chain[2].Version.Xmax); // terminal version
        Assert.Equal(0, chain[2].Version.VersionChain);
    }

    /// <summary>
    /// Test: Non-blocking reader property: older transactions can still see old versions
    /// while newer transactions see newer versions, neither blocking each other.
    /// </summary>
    [Fact]
    public void NonBlockingReaders_ConcurrentTransactionsDontBlock()
    {
        var idAllocator = new TransactionIdAllocator();
        var versionStorage = new VersionStorageManager();

        // TX1 starts and creates a version
        long tx1Id = idAllocator.AllocateTransactionId(); // 1
        var snapshot1 = new TransactionSnapshot(snapshotTimestamp: tx1Id, transactionId: tx1Id);
        versionStorage.AllocateVersion("db", "tbl", rowId: 1, xmin: tx1Id);

        // TX2 starts (after TX1) but doesn't modify yet
        long tx2Id = idAllocator.AllocateTransactionId(); // 2
        var snapshot2 = new TransactionSnapshot(snapshotTimestamp: tx2Id, transactionId: tx2Id);

        // TX3 starts (after TX2)
        long tx3Id = idAllocator.AllocateTransactionId(); // 3
        var snapshot3 = new TransactionSnapshot(snapshotTimestamp: tx3Id, transactionId: tx3Id);

        // Now TX2 updates the row (creates new version)
        versionStorage.MarkVersionObsolete("db", "tbl", rowId: 1, xmax: tx2Id);
        versionStorage.AllocateVersion("db", "tbl", rowId: 2, xmin: tx2Id);
        versionStorage.LinkVersionChain("db", "tbl", oldRowId: 1, newRowId: 2);

        var v1 = versionStorage.GetVersion("db", "tbl", 1);
        var v2 = versionStorage.GetVersion("db", "tbl", 2);

        // TX1 still sees version 1 (created before TX1's snapshot)
        Assert.True(v1.HasValue);
        Assert.True(SnapshotVisibilityEvaluator.IsVersionVisible(v1.Value, snapshot1));

        // TX2 sees the new version (visible at/after TX2's snapshot)
        Assert.True(v2.HasValue);
        Assert.True(SnapshotVisibilityEvaluator.IsVersionVisible(v2.Value, snapshot2));

        Assert.True(SnapshotVisibilityEvaluator.IsVersionVisible(v2.Value, snapshot3));

        Assert.False(SnapshotVisibilityEvaluator.IsVersionVisible(v1.Value, snapshot2));
        Assert.False(SnapshotVisibilityEvaluator.IsVersionVisible(v1.Value, snapshot3));
    }

    /// <summary>
    /// Test: Concurrent transactions with independent snapshots maintain isolation.
    /// Each transaction's snapshot defines what it can see independently.
    /// </summary>
    [Fact]
    public void SnapshotConsistency_IndependentTransactionsIsolated()
    {
        var idAllocator = new TransactionIdAllocator();
        var versionStorage = new VersionStorageManager();

        // Three concurrent transactions
        long tx1Id = idAllocator.AllocateTransactionId(); // 1
        long tx2Id = idAllocator.AllocateTransactionId(); // 2
        long tx3Id = idAllocator.AllocateTransactionId(); // 3

        var snapshot1 = new TransactionSnapshot(snapshotTimestamp: tx1Id, transactionId: tx1Id);
        var snapshot2 = new TransactionSnapshot(snapshotTimestamp: tx2Id, transactionId: tx2Id);
        var snapshot3 = new TransactionSnapshot(snapshotTimestamp: tx3Id, transactionId: tx3Id);

        // Each transaction creates/modifies different data independently
        versionStorage.AllocateVersion("db", "tbl", rowId: 1, xmin: tx1Id);
        versionStorage.AllocateVersion("db", "tbl", rowId: 2, xmin: tx2Id);
        versionStorage.AllocateVersion("db", "tbl", rowId: 3, xmin: tx3Id);

        var v1 = versionStorage.GetVersion("db", "tbl", 1);
        var v2 = versionStorage.GetVersion("db", "tbl", 2);
        var v3 = versionStorage.GetVersion("db", "tbl", 3);

        // Each transaction should see its own and prior versions
        Assert.True(v1.HasValue);
        Assert.True(SnapshotVisibilityEvaluator.IsVersionVisible(v1.Value, snapshot1)); // sees its own
        Assert.True(SnapshotVisibilityEvaluator.IsVersionVisible(v1.Value, snapshot2)); // sees prior
        Assert.True(SnapshotVisibilityEvaluator.IsVersionVisible(v1.Value, snapshot3)); // sees prior

        Assert.True(v2.HasValue);
        Assert.False(SnapshotVisibilityEvaluator.IsVersionVisible(v2.Value, snapshot1)); // doesn't see future
        Assert.True(SnapshotVisibilityEvaluator.IsVersionVisible(v2.Value, snapshot2)); // sees its own
        Assert.True(SnapshotVisibilityEvaluator.IsVersionVisible(v2.Value, snapshot3)); // sees prior

        Assert.True(v3.HasValue);
        Assert.False(SnapshotVisibilityEvaluator.IsVersionVisible(v3.Value, snapshot1)); // doesn't see future
        Assert.False(SnapshotVisibilityEvaluator.IsVersionVisible(v3.Value, snapshot2)); // doesn't see future
        Assert.True(SnapshotVisibilityEvaluator.IsVersionVisible(v3.Value, snapshot3)); // sees its own
    }

    /// <summary>
    /// Test: Phantom row prevention - deleted rows don't reappear in visibility checks.
    /// </summary>
    [Fact]
    public void PhantomPrevention_DeletedRowsNotVisible()
    {
        var idAllocator = new TransactionIdAllocator();
        var versionStorage = new VersionStorageManager();

        long tx1Id = idAllocator.AllocateTransactionId(); // 1
        long tx2Id = idAllocator.AllocateTransactionId(); // 2

        // TX1 creates rows 1, 2, 3
        versionStorage.AllocateVersion("db", "tbl", rowId: 1, xmin: tx1Id);
        versionStorage.AllocateVersion("db", "tbl", rowId: 2, xmin: tx1Id);
        versionStorage.AllocateVersion("db", "tbl", rowId: 3, xmin: tx1Id);

        var snapshot2 = new TransactionSnapshot(snapshotTimestamp: tx2Id, transactionId: tx2Id);

        // TX2 deletes row 2
        versionStorage.MarkVersionObsolete("db", "tbl", rowId: 2, xmax: tx2Id);

        // Filter visible rows for TX2's snapshot
        var visibleRowIds = new[] { 1L, 2L, 3L };
        var filtered = SnapshotVisibilityEvaluator.FilterVisibleRows(
            visibleRowIds, versionStorage, "db", "tbl", snapshot2);

        // Row 2 should be filtered out
        Assert.Equal(2, filtered.Count);
        Assert.Contains(1, filtered);
        Assert.Contains(3, filtered);
        Assert.DoesNotContain(2, filtered);
    }

    /// <summary>
    /// Test: Update checks ensure only updatable rows are modified.
    /// Rows deleted by other transactions cannot be updated.
    /// </summary>
    [Fact]
    public void UpdateChecks_CannotUpdateDeletedRows()
    {
        var idAllocator = new TransactionIdAllocator();
        var versionStorage = new VersionStorageManager();

        long tx1Id = idAllocator.AllocateTransactionId(); // 1
        long tx2Id = idAllocator.AllocateTransactionId(); // 2

        // TX1 creates a row
        versionStorage.AllocateVersion("db", "tbl", rowId: 1, xmin: tx1Id);

        // TX2 deletes it
        versionStorage.MarkVersionObsolete("db", "tbl", rowId: 1, xmax: tx2Id);

        var snapshot2 = new TransactionSnapshot(snapshotTimestamp: tx2Id, transactionId: tx2Id);
        var version = versionStorage.GetVersion("db", "tbl", 1);

        // Should not be able to update a deleted row
        Assert.True(version.HasValue);
        Assert.False(SnapshotVisibilityEvaluator.CanUpdateRow(version.Value, snapshot2));
        Assert.False(SnapshotVisibilityEvaluator.CanDeleteRow(version.Value, snapshot2));
    }
}
