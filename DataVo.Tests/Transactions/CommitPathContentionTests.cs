using System.Globalization;
using DataVo.Core;
using DataVo.Core.MVCC;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Transactions;

namespace DataVo.Tests.Transactions;

/// <summary>
/// The two checks every statement and every commit run: whether a transaction is open (a lock-free counter, not
/// <c>ConcurrentDictionary.IsEmpty</c>, which locks every bucket when the dictionary is empty), and the durable
/// transaction-id reservation (refilled in the background, never letting an issued id pass the durable bound).
/// </summary>
public sealed class CommitPathContentionTests
{
    [Fact]
    public void OpenTransactionCount_FollowsBeginCommitRollbackAndRejectedBegins()
    {
        var manager = new TransactionManager();
        var ids = new TransactionIdAllocator();
        Guid first = Guid.NewGuid();
        Guid second = Guid.NewGuid();

        Assert.False(manager.HasAnyActiveTransaction());
        Assert.False(manager.HasAnyOtherActiveTransaction(0));
        Assert.Null(manager.OldestActiveSnapshot());

        manager.Begin(first, ids);
        Assert.True(manager.HasAnyActiveTransaction());
        long firstId = manager.GetContext(first)!.TransactionId;
        Assert.False(manager.HasAnyOtherActiveTransaction(firstId));
        Assert.True(manager.HasAnyOtherActiveTransaction(0));

        Assert.Throws<InvalidOperationException>(() => manager.Begin(first, ids)); // rejected: count unchanged
        manager.Begin(second, ids);
        Assert.Equal(firstId, manager.OldestActiveSnapshot());

        manager.Commit(first);
        Assert.True(manager.HasAnyActiveTransaction());
        manager.Rollback(second);
        Assert.False(manager.HasAnyActiveTransaction());
        Assert.False(manager.HasAnyOtherActiveTransaction(0));
        Assert.Throws<InvalidOperationException>(() => manager.Rollback(second)); // nothing to remove: count stays 0
        Assert.False(manager.HasAnyActiveTransaction());
    }

    [Fact]
    public void OpenTransactionCheck_NeverMissesATransactionThatIsOpen_UnderConcurrentBeginAndEnd()
    {
        var manager = new TransactionManager();
        var ids = new TransactionIdAllocator();
        Guid held = Guid.NewGuid();
        manager.Begin(held, ids);
        int stop = 0;
        var churn = new Thread(() =>
        {
            Guid session = Guid.NewGuid();
            while (Volatile.Read(ref stop) == 0)
            {
                manager.Begin(session, ids);
                manager.Commit(session);
            }
        }) { IsBackground = true };
        churn.Start();
        try
        {
            for (int i = 0; i < 200_000; i++)
            {
                Assert.True(manager.HasAnyActiveTransaction());
            }
        }
        finally
        {
            Volatile.Write(ref stop, 1);
            churn.Join();
        }

        manager.Commit(held);
        Assert.False(manager.HasAnyActiveTransaction());
    }

    [Fact]
    public void IssuedTransactionIds_NeverPassTheDurableReservation_AcrossBlockRefills_AndReopen()
    {
        string directory = TestStorageDirectories.Create("txid_reservation");
        try
        {
            var config = new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = directory, LsmStrictFsync = false };
            string statePath = config.ResolveTransactionIdStateFilePath();
            long highest = 0;
            using (DataVoEngine engine = DataVoEngine.Initialize(config))
            {
                // Two and a half reservation blocks: crosses the background refill point several times.
                for (int i = 0; i < 160_000; i++)
                {
                    highest = engine.TransactionIdAllocator.AllocateTransactionId();
                    if (i % 4_000 == 0)
                    {
                        Assert.True(ReadState(statePath) >= highest, $"id {highest} was issued before a bound covering it was durable");
                    }
                }
            }

            Assert.True(ReadState(statePath) >= highest);
            using DataVoEngine reopened = DataVoEngine.Initialize(config);
            Assert.True(reopened.TransactionIdAllocator.AllocateTransactionId() > highest);
        }
        finally
        {
            try
            {
                Directory.Delete(directory, recursive: true);
            }
            catch (IOException)
            {
            }
        }
    }

    private static long ReadState(string path) =>
        long.Parse(File.ReadAllText(path).Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture);
}
