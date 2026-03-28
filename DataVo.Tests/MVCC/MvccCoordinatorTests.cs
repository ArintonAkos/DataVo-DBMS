using DataVo.Core.MVCC;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine.Config;
using Xunit;

namespace DataVo.Tests.MVCC;

public class MvccCoordinatorTests
{
    [Fact]
    public void ResolveStatementTransactionId_UsesExplicitTransactionIdWhenPresent()
    {
        var engine = DataVoEngine.Initialize(new DataVoConfig { StorageMode = StorageMode.InMemory });
        try
        {
            long txId = MvccCoordinator.ResolveStatementTransactionId(engine, 42);
            Assert.Equal(42, txId);
        }
        finally
        {
            engine.Dispose();
        }
    }

    [Fact]
    public void ResolveStatementTransactionId_AutoAllocatesWhenNoExplicitId()
    {
        var engine = DataVoEngine.Initialize(new DataVoConfig { StorageMode = StorageMode.InMemory });
        try
        {
            long first = MvccCoordinator.ResolveStatementTransactionId(engine, null);
            long second = MvccCoordinator.ResolveStatementTransactionId(engine, null);

            Assert.True(first > 0);
            Assert.Equal(first + 1, second);
        }
        finally
        {
            engine.Dispose();
        }
    }

    [Fact]
    public void ValidateCanModifyRow_ThrowsWhenRowAlreadyObsoleted()
    {
        var engine = DataVoEngine.Initialize(new DataVoConfig { StorageMode = StorageMode.InMemory });
        try
        {
            engine.VersionStorageManager.AllocateVersion("db", "tbl", 1, xmin: 1);
            engine.VersionStorageManager.MarkVersionObsolete("db", "tbl", 1, xmax: 2);

            var ex = Assert.Throws<InvalidOperationException>(() =>
                MvccCoordinator.ValidateCanModifyRow(engine, "db", "tbl", 1, null, "UPDATE"));

            Assert.Contains("MVCC conflict", ex.Message);
        }
        finally
        {
            engine.Dispose();
        }
    }

    [Fact]
    public void RegisterUpdateVersion_CreatesLinkedVersionChain()
    {
        var engine = DataVoEngine.Initialize(new DataVoConfig { StorageMode = StorageMode.InMemory });
        try
        {
            engine.VersionStorageManager.AllocateVersion("db", "tbl", 10, xmin: 5);

            MvccCoordinator.RegisterUpdateVersion(engine, "db", "tbl", oldRowId: 10, newRowId: 11, transactionId: 6);

            var oldVersion = engine.VersionStorageManager.GetVersion("db", "tbl", 10);
            var newVersion = engine.VersionStorageManager.GetVersion("db", "tbl", 11);

            Assert.True(oldVersion.HasValue);
            Assert.True(newVersion.HasValue);
            Assert.Equal(6, oldVersion.Value.Xmax);
            Assert.Equal(11, oldVersion.Value.VersionChain);
            Assert.Equal(6, newVersion.Value.Xmin);
        }
        finally
        {
            engine.Dispose();
        }
    }
}
