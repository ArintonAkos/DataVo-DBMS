using DataVo.Core;
using DataVo.Core.Exceptions;
using DataVo.Core.MVCC;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser;
using DataVo.Core.Runtime;
using DataVo.Core.Services;
using DataVo.Core.Indexing;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.Enums;
using System.Collections.Concurrent;
using System.Reflection;
using Xunit;

namespace DataVo.Tests.AuditFixes;

/// <summary>
/// Tests covering the Phase 1 audit fixes: domain exceptions,
/// TransactionIdAllocator lock-free upgrade, and VersionStorageManager IDisposable.
/// </summary>
public class AuditFixTests
{
    // ── Domain Exceptions ──────────────────────────────────────────────

    [Fact]
    public void DataVoException_IsBaseOfStorageException()
    {
        var ex = new StorageException("disk error");
        Assert.IsAssignableFrom<DataVoException>(ex);
    }

    [Fact]
    public void DataVoException_IsBaseOfCatalogException()
    {
        var ex = new CatalogException("table not found");
        Assert.IsAssignableFrom<DataVoException>(ex);
    }

    [Fact]
    public void DataVoException_IsBaseOfBindingException()
    {
        var ex = new BindingException("binding failed");
        Assert.IsAssignableFrom<DataVoException>(ex);
    }

    [Fact]
    public void DataVoException_IsBaseOfIndexException()
    {
        var ex = new IndexException("index failed");
        Assert.IsAssignableFrom<DataVoException>(ex);
    }

    [Fact]
    public void RowDeletedException_IsStorageException()
    {
        var ex = new RowDeletedException(42, "Users");
        Assert.IsAssignableFrom<StorageException>(ex);
        Assert.Equal(42, ex.RowId);
        Assert.Equal("Users", ex.TableName);
    }

    [Fact]
    public void RowDeletedException_IncludesInnerException_WhenWrapping()
    {
        var inner = new IOException("disk failed");
        var ex = new StorageException("wrapped error", inner);
        Assert.Same(inner, ex.InnerException);
    }

    // ── TransactionIdAllocator (lock-free) ─────────────────────────────

    [Fact]
    public void AllocateTransactionId_ReturnsStrictlyIncreasingIds()
    {
        var allocator = new TransactionIdAllocator();
        long first = allocator.AllocateTransactionId();
        long second = allocator.AllocateTransactionId();
        long third = allocator.AllocateTransactionId();

        Assert.Equal(1, first);
        Assert.Equal(2, second);
        Assert.Equal(3, third);
    }

    [Fact]
    public void AllocateRange_ReturnsConsecutiveSpan()
    {
        var allocator = new TransactionIdAllocator();
        allocator.AllocateTransactionId(); // consume ID 1

        var (start, end) = allocator.AllocateRange(5);
        Assert.Equal(2, start);
        Assert.Equal(6, end);

        // Next single allocation should be 7
        Assert.Equal(7, allocator.AllocateTransactionId());
    }

    [Fact]
    public void GetCurrentHighWaterMark_ReflectsAllocations()
    {
        var allocator = new TransactionIdAllocator();
        Assert.Equal(0, allocator.GetCurrentHighWaterMark());

        allocator.AllocateTransactionId();
        Assert.Equal(1, allocator.GetCurrentHighWaterMark());
    }

    [Fact]
    public void RestoreHighWaterMark_AdvancesCounter()
    {
        var allocator = new TransactionIdAllocator();
        allocator.RestoreHighWaterMark(1000);

        Assert.Equal(999, allocator.GetCurrentHighWaterMark());
        Assert.Equal(1000, allocator.AllocateTransactionId());
    }

    [Fact]
    public void RestoreHighWaterMark_DoesNotRewind()
    {
        var allocator = new TransactionIdAllocator();
        allocator.AllocateRange(100); // advance to 101

        allocator.RestoreHighWaterMark(50); // should be a no-op
        Assert.Equal(100, allocator.GetCurrentHighWaterMark());
    }

    [Fact]
    public async Task AllocateTransactionId_IsThreadSafe()
    {
        var allocator = new TransactionIdAllocator();
        const int threadCount = 10;
        const int idsPerThread = 1000;
        var ids = new long[threadCount * idsPerThread];

        var tasks = Enumerable.Range(0, threadCount).Select(t =>
            Task.Run(() =>
            {
                for (int i = 0; i < idsPerThread; i++)
                {
                    ids[t * idsPerThread + i] = allocator.AllocateTransactionId();
                }
            })).ToArray();

        await Task.WhenAll(tasks);

        // All IDs should be unique
        Assert.Equal(threadCount * idsPerThread, ids.Distinct().Count());
    }

    // ── VersionStorageManager IDisposable ───────────────────────────────

    [Fact]
    public void VersionStorageManager_ImplementsIDisposable()
    {
        var manager = new VersionStorageManager();
        Assert.IsAssignableFrom<IDisposable>(manager);
    }

    [Fact]
    public void VersionStorageManager_CanBeDisposedInUsingBlock()
    {
        using var manager = new VersionStorageManager();
        manager.AllocateVersion("db", "tbl", 1, xmin: 1);
        // No assertion needed — test passes if Dispose() doesn't throw
    }

    [Fact]
    public void Parser_UnexpectedToken_ThrowsParserException()
    {
        var parser = new Parser([
            new Token(TokenType.Identifier, "BOGUS", position: 0, line: 1, column: 1),
            new Token(TokenType.EOF, string.Empty, position: 5, line: 1, column: 6)
        ]);

        ParserException ex = Assert.Throws<ParserException>(() => parser.Parse());
        Assert.Contains("Unexpected token", ex.Message);
    }

    [Fact]
    public void TableService_MissingAlias_ThrowsBindingException()
    {
        var tableService = new TableService("db");

        BindingException ex = Assert.Throws<BindingException>(() =>
            tableService.GetTableDetailByAliasOrName("missing"));

        Assert.Contains("not found", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void EngineCatalog_DropMissingDatabase_ThrowsCatalogException()
    {
        var catalog = new EngineCatalog(new DataVoConfig { StorageMode = StorageMode.InMemory });

        CatalogException ex = Assert.Throws<CatalogException>(() =>
            catalog.DropDatabase("missing_db"));

        Assert.Contains("does not exist", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void EngineCatalog_DatabaseAndTableLookups_AreCaseInsensitive()
    {
        var catalog = new EngineCatalog(new DataVoConfig { StorageMode = StorageMode.InMemory });
        var database = new Database { DatabaseName = "CaseDb", Tables = [] };
        catalog.CreateDatabase(database);

        var table = new Table
        {
            TableName = "Users",
            Fields = [],
            PrimaryKeys = [],
            ForeignKeys = [],
            UniqueAttributes = [],
            IndexFiles = []
        };

        catalog.CreateTable(table, "CaseDb");

        Assert.True(catalog.DatabaseExists("casedb"));
        Assert.True(catalog.TableExists("users", "CASEDB"));
    }

    [Fact]
    public void IndexManager_MissingIndexLookup_ThrowsIndexException()
    {
        string root = Path.Combine(Path.GetTempPath(), $"datavo_index_tests_{Guid.NewGuid():N}");
        Directory.CreateDirectory(root);

        try
        {
            var manager = new IndexManager();
            IndexException ex = Assert.Throws<IndexException>(() =>
                manager.FilterUsingIndex("Alice", "idx_missing", "Users", "Db"));

            Assert.Contains("does not exist", ex.Message, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            if (Directory.Exists(root))
            {
                Directory.Delete(root, recursive: true);
            }
        }
    }

    [Fact]
    public void StorageContext_SerializerBinding_RemainsEngineScoped_WhenFallbackEngineChanges()
    {
        DataVoEngine engineA = DataVoEngine.Initialize(new DataVoConfig { StorageMode = StorageMode.InMemory });
        DataVoEngine engineB = DataVoEngine.Initialize(new DataVoConfig { StorageMode = StorageMode.InMemory });

        try
        {
            var dbA = new Database { DatabaseName = "ScopedDb", Tables = [] };
            engineA.Catalog.CreateDatabase(dbA);
            engineA.Catalog.CreateTable(new Table
            {
                TableName = "ScopedTable",
                Fields =
                [
                    new Field { Name = "Value", Table = "ScopedTable", Type = DataTypes.Int }
                ],
                PrimaryKeys = [],
                ForeignKeys = [],
                UniqueAttributes = [],
                IndexFiles = []
            }, "ScopedDb");

            var dbB = new Database { DatabaseName = "ScopedDb", Tables = [] };
            engineB.Catalog.CreateDatabase(dbB);
            engineB.Catalog.CreateTable(new Table
            {
                TableName = "ScopedTable",
                Fields =
                [
                    new Field { Name = "Value", Table = "ScopedTable", Type = DataTypes.Bit }
                ],
                PrimaryKeys = [],
                ForeignKeys = [],
                UniqueAttributes = [],
                IndexFiles = []
            }, "ScopedDb");

            engineA.StorageContext.InsertOneIntoTable(new Dictionary<string, object?> { ["Value"] = 123 }, "ScopedTable", "ScopedDb");
            engineA.StorageContext.InsertOneIntoTable(new Dictionary<string, object?> { ["Value"] = 456 }, "ScopedTable", "ScopedDb");

            Dictionary<RowKey, Dictionary<string, object?>> rows = engineA.StorageContext.GetTableContents("ScopedTable", "ScopedDb");
            List<int> values = rows.Values.Select(r => (int)r["Value"]).OrderBy(v => v).ToList();

            Assert.Equal([123, 456], values);
        }
        finally
        {
            engineA.Dispose();
            engineB.Dispose();
        }
    }

    [Fact]
    public void StorageContext_Initialize_DisposesPreviousFallbackEngine()
    {
        string storagePath = Path.Combine(Path.GetTempPath(), $"datavo_engine_reset_{Guid.NewGuid():N}");
        Directory.CreateDirectory(storagePath);

        var diskConfig = new DataVoConfig
        {
            StorageMode = StorageMode.Lsm,
            DiskStoragePath = storagePath,
            TransactionIdStateFilePath = "tx-state.dat"
        };

        string txStatePath = diskConfig.ResolveTransactionIdStateFilePath();

        try
        {
            DataVoEngine engine = DataVoEngine.Initialize(diskConfig);
            engine.TransactionIdAllocator.AllocateTransactionId();
            engine.TransactionIdAllocator.AllocateTransactionId();

            StorageContext.Initialize(new DataVoConfig { StorageMode = StorageMode.InMemory });

            Assert.True(File.Exists(txStatePath));
            string persisted = File.ReadAllText(txStatePath).Trim();
            Assert.Equal("2", persisted);
        }
        finally
        {
            // Restore a fresh fallback engine for subsequent tests that rely on global defaults.
            DataVoEngine.Initialize(new DataVoConfig { StorageMode = StorageMode.InMemory });

            if (Directory.Exists(storagePath))
            {
                Directory.Delete(storagePath, recursive: true);
            }
        }
    }

    [Fact]
    public void StorageContext_CompactTable_RejectsIndexedTableWithoutExplicitRebuildOptIn()
    {
        DataVoEngine engine = DataVoEngine.Initialize(new DataVoConfig { StorageMode = StorageMode.InMemory });

        try
        {
            engine.Catalog.CreateDatabase(new Database { DatabaseName = "CompactGuardDb", Tables = [] });
            engine.Catalog.CreateTable(new Table
            {
                TableName = "Users",
                Fields =
                [
                    new Field { Name = "Id", Table = "Users", Type = DataTypes.Int }
                ],
                PrimaryKeys = [],
                ForeignKeys = [],
                UniqueAttributes = [],
                IndexFiles =
                [
                    new IndexFile { IndexFileName = "idx_users_id", AttributeNames = ["Id"] }
                ]
            }, "CompactGuardDb");

            InvalidOperationException ex = Assert.Throws<InvalidOperationException>(() =>
                engine.StorageContext.CompactTable("Users", "CompactGuardDb"));

            Assert.Contains("requires index rebuild", ex.Message, StringComparison.OrdinalIgnoreCase);

            var compacted = engine.StorageContext.CompactTable("Users", "CompactGuardDb", allowIndexedCompaction: true);
            Assert.Empty(compacted);
        }
        finally
        {
            engine.Dispose();
        }
    }

}
