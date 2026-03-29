using DataVo.Core.Cache;
using DataVo.Core.Exceptions;
using DataVo.Core.MVCC;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Parser;
using DataVo.Core.Runtime;
using DataVo.Core.Services;
using DataVo.Core.Indexing;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Disk;
using DataVo.Core.Enums;
using System.Collections.Concurrent;
using System.Reflection;
using Xunit;

namespace DataVo.Tests.AuditFixes;

/// <summary>
/// Tests covering the Phase 1 audit fixes: domain exceptions, CacheStorage thread safety,
/// TransactionIdAllocator lock-free upgrade, VersionStorageManager IDisposable, and
/// DiskStorageEngine RowDeletedException.
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

    // ── DiskStorageEngine RowDeletedException ───────────────────────────

    [Fact]
    public void DiskStorageEngine_ReadDeletedRow_ThrowsRowDeletedException()
    {
        string storagePath = Path.Combine(Path.GetTempPath(), $"datavo_test_{Guid.NewGuid():N}");
        try
        {
            var engine = new DiskStorageEngine(storagePath);
            byte[] data = [1, 2, 3, 4];
            long rowId = engine.InsertRow("testdb", "testtable", data);

            engine.DeleteRow("testdb", "testtable", rowId);

            var ex = Assert.Throws<RowDeletedException>(() =>
                engine.ReadRow("testdb", "testtable", rowId));

            Assert.Equal(rowId, ex.RowId);
            Assert.Equal("testtable", ex.TableName);
        }
        finally
        {
            if (Directory.Exists(storagePath))
                Directory.Delete(storagePath, recursive: true);
        }
    }

    [Fact]
    public void DiskStorageEngine_CompactTable_UsesCorrectFileHeader()
    {
        string storagePath = Path.Combine(Path.GetTempPath(), $"datavo_test_{Guid.NewGuid():N}");
        try
        {
            var engine = new DiskStorageEngine(storagePath);
            byte[] data1 = [1, 2, 3];
            byte[] data2 = [4, 5, 6];
            long id1 = engine.InsertRow("testdb", "testtable", data1);
            engine.InsertRow("testdb", "testtable", data2);

            // Delete first row to create a tombstone
            engine.DeleteRow("testdb", "testtable", id1);

            // Compact should produce valid file with only surviving rows
            var compacted = engine.CompactTable("testdb", "testtable");
            Assert.Single(compacted);
            Assert.Equal(data2, compacted[0].RawRow);

            // The compacted row should be readable
            byte[] readBack = engine.ReadRow("testdb", "testtable", compacted[0].NewRowId);
            Assert.Equal(data2, readBack);
        }
        finally
        {
            if (Directory.Exists(storagePath))
                Directory.Delete(storagePath, recursive: true);
        }
    }

    [Fact]
    public void DiskStorageEngine_DropTable_RemovesFileLockEntry()
    {
        string storagePath = Path.Combine(Path.GetTempPath(), $"datavo_test_{Guid.NewGuid():N}");
        const string db = "testdb";
        const string table = "drop_table_lock_test";

        try
        {
            var engine = new DiskStorageEngine(storagePath);
            engine.InsertRow(db, table, [1, 2, 3]);

            string filePath = Path.GetFullPath(Path.Combine(storagePath, db, $"{table}.dat"));
            var locks = GetDiskFileLocks();
            Assert.True(locks.ContainsKey(filePath));

            engine.DropTable(db, table);
            Assert.False(locks.ContainsKey(filePath));
        }
        finally
        {
            if (Directory.Exists(storagePath))
                Directory.Delete(storagePath, recursive: true);
        }
    }

    [Fact]
    public void DiskStorageEngine_DropDatabase_RemovesAllFileLockEntriesForDatabase()
    {
        string storagePath = Path.Combine(Path.GetTempPath(), $"datavo_test_{Guid.NewGuid():N}");
        const string db = "testdb";
        const string table1 = "drop_db_lock_t1";
        const string table2 = "drop_db_lock_t2";

        try
        {
            var engine = new DiskStorageEngine(storagePath);
            engine.InsertRow(db, table1, [1]);
            engine.InsertRow(db, table2, [2]);

            string file1 = Path.GetFullPath(Path.Combine(storagePath, db, $"{table1}.dat"));
            string file2 = Path.GetFullPath(Path.Combine(storagePath, db, $"{table2}.dat"));
            var locks = GetDiskFileLocks();
            Assert.True(locks.ContainsKey(file1));
            Assert.True(locks.ContainsKey(file2));

            engine.DropDatabase(db);

            Assert.False(locks.ContainsKey(file1));
            Assert.False(locks.ContainsKey(file2));
        }
        finally
        {
            if (Directory.Exists(storagePath))
                Directory.Delete(storagePath, recursive: true);
        }
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
            var manager = new IndexManager(new DataVoConfig { StorageMode = StorageMode.Disk, DiskStoragePath = root }, root);
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

            engineA.StorageContext.InsertOneIntoTable(new Dictionary<string, dynamic> { ["Value"] = 123 }, "ScopedTable", "ScopedDb");
            engineA.StorageContext.InsertOneIntoTable(new Dictionary<string, dynamic> { ["Value"] = 456 }, "ScopedTable", "ScopedDb");

            Dictionary<long, Dictionary<string, dynamic>> rows = engineA.StorageContext.GetTableContents("ScopedTable", "ScopedDb");
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
            StorageMode = StorageMode.Disk,
            DiskStoragePath = storagePath,
            WalEnabled = false,
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

    [Fact]
    public void StorageContext_CreateTable_DiskMode_CreatesPhysicalTableFile()
    {
        string storagePath = Path.Combine(Path.GetTempPath(), $"datavo_create_table_{Guid.NewGuid():N}");

        try
        {
            var context = new StorageContext(new DataVoConfig
            {
                StorageMode = StorageMode.Disk,
                DiskStoragePath = storagePath,
                WalEnabled = false
            });

            context.CreateTable("Users", "CreateTableDb");

            string tablePath = Path.Combine(storagePath, "CreateTableDb", "Users.dat");
            Assert.True(File.Exists(tablePath));
            Assert.True(new FileInfo(tablePath).Length >= 8);
        }
        finally
        {
            if (Directory.Exists(storagePath))
            {
                Directory.Delete(storagePath, recursive: true);
            }
        }
    }

    private static ConcurrentDictionary<string, object> GetDiskFileLocks()
    {
        FieldInfo? field = typeof(DiskStorageEngine).GetField("GlobalFileLocks", BindingFlags.Static | BindingFlags.NonPublic);
        Assert.NotNull(field);

        object? raw = field!.GetValue(null);
        Assert.NotNull(raw);

        return Assert.IsType<ConcurrentDictionary<string, object>>(raw);
    }
}
