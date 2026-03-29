using DataVo.Core.Cache;
using DataVo.Core.Exceptions;
using DataVo.Core.MVCC;
using DataVo.Core.Parser;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine;
using DataVo.Core.StorageEngine.Config;
using DataVo.Core.StorageEngine.Disk;
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

    private static ConcurrentDictionary<string, object> GetDiskFileLocks()
    {
        FieldInfo? field = typeof(DiskStorageEngine).GetField("GlobalFileLocks", BindingFlags.Static | BindingFlags.NonPublic);
        Assert.NotNull(field);

        object? raw = field!.GetValue(null);
        Assert.NotNull(raw);

        return Assert.IsType<ConcurrentDictionary<string, object>>(raw);
    }
}
