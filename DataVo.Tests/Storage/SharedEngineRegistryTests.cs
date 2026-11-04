using System.Reflection;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Exceptions;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine.Config;
using DataVo.Data;
using DataVo.Tests.BrowserParity;

namespace DataVo.Tests.Storage;

[BrowserTranslateIgnore("Data-directory sharing and file locks are disk-specific")]
public sealed class SharedEngineRegistryTests : IDisposable
{
    private readonly string _path = TestStorageDirectories.Create("shared");

    public static TheoryData<StorageMode> PersistentModes => new() { StorageMode.Disk, StorageMode.Lsm };

    [Theory]
    [MemberData(nameof(PersistentModes))]
    public void ContextsOnSameDirectory_ShareOneEngine_UntilLastDispose(StorageMode mode)
    {
        var first = Open(mode);
        AssertOk(first.Execute("CREATE DATABASE SharedDb"));
        AssertOk(first.Execute("USE SharedDb"));
        AssertOk(first.Execute("CREATE TABLE Items (Id INT PRIMARY KEY, Name VARCHAR(20))"));

        using var second = Open(mode);
        Assert.Same(first.Engine, second.Engine);

        AssertOk(second.Execute("USE SharedDb"));
        AssertOk(second.Execute("INSERT INTO Items (Id, Name) VALUES (1, 'a')"));
        Assert.Single(first.Execute("SELECT * FROM Items WHERE Id = 1")[0].Data);

        first.Dispose();
        first.Dispose(); // a repeated dispose must not release the other context's reference

        AssertOk(second.Execute("INSERT INTO Items (Id, Name) VALUES (2, 'b')"));
        Assert.Equal(2, second.Execute("SELECT * FROM Items")[0].Data.Count);
        Assert.Throws<DatabaseInUseException>(TryLockDirectory);
    }

    [Theory]
    [MemberData(nameof(PersistentModes))]
    public void LastDispose_ReleasesDirectoryLock(StorageMode mode)
    {
        using (Open(mode))
        using (Open(mode))
        {
        }

        TryLockDirectory();
    }

    [Fact]
    public void DirectoryLockedElsewhere_RejectsOpen()
    {
        using FileStream foreignLock = LockDirectory();

        var ex = Assert.Throws<DatabaseInUseException>(() => Open(StorageMode.Disk));
        Assert.Contains("locked by another process", ex.Message, StringComparison.Ordinal);
        Assert.Equal(Path.GetFullPath(_path), ex.Directory);
    }

    [Fact]
    public void IncompatibleConfigOnOpenDirectory_IsRejected()
    {
        using var first = Open(StorageMode.Disk);

        var ex = Assert.Throws<DatabaseInUseException>(() => new DataVoContext(new DataVoConfig
        {
            StorageMode = StorageMode.Disk,
            DiskStoragePath = _path,
            EnableAuthorization = true,
        }));
        Assert.Contains(nameof(DataVoConfig.EnableAuthorization), ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void EquivalentPathSpelling_SharesEngine()
    {
        using var first = Open(StorageMode.Disk);
        using var second = new DataVoContext(new DataVoConfig
        {
            StorageMode = StorageMode.Disk,
            DiskStoragePath = _path + Path.DirectorySeparatorChar,
            LsmStrictFsync = false,
        });

        Assert.Same(first.Engine, second.Engine);
    }

    [Fact]
    public void InMemoryContexts_StayPrivate()
    {
        using var first = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        using var second = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });

        Assert.NotSame(first.Engine, second.Engine);
    }

    [Fact]
    public void ConcurrentAdoConnections_ToSameDataSource_ShareEngine()
    {
        string dataSource = $"datavo_shared_ado_{Guid.NewGuid():N}";
        string connectionString = $"StorageMode=Disk;DataSource={dataSource}";
        try
        {
            using var writer = new DataVoConnection(connectionString);
            using var reader = new DataVoConnection(connectionString);
            writer.Open();
            reader.Open();

            using (var create = writer.CreateCommand())
            {
                create.CommandText = "CREATE TABLE Notes (Id INT PRIMARY KEY, Body VARCHAR(20))";
                create.ExecuteNonQuery();
            }

            using (var insert = writer.CreateCommand())
            {
                insert.CommandText = "INSERT INTO Notes (Id, Body) VALUES (1, 'hi')";
                Assert.Equal(1, insert.ExecuteNonQuery());
            }

            using var count = reader.CreateCommand();
            count.CommandText = "SELECT * FROM Notes";
            using var rows = count.ExecuteReader();
            Assert.True(rows.Read());
        }
        finally
        {
            string directory = Path.Combine(Directory.GetCurrentDirectory(), dataSource);
            if (Directory.Exists(directory))
            {
                Directory.Delete(directory, recursive: true);
            }
        }
    }

    [Fact]
    public void ConfigFingerprint_CoversEveryEngineSetting()
    {
        var captured = DataVoConfigSharing.Capture(new DataVoConfig()).Select(p => p.Key).ToHashSet();
        IEnumerable<string> settable = typeof(DataVoConfig)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite)
            .Select(p => p.Name);

        foreach (string name in settable)
        {
            Assert.True(
                captured.Contains(name) || DataVoConfigSharing.IgnoredProperties.Contains(name),
                $"DataVoConfig.{name} must be captured in DataVoConfigSharing.Capture or listed as ignored.");
        }
    }

    [Fact]
    public void ConfigFingerprint_DetectsEveryCapturedSettingChange()
    {
        DataVoConfig baseline = NewDiskConfig();
        IReadOnlyList<KeyValuePair<string, string>> expected = DataVoConfigSharing.Capture(baseline);

        foreach (PropertyInfo property in typeof(DataVoConfig).GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!property.CanWrite || DataVoConfigSharing.IgnoredProperties.Contains(property.Name))
            {
                continue;
            }

            DataVoConfig changed = NewDiskConfig();
            property.SetValue(changed, Mutate(property.PropertyType, property.GetValue(changed)));

            Assert.Equal(property.Name, DataVoConfigSharing.FindConflict(expected, DataVoConfigSharing.Capture(changed)));
        }
    }

    public void Dispose()
    {
    }

    private DataVoConfig NewDiskConfig() => new() { StorageMode = StorageMode.Disk, DiskStoragePath = _path };

    private DataVoContext Open(StorageMode mode) =>
        new(new DataVoConfig { StorageMode = mode, DiskStoragePath = _path, LsmStrictFsync = false });

    private FileStream LockDirectory() =>
        new(Path.Combine(_path, "datavo.lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);

    private void TryLockDirectory()
    {
        try
        {
            using FileStream probe = LockDirectory();
        }
        catch (IOException ex)
        {
            throw new DatabaseInUseException(_path, "directory still locked", ex);
        }
    }

    private static object? Mutate(Type type, object? value)
    {
        if (type == typeof(bool)) return !(bool)value!;
        if (type == typeof(int)) return (int)value! + 1;
        if (type == typeof(double)) return (double)value! + 0.5d;
        if (type == typeof(string)) return (value as string ?? "x") + "_changed";
        if (type.IsEnum)
        {
            Array values = Enum.GetValues(type);
            int index = Array.IndexOf(values, value);
            return values.GetValue((index + 1) % values.Length);
        }

        if (type == typeof(List<string>)) return new List<string> { "changed" };
        if (type == typeof(List<DataVoAuthUser>)) return new List<DataVoAuthUser> { new() { Username = "changed" } };

        throw new NotSupportedException($"Add a mutation for config property type {type}.");
    }

    private static void AssertOk(List<QueryResult> results)
    {
        QueryResult? error = results.FirstOrDefault(r => r.IsError);
        Assert.True(error is null, error is null ? null : string.Join("; ", error.Messages));
    }
}
