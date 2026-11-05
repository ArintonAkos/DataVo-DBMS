using System.Globalization;
using DataVo.Core;
using DataVo.Core.BTree;
using DataVo.Core.Runtime;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.BTree;

/// <summary>
/// Index keys must not depend on the thread's culture. CurrentCulture is per thread (ASP.NET sets it
/// per request); with a comma decimal separator the key of 1.25 was "[1,25]", a two-element vector,
/// so writers and readers on threads with different cultures disagreed about the same value.
/// </summary>
public sealed class IndexKeyCultureTests
{
    [Theory]
    [InlineData("hu-HU")]
    [InlineData("fr-FR")]
    [InlineData("en-US")]
    public void DictionaryKeys_AreCultureInvariant(string cultureName)
    {
        CultureInfo original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(cultureName);

            Assert.Equal("[1.25]", Key(1.25d));
            Assert.Equal("[1.25]", Key(1.25m));
            Assert.Equal("[1.25]", Key(1.25f));
            Assert.Equal("[42]", Key(42));
            Assert.Equal("[16777217]", Key(16_777_217)); // above float precision: must stay exact
            Assert.Equal("[4294967296]", Key(4_294_967_296L));
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    [Theory]
    [InlineData(StorageMode.InMemory)]
    [InlineData(StorageMode.Lsm)]
    public void FloatIndex_WrittenUnderCommaCulture_IsFoundWithInvariantKey(StorageMode mode)
    {
        CultureInfo original = CultureInfo.CurrentCulture;
        string? directory = mode == StorageMode.Lsm ? TestStorageDirectories.Create("index_key_culture") : null;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("hu-HU");
            using var context = new DataVoContext(new DataVoConfig { StorageMode = mode, DiskStoragePath = directory });
            context.Execute("CREATE DATABASE Culture");
            context.Execute("USE Culture");
            context.Execute("CREATE TABLE T (Id INT PRIMARY KEY, F FLOAT)");
            context.Execute("CREATE INDEX ix_f ON T (F)");
            context.Execute("INSERT INTO T (Id, F) VALUES (1, 12.5)");
            context.Execute("UPDATE T SET F = 1.25 WHERE Id = 1");

            var indexes = DataVoEngine.Current().IndexManager;
            Assert.Single(indexes.FilterUsingIndex("[1.25]", "ix_f", "T", "Culture"));
            Assert.Empty(indexes.FilterUsingIndex("[12.5]", "ix_f", "T", "Culture"));
            Assert.Empty(indexes.FilterUsingIndex("[1,25]", "ix_f", "T", "Culture"));
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }

    private static string Key(object value) =>
        IndexKeyEncoder.BuildKeyString(new Dictionary<string, object?> { ["C"] = value }, ["C"]);
}
