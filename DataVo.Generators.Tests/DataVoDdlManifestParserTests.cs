using System.Collections.Immutable;
using DataVo.Generators.Sql;

namespace DataVo.Generators.Tests;

public class DataVoDdlManifestParserTests
{
    [Fact]
    public void Parse_SingleColumnIndex_Resolves()
    {
        CompileTimeCatalog catalog = DataVoDdlManifestParser.Parse(ImmutableArray.Create(
            "CREATE TABLE OrderItems (OrderItemId INT PRIMARY KEY, OrderId INT, Sku VARCHAR(50)); " +
            "CREATE INDEX ix_OrderItems_OrderId ON OrderItems (OrderId);"));

        Assert.True(catalog.TryResolveSingleColumnIndex("OrderItems", "OrderId", out string name));
        Assert.Equal("ix_OrderItems_OrderId", name);
    }

    [Fact]
    public void Parse_PrimaryKey_IsRecognized()
    {
        CompileTimeCatalog catalog = DataVoDdlManifestParser.Parse(ImmutableArray.Create(
            "CREATE TABLE OrderItems (OrderItemId INT PRIMARY KEY, OrderId INT);"));

        Assert.True(catalog.IsPrimaryKey("OrderItems", "OrderItemId"));
        Assert.False(catalog.IsPrimaryKey("OrderItems", "OrderId"));
    }

    [Fact]
    public void Parse_TableConstraintPrimaryKey_IsRecognized()
    {
        CompileTimeCatalog catalog = DataVoDdlManifestParser.Parse(ImmutableArray.Create(
            "CREATE TABLE OrderItems (OrderItemId INT, OrderId INT, PRIMARY KEY (OrderItemId));"));

        Assert.True(catalog.IsPrimaryKey("OrderItems", "OrderItemId"));
    }

    [Fact]
    public void Parse_CompositeIndex_IsIgnored()
    {
        CompileTimeCatalog catalog = DataVoDdlManifestParser.Parse(ImmutableArray.Create(
            "CREATE INDEX ix_multi ON OrderItems (OrderId, Sku);"));

        Assert.False(catalog.TryResolveSingleColumnIndex("OrderItems", "OrderId", out _));
    }

    [Fact]
    public void Parse_LookupIsCaseInsensitive()
    {
        CompileTimeCatalog catalog = DataVoDdlManifestParser.Parse(ImmutableArray.Create(
            "CREATE INDEX ix ON Players (Name);"));

        Assert.True(catalog.TryResolveSingleColumnIndex("players", "name", out string name));
        Assert.Equal("ix", name);
    }

    [Fact]
    public void Parse_EmptyOrUnrecognized_ReturnsEmptyCatalog()
    {
        CompileTimeCatalog catalog = DataVoDdlManifestParser.Parse(ImmutableArray.Create("SELECT 1;", ""));

        Assert.False(catalog.TryResolveSingleColumnIndex("T", "Col", out _));
        Assert.False(catalog.IsPrimaryKey("T", "Col"));
    }

    [Fact]
    public void Parse_EqualManifests_ProduceEqualCatalogs()
    {
        var texts = ImmutableArray.Create("CREATE INDEX ix ON Players (Name);");
        CompileTimeCatalog a = DataVoDdlManifestParser.Parse(texts);
        CompileTimeCatalog b = DataVoDdlManifestParser.Parse(texts);

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }
}
