using DataVo.Core.Enums;
using DataVo.Core.Parser.DDL;

namespace DataVo.Tests.E2E.DDL;

/// <summary>
/// GC Reduction Slice 4, P3.2 (Gate 0 / G0.6): the ALTER-path <see cref="ColumnDefinitionParser"/> must
/// parse VECTOR (it previously fell through to VARCHAR), matching the CREATE TABLE type parser. DATE was
/// already handled.
/// </summary>
public class AlterTableColumnTypeParserTests
{
    [Theory]
    [InlineData("INT", DataTypes.Int)]
    [InlineData("FLOAT", DataTypes.Float)]
    [InlineData("BIT", DataTypes.Bit)]
    [InlineData("DATE", DataTypes.Date)]
    [InlineData("VARCHAR(50)", DataTypes.Varchar)]
    [InlineData("VECTOR(384)", DataTypes.Vector)]
    [InlineData("vector(3)", DataTypes.Vector)]
    public void ParseType_MapsCatalogTypes(string typeStr, DataTypes expected)
    {
        Assert.Equal(expected, ColumnDefinitionParser.ParseType(typeStr));
    }

    [Theory]
    [InlineData("VECTOR(384)", 384)]
    [InlineData("VARCHAR(50)", 50)]
    [InlineData("INT", 0)]
    public void ParseLength_ExtractsDimensionOrZero(string typeStr, int expected)
    {
        Assert.Equal(expected, ColumnDefinitionParser.ParseLength(typeStr));
    }
}
