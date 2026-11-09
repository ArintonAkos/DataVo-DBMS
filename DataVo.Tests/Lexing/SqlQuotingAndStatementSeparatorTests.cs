using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Parser;
using DataVo.Core.Parser.Types;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Lexing;

/// <summary>Doubled quotes are data inside a string, and a semicolon always ends a statement.</summary>
public sealed class SqlQuotingAndStatementSeparatorTests : IDisposable
{
    private readonly DataVoContext _db = new(new DataVoConfig { StorageMode = StorageMode.InMemory });

    public SqlQuotingAndStatementSeparatorTests()
    {
        Ok("CREATE DATABASE Q");
        Ok("USE Q");
        Ok("CREATE TABLE People (Id INT PRIMARY KEY, Name VARCHAR(40))");
    }

    public void Dispose() => _db.Dispose();

    [Theory]
    [InlineData("'O''Brien'", "O'Brien")]
    [InlineData("''''", "'")]
    [InlineData("''", "")]
    [InlineData("'a''''b'", "a''b")]
    [InlineData("'''quoted'''", "'quoted'")]
    [InlineData("'semi; colon'", "semi; colon")]
    [InlineData("'it''s; fine'", "it's; fine")]
    [InlineData("\"say \"\"hi\"\"\"", "say \"hi\"")]
    public void Insert_StoresTheUnescapedValue(string literal, string expected)
    {
        Ok($"INSERT INTO People (Id, Name) VALUES (1, {literal})");

        Assert.Equal(expected, Assert.Single(Ok("SELECT Name FROM People").Data)["Name"]);
    }

    [Fact]
    public void Where_MatchesAnEscapedLiteral()
    {
        Ok("INSERT INTO People (Id, Name) VALUES (1, 'O''Brien')");
        Ok("INSERT INTO People (Id, Name) VALUES (2, 'Brien')");

        Assert.Equal(1, Assert.Single(Ok("SELECT Id FROM People WHERE Name = 'O''Brien'").Data)["Id"]);
        Assert.Equal(1, Assert.Single(Ok("SELECT Id FROM People WHERE Name IN ('O''Brien', 'x')").Data)["Id"]);
    }

    [Fact]
    public void Where_MatchesAnEscapedLiteralThroughAnIndex()
    {
        Ok("CREATE INDEX ix_name ON People (Name)");
        Ok("INSERT INTO People (Id, Name) VALUES (1, 'O''Brien')");

        Assert.Single(Ok("SELECT Id FROM People WHERE Name = 'O''Brien'").Data);
    }

    [Fact]
    public void Update_And_Projection_HandleEscapedLiterals()
    {
        Ok("INSERT INTO People (Id, Name) VALUES (1, 'x')");
        Ok("UPDATE People SET Name = 'D''Arcy' WHERE Id = 1");

        Assert.Equal("D'Arcy", Assert.Single(Ok("SELECT Name FROM People").Data)["Name"]);
        Ok("UPDATE People SET Name = '''quoted''' WHERE Id = 1");
        Assert.Equal("'quoted'", Assert.Single(Ok("SELECT Name FROM People").Data)["Name"]);
    }

    [Fact]
    public void Lexer_KeepsPositionAfterAnEscapedQuote()
    {
        List<Token> tokens = new Lexer("SELECT 'a''b' FROM t").Tokenize();

        Assert.Equal(TokenType.StringLiteral, tokens[1].Type);
        Assert.Equal("FROM", tokens[2].Value, ignoreCase: true);
    }

    [Fact]
    public void UnterminatedString_IsAnError()
    {
        Assert.True(_db.Execute("INSERT INTO People (Id, Name) VALUES (1, 'oops'')")[^1].IsError);
    }

    [Fact]
    public void Update_WithWhere_DoesNotSwallowTheNextStatement()
    {
        Ok("INSERT INTO People (Id, Name) VALUES (1, 'a')");

        List<QueryResult> results = _db.Execute("UPDATE People SET Name = 'b' WHERE Id = 1; INSERT INTO People (Id, Name) VALUES (2, 'c'); SELECT Id FROM People");

        Assert.Equal(3, results.Count);
        Assert.DoesNotContain(results, result => result.IsError);
        Assert.Equal(2, results[^1].Data.Count);
    }

    [Fact]
    public void Delete_WithWhere_DoesNotSwallowTheNextStatement()
    {
        Ok("INSERT INTO People (Id, Name) VALUES (1, 'a')");
        Ok("INSERT INTO People (Id, Name) VALUES (2, 'b')");

        List<QueryResult> results = _db.Execute("DELETE FROM People WHERE Id = 1; UPDATE People SET Name = 'z' WHERE Id = 2; SELECT Name FROM People");

        Assert.DoesNotContain(results, result => result.IsError);
        Assert.Equal("z", Assert.Single(results[^1].Data)["Name"]);
    }

    [Fact]
    public void Update_WithoutWhere_StopsAtTheSemicolon()
    {
        Ok("INSERT INTO People (Id, Name) VALUES (1, 'a')");

        List<QueryResult> results = _db.Execute("UPDATE People SET Name = 'b'; SELECT Name FROM People");

        Assert.False(results[^1].IsError);
        Assert.Equal("b", Assert.Single(results[^1].Data)["Name"]);
    }

    [Fact]
    public void Select_WithWhereAndNoLimit_StopsAtTheSemicolon()
    {
        Ok("INSERT INTO People (Id, Name) VALUES (1, 'a')");

        List<QueryResult> results = _db.Execute("SELECT Id FROM People WHERE Id = 1; SELECT Name FROM People WHERE Id = 1;");

        Assert.Equal(2, results.Count);
        Assert.Equal("a", Assert.Single(results[^1].Data)["Name"]);
    }

    [Theory]
    [InlineData(";")]
    [InlineData(";;;")]
    [InlineData("  ;  ; ")]
    public void OnlySeparators_IsAnEmptyQuery(string sql)
    {
        Assert.True(_db.Execute(sql)[^1].IsError);
    }

    [Fact]
    public void RepeatedAndTrailingSeparators_AreIgnored()
    {
        Ok("INSERT INTO People (Id, Name) VALUES (1, 'a');;");

        List<QueryResult> results = _db.Execute(";SELECT Id FROM People;;  ;SELECT Name FROM People;");

        Assert.Equal(2, results.Count);
        Assert.DoesNotContain(results, result => result.IsError);
    }

    [Fact]
    public void SemicolonInsideString_DoesNotSplitTheStatement()
    {
        List<QueryResult> results = _db.Execute("INSERT INTO People (Id, Name) VALUES (1, 'a;b'); SELECT Name FROM People WHERE Name = 'a;b'");

        Assert.Equal("a;b", Assert.Single(results[^1].Data)["Name"]);
    }

    [Fact]
    public void ReactiveAndPreparedText_AcceptATrailingSemicolon()
    {
        Ok("INSERT INTO People (Id, Name) VALUES (1, 'a')");

        DataVoPreparedStatement prepared = _db.Prepare("SELECT Name FROM People WHERE Id = @id;");

        Assert.Equal("a", Assert.Single(prepared.Execute(("id", 1)).Data)["Name"]);
    }

    private QueryResult Ok(string sql)
    {
        QueryResult result = _db.Execute(sql)[^1];
        Assert.False(result.IsError, $"{sql} => {string.Join(" | ", result.Messages)}");
        return result;
    }
}
