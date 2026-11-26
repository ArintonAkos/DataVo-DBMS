using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

/// <summary>
/// <c>GO</c> is accepted as a no-op batch separator so T-SQL style scripts run unchanged.
/// </summary>
public sealed class GoStatementTests
{
    [Fact]
    public void Go_BetweenStatements_IsAcceptedAsNoOp_AndLaterStatementsStillRun()
    {
        using var db = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });

        List<QueryResult> results = db.Execute(
            "CREATE DATABASE G; GO; USE G; CREATE TABLE T (Id INT PRIMARY KEY); GO; INSERT INTO T (Id) VALUES (1)");

        Assert.All(results, result => Assert.False(result.IsError, string.Join(" / ", result.Messages)));
        Assert.Equal(6, results.Count);
        Assert.Empty(results[1].Data);
        Assert.Empty(results[1].Messages);

        QueryResult select = db.Execute("SELECT Id FROM T").Single();
        Assert.False(select.IsError, string.Join(" / ", select.Messages));
        Assert.Single(select.Data);
    }
}
