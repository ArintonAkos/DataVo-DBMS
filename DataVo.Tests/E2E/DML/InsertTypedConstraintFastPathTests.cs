using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E.DML;

public class InsertTypedConstraintFastPathTests
{
    [Fact]
    public void InsertTyped_DuplicatePrimaryKey_StillRejectedAfterFastPath()
    {
        using var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        foreach (var sql in new[] { "CREATE DATABASE FpDb", "USE FpDb",
            "CREATE TABLE Users (Id INT PRIMARY KEY, Name VARCHAR(20))" })
            Assert.False(ctx.Execute(sql).Last().IsError);

        var schema = new ReactiveRowSchema("Id", "Name");
        ctx.InsertTyped("Users", schema, [CellValue.From(1), CellValue.From("a")]);

        Assert.Throws<InvalidOperationException>(() =>
            ctx.InsertTyped("Users", schema, [CellValue.From(1), CellValue.From("b")]));
    }
}
