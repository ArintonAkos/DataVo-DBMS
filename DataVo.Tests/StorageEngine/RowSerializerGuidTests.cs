using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.StorageEngine;

public sealed class RowSerializerGuidTests
{
    [Fact]
    public void DictionaryInsertAndSelect_RoundTripsGuidColumn()
    {
        using DataVoContext context = CreateContext();
        Guid id = Guid.Parse("5d38c985-1ce2-4f91-bc13-5359c9ae8236");

        context.BulkInsert("Sessions", [
            new Dictionary<string, object?> { ["Id"] = id, ["Name"] = "alpha" }
        ]);

        QueryResult result = ExecuteOk(context, "SELECT Id, Name FROM Sessions WHERE Name = 'alpha'").Last();
        Assert.Equal(id, result.Data[0]["Id"]);
        Assert.Equal("alpha", result.Data[0]["Name"]);
    }

    [Fact]
    public void TypedInsertAndSelect_RoundTripsGuidColumn()
    {
        using DataVoContext context = CreateContext();
        Guid id = Guid.Parse("fb89e770-98dd-44cf-b186-437d5238458a");

        context.InsertTyped(
            "Sessions",
            new ReactiveRowSchema("Id", "Name"),
            [CellValue.From(id), CellValue.From("beta")]);

        QueryResult result = ExecuteOk(context, "SELECT Id, Name FROM Sessions WHERE Name = 'beta'").Last();
        Assert.Equal(id, result.Data[0]["Id"]);
        Assert.Equal("beta", result.Data[0]["Name"]);
    }

    private static DataVoContext CreateContext()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ExecuteOk(context, "CREATE DATABASE GuidSerializer");
        ExecuteOk(context, "USE GuidSerializer");
        ExecuteOk(context, "CREATE TABLE Sessions (Id GUID PRIMARY KEY, Name VARCHAR(40))");
        return context;
    }

    private static List<QueryResult> ExecuteOk(DataVoContext context, string sql)
    {
        List<QueryResult> results = context.Execute(sql);
        QueryResult last = results.Last();
        if (last.IsError)
        {
            throw new InvalidOperationException($"{sql}{Environment.NewLine}{string.Join(" | ", last.Messages)}");
        }

        return results;
    }
}
