using DataVo.Core;
using DataVo.Core.CompiledQueries;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Indexing;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.E2E;

public sealed class GuidFastPathTests
{
    [Fact]
    public void GuidFastLaneComparer_MatchesGuidValueSemantics()
    {
        Guid value = Guid.Parse("3d09eb2f-0a62-4c5d-a8c3-f314d4e18664");
        Guid sameValue = Guid.Parse(value.ToString("D"));
        Guid differentValue = Guid.Parse("3d09eb2f-0a62-4c5d-a8c3-f314d4e18665");

        Assert.True(GuidSimdEqualityComparer.Instance.Equals(value, sameValue));
        Assert.False(GuidSimdEqualityComparer.Instance.Equals(value, differentValue));
        Assert.Equal(value.GetHashCode(), GuidSimdEqualityComparer.Instance.GetHashCode(value));
    }

    [Fact]
    public void TypedInsert_GuidPrimaryKey_UsesGuidPrimaryKeyFastLane()
    {
        using DataVoContext context = CreateContext();
        Guid id = Guid.Parse("a345ca3a-d8db-45c9-8edb-77d8f9872791");
        context.InsertTyped("Sessions", new ReactiveRowSchema("Id", "TenantId", "Name"),
            [CellValue.From(id), CellValue.From(Guid.NewGuid()), CellValue.From("alpha")]);

        Assert.True(context.Engine.IndexManager.HasGuidPrimaryKeyFastLane("_PK_Sessions", "Sessions", CurrentDatabase(context)));

        var plan = DataVoCompiledQueryPlan.SelectSingle("Sessions", ["Id", "Name"], "Id", "id");
        SessionProjection? hit = DataVoCompiledQuery.SelectSingleTyped(
            context,
            plan,
            [new DataVoCompiledQueryParameter("id", id)],
            static row => new SessionProjection(row.GetGuid("Id"), row.GetString("Name")!));

        Assert.NotNull(hit);
        Assert.Equal(id, hit.Id);
        Assert.Equal("alpha", hit.Name);
    }

    [Fact]
    public void TypedInsert_GuidSecondaryIndex_ReturnsAllRowsForTenant()
    {
        using DataVoContext context = CreateContext();
        Guid tenant = Guid.Parse("f5836302-1b7b-4a09-9c31-8ef9e5690a4d");
        Insert(context, Guid.Parse("c7314fa6-7a60-4913-b5dc-dfbb06978b10"), tenant, "one");
        Insert(context, Guid.Parse("588bff4b-eefb-46b6-95bb-c02a988c7606"), tenant, "two");

        Assert.True(context.Engine.IndexManager.HasGuidIndexFastLane("IX_Sessions_TenantId", "Sessions", CurrentDatabase(context)));

        var plan = DataVoCompiledQueryPlan.SelectMany(
            "Sessions",
            ["Id", "Name"],
            "TenantId",
            "tenantId",
            CompiledAccessPath.SingleColumnIndex,
            "IX_Sessions_TenantId");

        IReadOnlyList<SessionProjection> hits = DataVoCompiledQuery.SelectManyTyped(
            context,
            plan,
            [new DataVoCompiledQueryParameter("tenantId", tenant)],
            static row => new SessionProjection(row.GetGuid("Id"), row.GetString("Name")!));

        Assert.Equal(["one", "two"], hits.Select(static x => x.Name).Order());
    }

    [Fact]
    public void PreparedSelectSingle_GuidPrimaryKey_UsesDirectGuidLookup()
    {
        using DataVoContext context = CreateContext();
        Guid id = Guid.Parse("dab9e9b5-b3f4-4686-92f1-c175986fae15");
        Insert(context, id, Guid.NewGuid(), "prepared");

        var plan = DataVoCompiledQueryPlan.SelectSingle(
            "Sessions",
            ["Id", "Name"],
            "Id",
            "id",
            CompiledAccessPath.SingleColumnIndex,
            "_PK_Sessions");

        DataVoPreparedSelectSingle<SessionProjection> prepared =
            DataVoCompiledQuery.PrepareSelectSingleTyped(
                context,
                plan,
                static row => new SessionProjection(row.GetGuid("Id"), row.GetString("Name")!));

        Assert.Contains(
            typeof(DataVoPreparedSelectSingle<SessionProjection>).GetMethods().Where(static method => method.Name == "Execute"),
            static method => method.GetParameters() is [{ ParameterType: var parameterType }] && parameterType == typeof(Guid));

        SessionProjection? hit = prepared.Execute(id);

        Assert.NotNull(hit);
        Assert.Equal("prepared", hit.Name);
    }

    private static DataVoContext CreateContext()
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        ExecuteOk(context, "CREATE DATABASE GuidFastPath");
        ExecuteOk(context, "USE GuidFastPath");
        ExecuteOk(context, "CREATE TABLE Sessions (Id GUID PRIMARY KEY, TenantId GUID, Name VARCHAR(40))");
        ExecuteOk(context, "CREATE INDEX IX_Sessions_TenantId ON Sessions (TenantId)");
        return context;
    }

    private static void Insert(DataVoContext context, Guid id, Guid tenantId, string name)
    {
        context.InsertTyped("Sessions", new ReactiveRowSchema("Id", "TenantId", "Name"),
            [CellValue.From(id), CellValue.From(tenantId), CellValue.From(name)]);
    }

    private static string CurrentDatabase(DataVoContext context) =>
        context.Engine.Sessions.Get(context.SessionId) ?? throw new InvalidOperationException("Expected current database.");

    private static void ExecuteOk(DataVoContext context, string sql)
    {
        QueryResult result = context.Execute(sql).Last();
        if (result.IsError)
        {
            throw new InvalidOperationException($"{sql}{Environment.NewLine}{string.Join(" | ", result.Messages)}");
        }
    }

    private sealed record SessionProjection(Guid Id, string Name);
}
