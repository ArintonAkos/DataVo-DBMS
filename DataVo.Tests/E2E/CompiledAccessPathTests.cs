using DataVo.Core.CompiledQueries;

namespace DataVo.Tests.E2E;

public class CompiledAccessPathTests
{
    [Fact]
    public void SelectMany_DefaultAccessPath_IsRuntimeResolve()
    {
        var plan = DataVoCompiledQueryPlan.SelectMany("Players", ["Id", "Name"], "Name", "name");

        Assert.Equal(CompiledAccessPath.RuntimeResolve, plan.AccessPath);
        Assert.Null(plan.ResolvedIndexName);
    }

    [Fact]
    public void SelectMany_TaggedSingleColumnIndex_CarriesAccessPathAndIndexName()
    {
        var plan = DataVoCompiledQueryPlan.SelectMany(
            "Players", ["Id", "Name"], "Name", "name",
            accessPath: CompiledAccessPath.SingleColumnIndex,
            resolvedIndexName: "ix_players_name");

        Assert.Equal(CompiledAccessPath.SingleColumnIndex, plan.AccessPath);
        Assert.Equal("ix_players_name", plan.ResolvedIndexName);
    }

    [Fact]
    public void SelectMany_SingleColumnIndexWithoutIndexName_Throws()
    {
        Assert.Throws<ArgumentException>(() => DataVoCompiledQueryPlan.SelectMany(
            "Players", ["Id", "Name"], "Name", "name",
            accessPath: CompiledAccessPath.SingleColumnIndex,
            resolvedIndexName: null));
    }
}
