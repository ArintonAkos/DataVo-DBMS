using DataVo.Core;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Changes;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;

namespace DataVo.Tests.Reactive;

/// <summary>Helpers for driving reactive operators directly in tests.</summary>
internal static class ReactiveTestRows
{
    /// <summary>Converts dictionary rows into the typed seed rows operators consume.</summary>
    public static IEnumerable<TypedRow> Typed(params IReadOnlyDictionary<string, object?>[] rows) =>
        rows.Select(row => TypedRow.FromImage(row)!.Value).ToArray();

    /// <summary>Builds a delta with the operator and materializes it, as the registry does for owned subscribers.</summary>
    public static QueryChange ApplyOwned(this IBorrowedReactiveQuery query, IReadOnlyList<RowChange> changes)
    {
        var builder = new QueryChangeBuilder(query.OutputSchema);
        query.ApplyInto(changes, builder);
        return builder.Build().Materialize();
    }

    /// <summary>Creates an in-memory engine whose database holds the given tables (operators resolve columns from the catalog).</summary>
    public static DataVoEngine EngineWith(string database, params string[] createTableSql)
    {
        var context = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
        Assert.False(context.Execute($"CREATE DATABASE {database}").Last().IsError);
        Assert.False(context.Execute($"USE {database}").Last().IsError);
        foreach (string sql in createTableSql)
        {
            Assert.False(context.Execute(sql).Last().IsError, sql);
        }

        return context.Engine;
    }
}
