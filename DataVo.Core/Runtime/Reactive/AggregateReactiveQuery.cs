using DataVo.Core.Parser.AST;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A single-table per-group incremental aggregate (L2): <c>GROUP BY</c> with
/// <c>COUNT</c>/<c>SUM</c>/<c>AVG</c>/<c>MIN</c>/<c>MAX</c>.
/// </summary>
internal sealed partial class AggregateReactiveQuery : IReactiveQuery
{
    /// <summary>
    /// Compiles the supplied parsed SELECT into an incremental aggregate operator.
    /// </summary>
    /// <param name="select">The parsed aggregate SELECT.</param>
    /// <param name="engine">The owning engine (catalog/storage access).</param>
    /// <param name="databaseName">The database that owns the source table.</param>
    public AggregateReactiveQuery(SelectStatement select, DataVoEngine engine, string databaseName)
    {
        throw new NotImplementedException();
    }

    /// <inheritdoc />
    public string Table => throw new NotImplementedException();

    /// <inheritdoc />
    public void Seed(IEnumerable<(long RowId, IReadOnlyDictionary<string, object?> Row)> rows) =>
        throw new NotImplementedException();

    /// <inheritdoc />
    public QueryChange Apply(IReadOnlyList<Changes.RowChange> tableChanges) =>
        throw new NotImplementedException();
}
