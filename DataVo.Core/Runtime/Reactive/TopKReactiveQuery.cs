using DataVo.Core.Parser.AST;

namespace DataVo.Core.Runtime.Reactive;

/// <summary>
/// A single-table maintained top-K (L2): <c>ORDER BY … LIMIT</c> over the matching rows.
/// </summary>
internal sealed partial class TopKReactiveQuery : IReactiveQuery
{
    /// <summary>
    /// Compiles the supplied parsed SELECT into a maintained top-K operator.
    /// </summary>
    /// <param name="select">The parsed top-K SELECT.</param>
    /// <param name="engine">The owning engine (catalog/storage access).</param>
    /// <param name="databaseName">The database that owns the source table.</param>
    public TopKReactiveQuery(SelectStatement select, DataVoEngine engine, string databaseName)
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
