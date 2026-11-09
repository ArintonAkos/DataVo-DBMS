using DataVo.Core.Parser.AST;
using DataVo.Core.Runtime;

namespace DataVo.Core.Parser.Binding;

/// <summary>
/// Rewrites the database, table and index names in parsed statements to the spelling they were created
/// with. SQL names are case-insensitive, but locks, index caches, the commit log and data directories all
/// key on the exact text, so every statement must reach them with one spelling.
/// </summary>
/// <remarks>
/// A batch is resolved before any of it runs, so names created, dropped or selected earlier in the same
/// batch are tracked here rather than read from the catalog.
/// </remarks>
/// <example>
/// <code>
/// List&lt;SqlStatement&gt; statements = new Parser(tokens).Parse();
/// SqlNameResolver.Resolve(statements, engine, sessionId);
/// </code>
/// </example>
internal sealed class SqlNameResolver
{
    private readonly EngineCatalog _catalog;
    private string? _database;
    private Dictionary<string, string?>? _batchDatabases;
    private Dictionary<(string Database, string Table), string?>? _batchTables;
    private HashSet<string>? _cteNames;

    private SqlNameResolver(EngineCatalog catalog, string? database)
    {
        _catalog = catalog;
        _database = database;
    }

    /// <summary>
    /// Resolves every statement of a batch in order, starting from the session's current database.
    /// </summary>
    /// <param name="statements">The statements, which are modified in place.</param>
    /// <param name="engine">The engine whose catalog owns the names.</param>
    /// <param name="session">The session the batch runs under.</param>
    /// <example>
    /// <code>
    /// SqlNameResolver.Resolve(statements, engine, sessionId);
    /// </code>
    /// </example>
    public static void Resolve(IReadOnlyList<SqlStatement> statements, DataVoEngine engine, Guid session)
    {
        var resolver = new SqlNameResolver(engine.Catalog, engine.Sessions.Get(session));
        for (int i = 0; i < statements.Count; i++)
        {
            resolver.ResolveStatement(statements[i]);
        }
    }

    /// <summary>
    /// Resolves a single statement against a known database.
    /// </summary>
    /// <param name="statement">The statement, which is modified in place.</param>
    /// <param name="engine">The engine whose catalog owns the names.</param>
    /// <param name="databaseName">The database the statement runs against.</param>
    /// <example>
    /// <code>
    /// SqlNameResolver.Resolve(select, engine, "Shop");
    /// </code>
    /// </example>
    public static void Resolve(SqlStatement statement, DataVoEngine engine, string databaseName)
    {
        new SqlNameResolver(engine.Catalog, databaseName).ResolveStatement(statement);
    }

    private void ResolveStatement(SqlStatement statement)
    {
        switch (statement)
        {
            case SelectStatement select:
                ResolveSelect(select);
                break;
            case UnionSelectStatement union:
                ResolveSelect(union.Left);
                union.Branches.ForEach(branch => ResolveSelect(branch.Select));
                break;
            case ExplainStatement explain:
                ResolveSelect(explain.Select);
                break;
            case InsertIntoStatement insert:
                ResolveTable(insert.TableName);
                ResolveInsertValues(insert);
                break;
            case UpdateStatement update:
                ResolveTable(update.TableName);
                update.SetClauses.ForEach(clause => ResolveExpression(clause.Value));
                ResolveExpression(update.WhereClause);
                break;
            case DeleteFromStatement delete:
                ResolveTable(delete.TableName);
                ResolveExpression(delete.WhereExpression);
                break;
            default:
                ResolveCatalogStatement(statement);
                break;
        }
    }

    private void ResolveCatalogStatement(SqlStatement statement)
    {
        switch (statement)
        {
            case UseStatement use:
                ResolveDatabase(use.DatabaseName);
                _database = use.DatabaseName.Name;
                break;
            case CreateDatabaseStatement create:
                ResolveDatabase(create.DatabaseName);
                RememberDatabase(create.DatabaseName.Name, create.DatabaseName.Name);
                break;
            case DropDatabaseStatement drop:
                ResolveDatabase(drop.DatabaseName);
                RememberDatabase(drop.DatabaseName.Name, null);
                break;
            case CreateTableStatement create:
                ResolveCreateTable(create);
                break;
            case DropTableStatement drop:
                ResolveTable(drop.TableName);
                RememberTable(drop.TableName.Name, null);
                break;
            case AlterTableStatement alter:
                ResolveTable(alter.TableName);
                break;
            case CreateIndexStatement createIndex:
                ResolveTable(createIndex.TableName);
                break;
            case DropIndexStatement dropIndex:
                ResolveTable(dropIndex.TableName);
                ResolveIndex(dropIndex);
                break;
            case DescribeStatement describe:
                ResolveTable(describe.TableName);
                break;
            case VacuumStatement vacuum:
                ResolveTable(vacuum.TableName);
                break;
        }
    }

    private void ResolveCreateTable(CreateTableStatement create)
    {
        ResolveTable(create.TableName);
        RememberTable(create.TableName.Name, create.TableName.Name);
        foreach (ColumnDefinitionNode column in create.Columns)
        {
            if (column.ReferencesTable is not null)
            {
                ResolveTable(column.ReferencesTable);
            }
        }
    }

    private void ResolveInsertValues(InsertIntoStatement insert)
    {
        foreach (List<SqlNode> row in insert.ValuesLists)
        {
            foreach (SqlNode value in row)
            {
                ResolveExpression(value as ExpressionNode);
            }
        }
    }

    private void ResolveSelect(SelectStatement select)
    {
        HashSet<string>? outerCtes = _cteNames;
        if (select.Ctes.Count > 0)
        {
            _cteNames = outerCtes is null
                ? new HashSet<string>(StringComparer.OrdinalIgnoreCase)
                : new HashSet<string>(outerCtes, StringComparer.OrdinalIgnoreCase);
            select.Ctes.ForEach(cte => _cteNames.Add(cte.Name.Name));
            select.Ctes.ForEach(cte => ResolveSelect(cte.Select));
        }

        if (select.FromTable is not null)
        {
            select.FromAlias = ResolveTableReference(select.FromTable, select.FromAlias);
        }

        foreach (JoinDetailNode join in select.Joins)
        {
            join.Alias = ResolveTableReference(join.TableName, join.Alias);
        }

        select.Columns.ForEach(column => ResolveExpression(column.Expression));
        ResolveExpression(select.WhereExpression);
        ResolveExpression(select.HavingExpression);
        _cteNames = outerCtes;
    }

    /// <summary>
    /// Resolves a FROM or JOIN table. The spelling written in the query stays as the alias, so qualified
    /// columns such as <c>items.Id</c> keep naming this table once it is stored as <c>Items</c>.
    /// </summary>
    private IdentifierNode? ResolveTableReference(IdentifierNode table, IdentifierNode? alias)
    {
        int aliasStart = table.Name.IndexOf(' ');
        string written = aliasStart > 0 ? table.Name[..aliasStart] : table.Name;
        if (_cteNames?.Contains(written) == true)
        {
            return alias;
        }

        string stored = StoredTableName(written);
        if (string.Equals(stored, written, StringComparison.Ordinal))
        {
            return alias;
        }

        table.Name = stored + table.Name[written.Length..];
        return alias ?? (aliasStart > 0 ? null : new IdentifierNode(written));
    }

    private void ResolveExpression(ExpressionNode? expression)
    {
        switch (expression)
        {
            case BinaryExpressionNode binary:
                ResolveExpression(binary.Left);
                ResolveExpression(binary.Right);
                break;
            case AggregateExpressionNode aggregate:
                ResolveExpression(aggregate.Argument);
                break;
            case ScalarFunctionExpressionNode function:
                function.Arguments.ForEach(ResolveExpression);
                break;
            case InSubqueryExpressionNode inSubquery:
                ResolveExpression(inSubquery.Left);
                ResolveStatement(inSubquery.Subquery);
                break;
            case ExistsSubqueryExpressionNode exists:
                ResolveStatement(exists.Subquery);
                break;
            case ScalarSubqueryExpressionNode scalar:
                ResolveStatement(scalar.Subquery);
                break;
        }
    }

    private void ResolveDatabase(IdentifierNode database)
    {
        if (_batchDatabases is not null && _batchDatabases.TryGetValue(database.Name, out string? inBatch))
        {
            database.Name = inBatch ?? database.Name;
            return;
        }

        database.Name = _catalog.FindDatabaseName(database.Name) ?? database.Name;
    }

    private void ResolveTable(IdentifierNode table)
    {
        table.Name = StoredTableName(table.Name);
    }

    private void ResolveIndex(DropIndexStatement dropIndex)
    {
        if (_database is null || _catalog.FindTableName(dropIndex.TableName.Name, _database) is null)
        {
            return;
        }

        foreach (var index in _catalog.GetTableIndexesReadOnly(dropIndex.TableName.Name, _database))
        {
            if (string.Equals(index.IndexFileName, dropIndex.IndexName.Name, StringComparison.OrdinalIgnoreCase))
            {
                dropIndex.IndexName.Name = index.IndexFileName;
                return;
            }
        }
    }

    private string StoredTableName(string written)
    {
        if (_database is null)
        {
            return written;
        }

        if (_batchTables is not null && _batchTables.TryGetValue((_database, written), out string? inBatch))
        {
            return inBatch ?? written;
        }

        return _catalog.FindTableName(written, _database) ?? written;
    }

    private void RememberDatabase(string name, string? stored)
    {
        _batchDatabases ??= new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        _batchDatabases[name] = stored;
    }

    private void RememberTable(string name, string? stored)
    {
        if (_database is null)
        {
            return;
        }

        _batchTables ??= new Dictionary<(string, string), string?>(TableSchemaKeyComparer.Instance);
        _batchTables[(_database, name)] = stored;
    }
}
