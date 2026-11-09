using DataVo.Core.Contracts;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Parser.AST;
using DataVo.Core.Parser.Binding;
using DataVo.Core.Parser.DQL;
using DataVo.Core.PreparedStatements;
using DataVo.Core.Runtime;

namespace DataVo.Core.Parser;

/// <summary>Opens a streamed SELECT: a paged scan when the query allows one, otherwise its evaluated result.</summary>
internal static class QueryStream
{
    public static IEnumerable<DataVoRow> Open(DataVoEngine engine, string sql, DataVoQueryParameter[] parameters, Guid session)
    {
        using SnapshotLockScope readScope = engine.EnterRuntimeReadScope();
        using IDisposable current = DataVoEngine.PushCurrent(engine);

        List<SqlStatement> statements = new Parser(new Lexer(sql).Tokenize()).Parse();
        if (statements is not [SelectStatement or UnionSelectStatement])
        {
            throw new ArgumentException("StreamQuery runs exactly one SELECT statement.", nameof(sql));
        }

        statements = SqlParameterBinder.BindStatements(statements, parameters);
        SqlNameResolver.Resolve(statements, engine, session);

        if (CreateAction(statements, engine) is Select select && select.TryOpenStream(session) is { } stream)
        {
            return stream;
        }

        return Evaluate(statements, engine, session);
    }

    private static IEnumerable<DataVoRow> Evaluate(List<SqlStatement> statements, DataVoEngine engine, Guid session)
    {
        QueryResult result = CreateAction(statements, engine).Perform(session);
        if (result.IsError)
        {
            throw new InvalidOperationException(string.Join(" | ", result.Messages));
        }

        var schema = new DataVoRowSchema(result.Fields);
        return result.Data.Select(row => new DataVoRow(schema, [.. result.Fields.Select(field => row.GetValueOrDefault(field))]));
    }

    private static IDbAction CreateAction(List<SqlStatement> statements, DataVoEngine engine) =>
        new Evaluator(statements, engine).ToRunnables()[0].Dequeue();
}
