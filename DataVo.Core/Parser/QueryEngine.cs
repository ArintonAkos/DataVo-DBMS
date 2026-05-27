using DataVo.Core.Contracts;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Logging;
using DataVo.Core.Runtime;
using DataVo.Core.Runtime.Diagnostics;

namespace DataVo.Core.Parser;

/// <summary>
/// Orchestrates the SQL parse pipeline from raw query text to executed actions.
/// </summary>
/// <remarks>
/// The query engine is responsible for coordinating lexing, parsing, evaluator dispatch, and action execution
/// for a single input batch.
/// </remarks>
/// <example>
/// <code>
/// var engine = DataVoEngine.Initialize(new DataVoConfig { StorageMode = StorageMode.InMemory });
/// var queryEngine = new QueryEngine("SELECT * FROM Users", sessionId, engine);
/// List&lt;QueryResult&gt; results = queryEngine.Parse();
/// </code>
/// </example>
public class QueryEngine(string query, Guid session, DataVoEngine? engine = null)
{
    private readonly DataVoEngine _engine = engine ?? DataVoEngine.Current();

    /// <summary>
    /// Gets a value indicating whether verbose parser diagnostics are enabled through environment variables.
    /// </summary>
    private static bool ParserDebugEnabled =>
        string.Equals(Environment.GetEnvironmentVariable("DATAVO_PARSER_DEBUG"), "1", StringComparison.OrdinalIgnoreCase)
        || string.Equals(Environment.GetEnvironmentVariable("DATAVO_PARSER_DEBUG"), "true", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Parses and executes the input query batch.
    /// </summary>
    /// <returns>The execution results for each runnable statement batch.</returns>
    public List<QueryResult> Parse()
    {
        using var _ = DataVoEngine.PushCurrent(_engine);
        string? databaseName = _engine.Sessions.Get(session);
        RuntimeQueryStatsBuilder? diagnosticsBuilder = null;
        if (_engine.Diagnostics.Enabled)
        {
            diagnosticsBuilder = new RuntimeQueryStatsBuilder
            {
                QueryText = query,
                StorageMode = _engine.Config.StorageMode,
                DatabaseName = databaseName
            };
            diagnosticsBuilder.SetOperation(InferOperation(query));
        }

        using RuntimeQueryDiagnosticsScope? diagnosticsScope =
            RuntimeQueryDiagnosticsScope.Start(_engine.Diagnostics, diagnosticsBuilder);

        List<QueryResult> response = [];

        List<Queue<IDbAction>> runnables;
        try
        {
            if (ParserDebugEnabled)
            {
                Logger.Info($"[ParserDebug] Incoming query: {query}");
            }

            var lexer = new Lexer(query);
            var tokens = lexer.Tokenize();

            if (ParserDebugEnabled)
            {
                Logger.Info($"[ParserDebug] Tokens ({tokens.Count}): {string.Join(" | ", tokens.Select(t => t.ToString()))}");
            }

            var parser = new Parser(tokens);
            var statements = parser.Parse();

            if (ParserDebugEnabled)
            {
                Logger.Info($"[ParserDebug] Parsed statements ({statements.Count}): {string.Join(", ", statements.Select(s => s.GetType().Name))}");
            }

            var evaluator = new Evaluator(statements, _engine);
            runnables = evaluator.ToRunnables();
        }
        catch (Exception ex)
        {
            diagnosticsBuilder?.RecordError(ex.Message);
            if (ParserDebugEnabled)
            {
                Logger.Error($"[ParserDebug] Parse pipeline failure: {ex.GetType().Name}: {ex.Message}");
            }

            response.Add(QueryResult.Error(ex.ToString()));
            return response;
        }

        foreach (Queue<IDbAction> runnable in runnables)
        {
            ExecuteRunnableQueue(runnable, response, diagnosticsBuilder);
        }

        return response;
    }

    private static string InferOperation(string sql)
    {
        string trimmed = sql.TrimStart();
        if (trimmed.Length == 0)
        {
            return "UNKNOWN";
        }

        int end = 0;
        while (end < trimmed.Length && !char.IsWhiteSpace(trimmed[end]) && trimmed[end] != ';')
        {
            end++;
        }

        return trimmed[..end].ToUpperInvariant();
    }

    /// <summary>
    /// Executes a queue of actions until completion or until one action fails.
    /// </summary>
    private void ExecuteRunnableQueue(
        Queue<IDbAction> runnable,
        List<QueryResult> response,
        RuntimeQueryStatsBuilder? diagnosticsBuilder)
    {
        while (runnable.Count != 0)
        {
            try
            {
                QueryResult result = runnable.Dequeue().Perform(session);
                response.Add(result);
                if (diagnosticsBuilder is not null)
                {
                    RecordResultMetrics(diagnosticsBuilder, result);
                }
            }
            catch (Exception ex)
            {
                diagnosticsBuilder?.RecordError(ex.Message);
                response.Add(QueryResult.Error(ex.ToString()));
                break;
            }
        }
    }

    private static void RecordResultMetrics(RuntimeQueryStatsBuilder builder, QueryResult result)
    {
        if (result.IsError)
        {
            builder.RecordError(string.Join(" | ", result.Messages));
        }

        if (result.Data.Count > 0)
        {
            builder.AddRowsReturned(result.Data.Count);
        }

        foreach (string message in result.Messages)
        {
            if (TryReadMessageCount(message, "Rows affected:", out int affected))
            {
                builder.AddRowsAffected(affected);
                continue;
            }

            if (result.Data.Count == 0 && TryReadMessageCount(message, "Rows selected:", out int selected))
            {
                builder.AddRowsReturned(selected);
            }
        }
    }

    private static bool TryReadMessageCount(string message, string prefix, out int count)
    {
        count = 0;
        return message.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)
            && int.TryParse(message[prefix.Length..].Trim(), out count);
    }
}
