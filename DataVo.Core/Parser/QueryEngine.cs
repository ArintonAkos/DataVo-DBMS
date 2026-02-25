using DataVo.Core.Contracts;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Logging;

namespace DataVo.Core.Parser;

public class QueryEngine(string query, Guid session)
{
    private static bool ParserDebugEnabled =>
        string.Equals(Environment.GetEnvironmentVariable("DATAVO_PARSER_DEBUG"), "1", StringComparison.OrdinalIgnoreCase)
        || string.Equals(Environment.GetEnvironmentVariable("DATAVO_PARSER_DEBUG"), "true", StringComparison.OrdinalIgnoreCase);

    public List<QueryResult> Parse()
    {
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

            var evaluator = new Evaluator(statements);
            runnables = evaluator.ToRunnables();
        }
        catch (Exception ex)
        {
            if (ParserDebugEnabled)
            {
                Logger.Error($"[ParserDebug] Parse pipeline failure: {ex.GetType().Name}: {ex.Message}");
            }

            response.Add(QueryResult.Error(ex.Message));
            return response;
        }

        foreach (Queue<IDbAction> runnable in runnables)
        {
            while (runnable.Count != 0)
            {
                try
                {
                    response.Add(runnable.Dequeue().Perform(session));
                }
                catch (Exception ex)
                {
                    response.Add(QueryResult.Error(ex.ToString()));
                    break;
                }
            }
        }

        return response;
    }
}
