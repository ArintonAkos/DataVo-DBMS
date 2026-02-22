using DataVo.Core.Contracts;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Parser.Utils;

namespace DataVo.Core.Parser;

public class QueryEngine
{
    public QueryEngine(string query, Guid session)
    {
        Query = query;
        Session = session;
    }

    private string Query { get; }
    private Guid Session { get; }

    public List<QueryResult> Parse()
    {
        List<QueryResult> response = new();

        List<Queue<IDbAction>> runnables = new();
        try
        {
            var lexer = new Lexer(Query);
            var tokens = lexer.Tokenize();
            var parser = new DataVo.Core.Parser.Parser(tokens);
            var statements = parser.Parse();
            var evaluator = new Evaluator(statements);
            runnables = evaluator.ToRunnables();
        }
        catch (Exception ex)
        {
            response.Add(QueryResult.Error(ex.Message));
            return response;
        }

        foreach (Queue<IDbAction> runnable in runnables)
        {
            while (runnable.Any())
            {
                try
                {
                    response.Add(runnable.Dequeue().Perform(Session));
                }
                catch (Exception ex)
                {
                    response.Add(QueryResult.Error(ex.Message));
                    break;
                }
            }
        }

        return response;
    }
}
