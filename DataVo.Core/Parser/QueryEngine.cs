using DataVo.Core.Contracts;
using DataVo.Core.Contracts.Results;

namespace DataVo.Core.Parser;

public class QueryEngine(string query, Guid session)
{
    public List<QueryResult> Parse()
    {
        List<QueryResult> response = [];

        List<Queue<IDbAction>> runnables;
        try
        {
            var lexer = new Lexer(query);
            var tokens = lexer.Tokenize();
            var parser = new Parser(tokens);
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
            while (runnable.Count != 0)
            {
                try
                {
                    response.Add(runnable.Dequeue().Perform(session));
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
