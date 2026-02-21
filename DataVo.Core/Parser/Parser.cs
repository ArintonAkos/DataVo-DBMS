using DataVo.Core.Contracts;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Parser.Utils;
using DataVo.Core.Contracts.Results;

namespace DataVo.Core.Parser;

public class Parser
{
    public Parser(string query, Guid session)
    {
        Query = query;
        Session = session;
    }

    private string Query { get; }
    private Guid Session { get; }

    public List<QueryResult> Parse()
    {
        List<QueryResult> response = new();

        List<Queue<IDbAction>> runnables = RequestMapper.ToRunnables(Query);

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
