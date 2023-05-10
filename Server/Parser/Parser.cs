using Server.Parser.Actions;
using Server.Parser.Utils;
using Server.Server.Requests.Controllers.Parser;
using Server.Server.Responses;
using Server.Server.Responses.Controllers.Parser;

namespace Server.Parser;

internal class Parser
{
    public Parser(ParseRequest request) => Request = request;

    private ParseRequest Request { get; }

    public ParseResponse Parse()
    {
        ParseResponse response = new();

        List<Queue<IDbAction>> runnables = RequestMapper.ToRunnables(Request);

        foreach (Queue<IDbAction> runnable in runnables)
        {
            ScriptResponse scriptResponse = new();

            while (runnable.Any())
            {
                try
                {
                    scriptResponse.Actions.Add(runnable.Dequeue().Perform(Request.Session));
                }
                catch (Exception ex)
                {
                    scriptResponse.Actions.Add(ActionResponse.Error(ex));
                    scriptResponse.IsSuccess = false;
                    break;
                }
            }

            response.Data.Add(scriptResponse);
        }

        return response;
    }
}