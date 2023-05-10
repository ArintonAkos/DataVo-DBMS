using Server.Parser.Utils;
using Server.Server.Requests.Controllers.Parser;
using Server.Server.Responses;
using Server.Server.Responses.Controllers.Parser;

namespace Server.Parser;

internal class Parser
{
    public Parser(ParseRequest request)
    {
        Request = request;
    }

    private ParseRequest Request { get; }

    public ParseResponse Parse()
    {
        ParseResponse response = new();

        var runnables = RequestMapper.ToRunnables(Request);

        foreach (var runnable in runnables)
        {
            ScriptResponse scriptResponse = new();

            while (runnable.Any())
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

            response.Data.Add(scriptResponse);
        }

        return response;
    }
}