using Server.Server.Http.Attributes;
using DataVo.Core.Parser;
using Server.Server.Responses;
using Server.Server.Requests.Controllers.Parser;
using Server.Server.Responses.Controllers.Parser;

namespace Server.Server.Http.Controllers;

[Route("parser")]
internal class ParserController
{
    [Method("POST")]
    [Route("parse")]
    public static ParseResponse Parse(ParseRequest request)
    {
        if (request == null)
        {
            throw new Exception("Error while parsing data!");
        }

        var engine = new DataVo.Core.Parser.QueryEngine(request.Data, request.Session);
        
        var response = new ParseResponse();
        var scriptResponse = new ScriptResponse();
        
        foreach (var queryResult in engine.Parse())
        {
            scriptResponse.Actions.Add(ActionResponse.FromQueryResult(queryResult));
            if (queryResult.IsError)
            {
                scriptResponse.IsSuccess = false;
            }
        }
        
        response.Data.Add(scriptResponse);

        return response;
    }
}