using Server.Server.Http.Attributes;
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

        var parser = new Parser.Parser(request);

        return parser.Parse();
    }
}