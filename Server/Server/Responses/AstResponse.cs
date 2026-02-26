using System.Collections.Generic;
using DataVo.Core.Parser.AST;
using Server.Server.Responses;

namespace Server.Server.Responses;

public class AstResponse : Response
{
    public AstResponse(List<SqlStatement> statements)
    {
        Data = statements;
    }
}
