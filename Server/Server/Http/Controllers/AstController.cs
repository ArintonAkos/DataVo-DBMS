using System;
using DataVo.Core.Exceptions;
using DataVo.Core.Parser;
using Server.Server.Http.Attributes;
using Server.Server.Requests;
using Server.Server.Responses;

namespace Server.Server.Http.Controllers;

[Route("Ast")]
public class AstController
{
    [Route("Parse")]
    [Method("POST")]
    public static Response ParseSql(AstRequest request)
    {
        try
        {
            var lexer = new Lexer(request.Data);
            var tokens = lexer.Tokenize();
            var parser = new Parser(tokens);
            var statements = parser.Parse();
            return new AstResponse(statements);
        }
        catch (LexerException le)
        {
            return new ErrorResponse(le);
        }
        catch (ParserException pe)
        {
            return new ErrorResponse(pe);
        }
        catch (Exception ex)
        {
            return new ErrorResponse(ex);
        }
    }
}
