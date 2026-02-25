using DataVo.Core.Contracts;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.Commands;

internal class Go : IDbAction
{
    public Go(GoStatement _) { }
    public QueryResult Perform(Guid session) => QueryResult.Default();
}
