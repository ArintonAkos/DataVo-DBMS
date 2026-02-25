using System.Text.RegularExpressions;
using DataVo.Core.Models.DQL;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Cache;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.Commands;

internal class Use(UseStatement ast) : BaseDbAction
{
    private readonly UseModel _model = UseModel.FromAst(ast);

    public override void PerformAction(Guid session)
    {
        CacheStorage.Set(session, _model.DatabaseName);
    }
}