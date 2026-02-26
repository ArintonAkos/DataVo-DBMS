using DataVo.Core.Enums;
using DataVo.Core.Exceptions;
using DataVo.Core.Models.Statement;
using DataVo.Core.Parser.Types;

namespace DataVo.Core.Parser.Statements.JoinStrategies;

internal class FullJoinStrategy : IJoinStrategy
{
    public string JoinType => JoinTypes.FULL;

    public HashedTable Execute(HashedTable leftRows, JoinModel.JoinCondition? condition, JoinStrategyContext context)
    {
        throw new EvaluationException("JOIN strategy 'FULL' is not implemented yet.");
    }
}
