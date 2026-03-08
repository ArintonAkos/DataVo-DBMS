using System.Text.RegularExpressions;
using DataVo.Core.Logging;
using DataVo.Core.Models.DQL;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.Commands;

internal class Describe(DescribeStatement ast) : BaseDbAction
{
    private readonly DescribeModel _model = DescribeModel.FromAst(ast);

    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = GetDatabaseName(session);

            Catalog.GetTableColumns(_model.TableName, databaseName)
            .ForEach(column => Fields.Add(column.Name));
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }
}