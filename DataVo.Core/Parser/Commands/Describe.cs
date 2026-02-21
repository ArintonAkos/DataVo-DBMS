using System.Text.RegularExpressions;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DQL;
using DataVo.Core.Parser.Actions;
using DataVo.Core.Cache;

namespace DataVo.Core.Parser.Commands;

internal class Describe : BaseDbAction
{
    private readonly DescribeModel _model;

    public Describe(Match match) => _model = DescribeModel.FromMatch(match);

    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

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