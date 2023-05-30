using System.Text.RegularExpressions;
using Server.Logging;
using Server.Models.Catalog;
using Server.Models.DQL;
using Server.Parser.Actions;
using Server.Server.Cache;
using Server.Server.Requests.Controllers.Parser;
using Server.Server.Responses.Parts;

namespace Server.Parser.Commands;

internal class Describe : BaseDbAction
{
    private readonly DescribeModel _model;

    public Describe(Match match, ParseRequest request) => _model = DescribeModel.FromMatch(match);

    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            Catalog.GetTableColumns(_model.TableName, databaseName)
            .ForEach(column => Fields.Add(new FieldResponse
            {
                FieldName = column.Name,
            }));
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }
}