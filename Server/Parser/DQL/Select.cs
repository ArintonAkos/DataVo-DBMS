using System.Text.RegularExpressions;
using Server.Logging;
using Server.Models.DQL;
using Server.Parser.Actions;
using Server.Server.Cache;
using Server.Server.Responses.Parts;

namespace Server.Parser.DQL;

internal class Select : BaseDbAction
{
    private readonly SelectModel _model;

    public Select(Match match) => _model = SelectModel.FromMatch(match);

    public override void PerformAction(Guid session)
    {
        string databaseName = CacheStorage.Get(session);
        bool hasMissingColumns = _model.Validate(databaseName);

        if (!_model.JoinStatement.ContainsJoin() && hasMissingColumns)
        {
            throw new Exception("Invalid columns specified");
        }

        try
        {
            List<string>? selectedIds = null;

            if (_model.WhereStatement.IsEvaluatable())
            {
                selectedIds = _model.WhereStatement.Evaluate(_model.TableName, databaseName).ToList();
            }

            Dictionary<string, Dictionary<string, dynamic>> data =
                Context.SelectFromTable(selectedIds, _model.Columns, _model.TableName, databaseName);

            Logger.Info($"Rows selected: {data.Count}");
            Messages.Add($"Rows selected: {data.Count}");

            Data = data.Select(row => row.Value
                .Select(column => new DataResponse
                {
                    FieldName = column.Key,
                    Value = column.Value.ToString(),
                }).ToList()).ToList();
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }
}