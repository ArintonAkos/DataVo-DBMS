using System.Text.RegularExpressions;
using Server.Logging;
using Server.Models.Catalog;
using Server.Models.DQL;
using Server.Parser.Actions;
using Server.Parser.Statements;
using Server.Server.Cache;
using Server.Server.Requests.Controllers.Parser;
using Server.Server.Responses.Parts;

namespace Server.Parser.DQL;
using TableRows = List<Dictionary<string, Dictionary<string, dynamic>>>;

internal class Select : BaseDbAction
{
    private readonly SelectModel _model;
    private readonly string _databaseName;

    public Select(Match match, ParseRequest request)
    {
        _databaseName = CacheStorage.Get(request.Session)
                ?? throw new Exception("No database in use!");

        _model = SelectModel.FromMatch(match, _databaseName);
    }

    public override void PerformAction(Guid session)
    {
        try
        {
            bool hasMissingColumns = _model.Validate(_databaseName);

            if (!_model.JoinStatement.ContainsJoin() && hasMissingColumns)
            {
                throw new Exception("Invalid columns specified'");
            }

            TableRows result = new();

            if (_model.WhereStatement.IsEvaluatable())
            {
                result = _model.WhereStatement.EvaluateWithJoin(_model.TableService, _model.JoinStatement);
            }
            else
            {
                result = _model.FromTable!.TableContentValues!
                    .Select(row => new Dictionary<string, Dictionary<string, dynamic>> { { _model.FromTable.TableName, row } })
                    .ToList();

                if (_model.JoinStatement.ContainsJoin())
                {
                    result = _model.JoinStatement.Evaluate(result);
                }
            }

            Logger.Info($"Rows selected: {result.Count}");
            Messages.Add($"Rows selected: {result.Count}");

            Fields = CreateFieldsFromColumns();

            Data = result.ToList();
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }

    private List<FieldResponse> CreateFieldsFromColumns()
    {
        var allColumns = _model.GetSelectedColumns();
    
        return allColumns.Select(columnName => 
            new FieldResponse()
            {
                FieldName = columnName
            })
            .ToList();
    }
}