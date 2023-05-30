using System.Text.RegularExpressions;
using Server.Logging;
using Server.Models.Catalog;
using Server.Models.DQL;
using Server.Parser.Actions;
using Server.Server.Cache;
using Server.Server.Requests.Controllers.Parser;
using Server.Server.Responses.Parts;

namespace Server.Parser.DQL;

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

            List<string>? selectedIds = null;

            if (_model.WhereStatement.IsEvaluatable())
            {
                selectedIds = _model.WhereStatement.Evaluate(_model.TableService, _model.JoinStatement).ToList();
            }

            Dictionary<string, Dictionary<string, dynamic>> data =
                Context.SelectFromTable(selectedIds, _model.Columns, _model.TableName, _databaseName);

            Logger.Info($"Rows selected: {data.Count}");
            Messages.Add($"Rows selected: {data.Count}");

            Fields = CreateFieldsFromColumns(_databaseName);

            Data = data
                .Select(row => row.Value.Values.ToList())
                .ToList();
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }

    private List<FieldResponse> CreateFieldsFromColumns(string databaseName)
    {
        if (_model.Columns.Count > 0)
        {
            return Catalog.GetTableColumns(_model.TableName, databaseName)
                .Select(column => column.Name)
                .Where(column => _model.Columns.Contains(column))
                .Select(columnName => new FieldResponse()
                {
                    FieldName = columnName
                })
                .ToList();
        }
    
        return Catalog.GetTableColumns(_model.TableName, databaseName)
            .Select(col => col.Name)
            .Select(columnName => new FieldResponse()
            {
                FieldName = columnName
            })
            .ToList();
    }
}