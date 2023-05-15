using System.Text.RegularExpressions;
using Server.Logging;
using Server.Models.Catalog;
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
        try
        {
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            bool hasMissingColumns = _model.Validate(databaseName);

            if (!_model.JoinStatement.ContainsJoin() && hasMissingColumns)
            {
                throw new Exception("Invalid columns specified'");
            }

            List<string>? selectedIds = null;

            if (_model.WhereStatement.IsEvaluatable())
            {
                selectedIds = _model.WhereStatement.Evaluate(_model.TableName, databaseName).ToList();
            }

            Dictionary<string, Dictionary<string, dynamic>> data =
                Context.SelectFromTable(selectedIds, _model.Columns, _model.TableName, databaseName);

            Logger.Info($"Rows selected: {data.Count}");
            Messages.Add($"Rows selected: {data.Count}");

            Fields = CreateFieldsFromColumns(databaseName);

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