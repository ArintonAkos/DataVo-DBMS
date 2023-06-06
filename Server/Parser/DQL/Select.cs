using System.Text.RegularExpressions;
using MongoDB.Driver;
using Server.Logging;
using Server.Models.DQL;
using Server.Parser.Actions;
using Server.Parser.Types;
using Server.Server.Cache;
using Server.Server.Responses.Parts;
using Server.Utils;

namespace Server.Parser.DQL;

internal class Select : BaseDbAction
{
    private readonly SelectModel _model;

    public Select(Match match)
    {
        _model = SelectModel.FromMatch(match);
    }

    public override void PerformAction(Guid session)
    {
        try
        {
            string database = ValidateDatabase(session);

            if (!_model.JoinStatement.ContainsJoin())
            {
                ValidateColumns(database);
            }

            ListedTable result = EvaluateStatements();

            GroupedTable groupedTable = GroupResults(result);

            result = AggregateGroupedTable(groupedTable);

            Logger.Info($"Rows selected: {result.Count}");
            Messages.Add($"Rows selected: {result.Count}");

            Fields = CreateFieldsFromColumns();

            Data = CreateDataFromResult(result);
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }

    private GroupedTable GroupResults(ListedTable tableData)
    {
        return _model.GroupByStatement.Evaluate(tableData);
    }

    private ListedTable AggregateGroupedTable(GroupedTable groupedTable)
    {
        return _model.AggregateStatement.Perform(groupedTable);
    }

    private string ValidateDatabase(Guid session)
    {
        string databaseName = CacheStorage.Get(session)
            ?? throw new Exception("No database in use!");

        bool hasMissingColumns = _model.Validate(databaseName);

        if (!_model.JoinStatement.ContainsJoin() && hasMissingColumns)
        {
            throw new Exception("Invalid columns specified'");
        }

        return databaseName;
    }

    private void ValidateColumns(string databaseName)
    {
        if (_model.Validate(databaseName))
        {
            throw new Exception("Invalid columns specified'");
        }
    }

    private ListedTable EvaluateStatements()
    {
        ListedTable result;

        if (_model.WhereStatement.IsEvaluatable())
        {
            result = _model.WhereStatement.EvaluateWithJoin(_model.TableService!, _model.JoinStatement);
        }
        else if (_model.JoinStatement.ContainsJoin())
        {
            result = EvaluateJoin();
        }
        else
        {
            var listResult = _model.FromTable!.TableContentValues!
                .Select(row => new JoinedRow(_model.FromTable.TableName, row.ToRow()))
                .ToList();

            result = new ListedTable(listResult);
        }

        return result;
    }

    private ListedTable EvaluateJoin()
    {
        HashedTable groupedInitialTable = new();

        foreach (var row in _model.FromTable.TableContent!)
        {
            groupedInitialTable.Add(row.Key, new JoinedRow(_model.FromTable.TableName, row.Value.ToRow()));
        }

        return _model.JoinStatement!.Evaluate(groupedInitialTable).ToListedTable();
    }


    private List<FieldResponse> CreateFieldsFromColumns()
    {
        List<string> selectedColumns = _model.GetSelectedColumns();
        List<FieldResponse> fields = new();
        
        foreach (string column in selectedColumns)
        {
            string[] splittedColumn = column.Split('.');
            string tableName = splittedColumn[0];
            string columnName = splittedColumn[1];

            string inUseNameOfTable = _model.TableService!.GetTableDetailByAliasOrName(tableName).GetTableNameInUse();

            fields.Add(new()
            {
                FieldName = $"{inUseNameOfTable}.{columnName}",
            });
        }

        return fields;
    }

    private List<List<dynamic>> CreateDataFromResult(ListedTable filteredTable)
    {
        List<List<dynamic>> result = new();

        foreach (var row in filteredTable)
        {
            List<dynamic> data = new();
            foreach (string nameAssembly in _model.GetSelectedColumns())
            {
                string[] splittedAssembly = nameAssembly.Split('.');
                string tableName = splittedAssembly[0];
                string columnName = splittedAssembly[1];

                data.Add(row[tableName][columnName]);
            }

            result.Add(data);
        }

        return result;
    }
}