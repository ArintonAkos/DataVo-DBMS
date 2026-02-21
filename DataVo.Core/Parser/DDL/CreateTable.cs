using System.Text.RegularExpressions;
using DataVo.Core.Contracts.Results;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DDL;
using DataVo.Core.Parser.Actions;
using DataVo.Core.BTree;
using DataVo.Core.Cache;
using DataVo.Core.MongoDB;

namespace DataVo.Core.Parser.DDL;

internal class CreateTable : BaseDbAction
{
    private readonly CreateTableModel _model;

    public CreateTable(Match match) => _model = CreateTableModel.FromMatch(match);

    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            Catalog.CreateTable(_model.ToTable(), databaseName);

            DbContext.Instance.CreateTable(_model.TableName, databaseName);

            List<string> uniqueKeys = Catalog.GetTableUniqueKeys(_model.TableName, databaseName);
            uniqueKeys.ForEach(key =>
            {
                IndexManager.Instance.CreateIndex(new Dictionary<string, List<string>>(), $"_UK_{key}", _model.TableName, databaseName);
            });

            Logger.Info($"New table {_model.TableName} successfully created!");
            Messages.Add($"Table {_model.TableName} successfully created!");
        }
        catch (Exception e)
        {
            Logger.Error(e.Message);
            Messages.Add(e.Message);
        }
    }
}