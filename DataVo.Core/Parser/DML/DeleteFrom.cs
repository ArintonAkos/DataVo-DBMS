using System.Text.RegularExpressions;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DML;
using DataVo.Core.Parser.Actions;
using DataVo.Core.BTree;
using DataVo.Core.Cache;
using DataVo.Core.StorageEngine;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.DML;

internal class DeleteFrom(DeleteFromStatement ast) : BaseDbAction
{
    private readonly DeleteFromModel _model = DeleteFromModel.FromAst(ast);

    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            List<long> toBeDeleted = _model.WhereStatement.EvaluateWithoutJoin(_model.TableName, databaseName).ToList();
            
            // Delete entries from the main table
            StorageContext.Instance.DeleteFromTable(toBeDeleted, _model.TableName, databaseName);

            // Delete entries from all indexes
            Catalog.GetTableIndexes(_model.TableName, databaseName)
                .Select(e => e.IndexFileName)
                .ToList()
                .ForEach(indexFile =>
                {
                    IndexManager.Instance.DeleteFromIndex(toBeDeleted, indexFile, _model.TableName, databaseName);
                });

            // Logger.Info($"Rows affected: {toBeDeleted.Count}");
            Messages.Add($"Rows affected: {toBeDeleted.Count}");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }
}