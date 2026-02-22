using System.Text.RegularExpressions;
using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DDL;
using DataVo.Core.Parser.Actions;
using DataVo.Core.BTree;
using DataVo.Core.Cache;
using DataVo.Core.StorageEngine;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.DDL;

internal class CreateIndex : BaseDbAction
{
    private readonly CreateIndexModel _model;

    public CreateIndex(Match match) => _model = CreateIndexModel.FromMatch(match);
    public CreateIndex(CreateIndexStatement ast) => _model = CreateIndexModel.FromAst(ast);

    public override void PerformAction(Guid session)
    {
        try
        {
            string databaseName = CacheStorage.Get(session)
                ?? throw new Exception("No database in use!");

            Catalog.CreateIndex(_model.ToIndexFile(), _model.TableName, databaseName);

            var tableDataRows = StorageContext.Instance.GetTableContents(_model.TableName, databaseName);
            Dictionary<string, Dictionary<string, dynamic>> tableData = [];
            foreach (var r in tableDataRows)
            {
                tableData[r.Key.ToString()] = r.Value;
            }

            Dictionary<string, List<string>> indexValues = CreateIndexContents(tableData);

            IndexManager.Instance.CreateIndex(indexValues, _model.IndexName, _model.TableName, databaseName);

            Logger.Info($"New index file {_model.IndexName} successfully created!");
            Messages.Add($"New index file {_model.IndexName} successfully created!");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }

    private Dictionary<string, List<string>> CreateIndexContents(Dictionary<string, Dictionary<string, dynamic>> tableData)
    {
        Dictionary<string, List<string>> indexContentDict = [];

        foreach (KeyValuePair<string, Dictionary<string, dynamic>> row in tableData)
        {
            string? key = string.Empty;

            foreach (KeyValuePair<string, dynamic> col in row.Value)
            {
                if (_model.Attributes.Contains(col.Key))
                {
                    key += col.Value + "##";
                }
            }

            key = key.Remove(key.Length - 2, count: 2);

            if (indexContentDict.ContainsKey(key))
            {
                indexContentDict[key].Add(row.Key);
            }
            else
            {
                indexContentDict.Add(key, [row.Key]);
            }
        }

        return indexContentDict;
    }
}