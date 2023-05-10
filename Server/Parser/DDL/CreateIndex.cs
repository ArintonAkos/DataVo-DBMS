using System.Text.RegularExpressions;
using MongoDB.Bson;
using Server.Logging;
using Server.Models.Catalog;
using Server.Models.DDL;
using Server.Parser.Actions;
using Server.Server.MongoDB;

namespace Server.Parser.DDL;

internal class CreateIndex : BaseDbAction
{
    private readonly CreateIndexModel _model;

    public CreateIndex(Match match) => _model = CreateIndexModel.FromMatch(match);

    public override void PerformAction(Guid session)
    {
        try
        {
            Catalog.CreateIndex(_model.ToIndexFile(), _model.TableName, "University");

            Dictionary<string, Dictionary<string, dynamic>> tableData =
                DbContext.Instance.GetTableContents(_model.TableName, "University");

            List<BsonDocument> indexValues = CreateIndexContents(tableData);

            DbContext.Instance.CreateIndex(indexValues, _model.IndexName, _model.TableName, "University");

            Logger.Info($"New index file {_model.IndexName} successfully created!");
            Messages.Add($"New index file {_model.IndexName} successfully created!");
        }
        catch (Exception ex)
        {
            Logger.Error(ex.Message);
            Messages.Add(ex.Message);
        }
    }

    private List<BsonDocument> CreateIndexContents(Dictionary<string, Dictionary<string, dynamic>> tableData)
    {
        Dictionary<string, string> indexContentDict = new();
        List<BsonDocument> indexContents = new();

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
                indexContentDict[key] += $"##{row.Key}";
            }
            else
            {
                indexContentDict.Add(key, row.Key);
            }
        }

        foreach (KeyValuePair<string, string> entry in indexContentDict)
        {
            BsonDocument bsonDoc = new()
            {
                new BsonElement("_id", entry.Key),
                new BsonElement("columns", entry.Value),
            };

            indexContents.Add(bsonDoc);
        }

        return indexContents;
    }
}