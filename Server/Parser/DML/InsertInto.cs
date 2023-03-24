using MongoDB.Bson;
using Server.Logging;
using Server.Models.Catalog;
using Server.Models.DML;
using Server.Parser.Actions;
using Server.Server.MongoDB;
using System.Text.RegularExpressions;

namespace Server.Parser.DML
{
    internal class InsertInto : BaseDbAction
    {
        private readonly InsertIntoModel _model;

        public InsertInto(Match match)
        {
            _model = InsertIntoModel.FromMatch(match);
        }

        public override void PerformAction(Guid session)
        {
            try
            {
                BsonDocument bsonData = ProcessTableRows();

                DbContext.Instance.InsertIntoTable(bsonData, _model.TableName, "University");

                Messages.Add($"Successfully inserted values into table {_model.TableName}!");
                Logger.Info($"Successfully inserted values into table {_model.TableName}!");
            }
            catch (Exception e)
            {
                Messages.Add(e.Message);
                Logger.Error(e.Message);
            }
        }

        private BsonDocument ProcessTableRows()
        {
            List<string> primaryKeys = Catalog.GetTablePrimaryKeys(_model.TableName, "University");
            List<Column> columns = Catalog.GetTableColumnTypes(_model.TableName, "University");
            BsonDocument bsonData = new();

            foreach (var row in _model.Rows)
            {
                string id = string.Empty;
                string data = string.Empty;
                for (int i = 0; i < row.Count; ++i)
                {
                    Column tableColumn = columns[i];
                    tableColumn.Value = row[i].Replace("'", "");

                    if (tableColumn.ParsedValue == null)
                    {
                        throw new Exception("Argument types does not match!");
                    }

                    if (primaryKeys.Contains(_model.Columns[i]))
                    {
                        id += tableColumn.ParsedValue + "#";
                        continue;
                    }

                    data += tableColumn.ParsedValue + "#";
                }

                bsonData.Add(new BsonElement("_id", id.Remove(id.Length - 1)));
                bsonData.Add(new BsonElement("columns", data.Remove(data.Length - 1)));
            }

            return bsonData;
        }
    }
}
