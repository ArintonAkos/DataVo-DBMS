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
                List<BsonDocument> values = ProcessTableRows();

                DbContext.Instance.InsertIntoTable(values, _model.TableName, "University");

                Messages.Add($"Rows affected {values.Count}!");
                Logger.Info($"Rows affected {values.Count}!");
            }
            catch (Exception e)
            {
                Messages.Add(e.Message);
                Logger.Error(e.Message);
            }
        }

        private List<BsonDocument> ProcessTableRows()
        {
            List<string> primaryKeys = Catalog.GetTablePrimaryKeys(_model.TableName, "University");
            List<Column> columns = Catalog.GetTableColumnTypes(_model.Columns, _model.TableName, "University");
            List<BsonDocument> bsonData = new();

            int rowNumber = 0;

            foreach (var row in _model.Rows)
            {
                bool invalidRow = false;
                string id = string.Empty;
                string data = string.Empty;
                
                rowNumber++;

                for (int i = 0; i < row.Count; ++i)
                {
                    Column tableColumn = columns[i];
                    tableColumn.Value = row[i].Replace("'", "");

                    if (tableColumn.ParsedValue == null)
                    {
                        invalidRow = true;
                        Messages.Add($"Type of argument doesn't match with column type in row {rowNumber}!");
                        Logger.Error($"Type of argument doesn't match with column type in row {rowNumber}!");
                        break;
                    }

                    if (primaryKeys.Contains(_model.Columns[i]))
                    {
                        id += tableColumn.ParsedValue + "#";
                        continue;
                    }

                    data += tableColumn.ParsedValue + "#";
                }

                if (!invalidRow)
                {
                    BsonDocument bsonDoc = new()
                    {
                        new BsonElement("_id", id.Remove(id.Length - 1)),
                        new BsonElement("columns", data.Remove(data.Length - 1))
                    };

                    bsonData.Add(bsonDoc);
                }
            }

            return bsonData;
        }
    }
}
