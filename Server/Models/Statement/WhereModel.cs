using MongoDB.Bson;
using Server.Models.Catalog;
using Server.Parser.Statements;

namespace Server.Models.Statement
{
    internal class WhereModel
    {
        private Node Statement { get; set; }

        public static WhereModel FromString(string value)
        {
            value = value.Remove(0, 5);
            Node statement = StatementParser.Parse(value);

            return new WhereModel
            {
                Statement = statement
            };
        }

        public List<string> Evaluate(List<string> primaryKeys, List<Column> tableColumns, List<BsonDocument> tableData)
        {
            List<Dictionary<string, dynamic>> dataDictionary = ParseTableData(primaryKeys, tableColumns, tableData);
            List<string> rowIds = tableData.Select(e => e.GetElement("_id").Value.AsString).ToList();
            List<string> matchingRows = new();

            for (int i = 0; i < dataDictionary.Count; i++)
            {
                Node statementInstance = Statement;
                
                if (StatementEvaluator.Evaluate(statementInstance, dataDictionary[i]))
                {
                    matchingRows.Add(rowIds[i]);
                }
            }

            return matchingRows;
        }

        private List<Dictionary<string, dynamic>> ParseTableData(List<string> primaryKeys, List<Column> tableColumns, List<BsonDocument> tableData)
        {
            List<Dictionary<string, dynamic>> parsedTableData = new();

            foreach (BsonDocument data in tableData)
            {
                string[] primaryKeyValues = data.GetElement("_id").Value.AsString.Split("#");
                string[] columnValues = data.GetElement("columns").Value.AsString.Split("#");
                Dictionary<string, dynamic> row = new();

                int primaryKeyIdx = 0;
                int columnValueIdx = 0;
                foreach (Column column in tableColumns)
                {
                    if (primaryKeys.Contains(column.Name))
                    {
                        row[column.Name] = primaryKeyValues[primaryKeyIdx++];
                    }
                    else
                    {
                        row[column.Name] = columnValues[columnValueIdx++];
                    }
                }

                parsedTableData.Add(row);
            }

            return parsedTableData;
        }
    }
}
