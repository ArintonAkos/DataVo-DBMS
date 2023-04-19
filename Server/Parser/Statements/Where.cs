using MongoDB.Bson;
using Server.Models.Catalog;
using Server.Models.Statement;

namespace Server.Parser.Statements
{
    internal class Where
    {
        private readonly WhereModel _model;

        public Where(string match)
        {
            _model = WhereModel.FromString(match);
        }

        public List<string> Evaluate(List<string> primaryKeys, List<Column> tableColumns, List<BsonDocument> tableData)
        {
            List<Dictionary<string, dynamic>> dataDictionary = ParseTableData(primaryKeys, tableColumns, tableData);
            List<string> rowIds = tableData.Select(e => e.GetElement("_id").Value.AsString).ToList();
            List<string> matchingRows = new();

            for (int i = 0; i < dataDictionary.Count; i++)
            {
                Node statementInstance = _model.Statement;

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
