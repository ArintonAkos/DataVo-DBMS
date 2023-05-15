using MongoDB.Bson;
using Server.Logging;
using Server.Models.Catalog;
using Server.Models.DML;
using Server.Parser.Actions;
using Server.Server.Cache;
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
                string databaseName = CacheStorage.Get(session)
                    ?? throw new Exception("No database in use!");

                int rowsAffected = ProcessAndInsertTableRows(databaseName);

                Messages.Add($"Rows affected: {rowsAffected}");
                Logger.Info($"Rows affected: {rowsAffected}");
            }
            catch (Exception e)
            {
                Messages.Add(e.Message);
                Logger.Error(e.Message);
            }
        }

        private int ProcessAndInsertTableRows(string databaseName)
        {
            List<string> primaryKeys = Catalog.GetTablePrimaryKeys(_model.TableName, databaseName);
            List<string> uniqueKeys = Catalog.GetTableUniqueKeys(_model.TableName, databaseName);
            List<ForeignKey> foreignKeys = Catalog.GetTableForeignKeys(_model.TableName, databaseName);
            List<IndexFile> indexFiles = Catalog.GetTableIndexes(_model.TableName, databaseName);
            List<Column> tableColumns = Catalog.GetTableColumns(_model.TableName, databaseName);
            
            _model.Columns.ForEach(name =>
            {
                if (!tableColumns.Any(column => column.Name == name))
                {
                    throw new Exception($"Column {name} doesn't exist in table {_model.TableName}!");
                }
            });

            List<BsonDocument> bsonData = new();

            int rowNumber = 0;
            int rowsAffected = 0;

            foreach (var row in _model.Rows)
            {
                bool invalidRow = false;
                string id = string.Empty;
                string data = string.Empty;
                
                rowNumber++;

                foreach (Column tableColumn in tableColumns)
                {
                    tableColumn.Value = row[tableColumn.Name].Replace("'", "");

                    if (tableColumn.ParsedValue == null)
                    {
                        invalidRow = true;
                        Messages.Add($"Type of argument doesn't match with column type in row {rowNumber}!");
                        Logger.Error($"Type of argument doesn't match with column type in row {rowNumber}!");
                        break;
                    }

                    if (uniqueKeys.Contains(tableColumn.Name) && 
                        DbContext.Instance.IndexContainsRow(tableColumn.Value, $"_UK_{tableColumn.Name}", _model.TableName, databaseName)
                    )
                    {
                        invalidRow = true;
                        Messages.Add($"Unique key violation in row {rowNumber}!");
                        Logger.Error($"Unique key violation in row {rowNumber}!");
                        break;
                    }

                    if (foreignKeys.Select(e => e.AttributeName).ToList().Contains(tableColumn.Name))
                    {
                        ForeignKey foreignKey = foreignKeys
                            .Where(e => e.AttributeName == tableColumn.Name)
                            .First();

                        if (!CheckForeignKeyConstraint(foreignKey, tableColumn.Value, databaseName))
                        {
                            invalidRow = true;
                            Messages.Add($"Foreign key violation in row {rowNumber}!");
                            Logger.Error($"Foreign key violation in row {rowNumber}!");
                            break;
                        }
                    }

                    if (primaryKeys.Contains(tableColumn.Name))
                    {
                        id += tableColumn.ParsedValue + "#";
                        continue;
                    }

                    data += tableColumn.ParsedValue + "#";
                }

                if (!invalidRow)
                {
                    if (!string.IsNullOrEmpty(id)) id = id.Remove(id.Length - 1);
                    if (!string.IsNullOrEmpty(data)) data = data.Remove(data.Length - 1);

                    if (DbContext.Instance.TableContainsRow(id, _model.TableName, databaseName))
                    {
                        Messages.Add($"Primary key violation in row {rowNumber}!");
                        Logger.Error($"Primary key violation in row {rowNumber}!");
                        continue;
                    }

                    MakeInsertion(id, data, indexFiles, tableColumns, databaseName);

                    rowsAffected++;
                }
            }

            return rowsAffected;
        }

        private void MakeInsertion(string id, string data, List<IndexFile> indexFiles, List<Column> tableColumns, string databaseName)
        {
            BsonDocument bsonDoc = new()
            {
                new BsonElement("_id", id),
                new BsonElement("columns", data)
            };

            DbContext.Instance.InsertOneIntoTable(bsonDoc, _model.TableName, databaseName);

            foreach (var index in indexFiles)
            {
                string indexValue = string.Empty;
                foreach (var indexAttribute in index.AttributeNames)
                {
                    indexValue += tableColumns
                        .Where(col => col.Name == indexAttribute)
                        .First()
                        .ParsedValue + "##";
                }
                indexValue = indexValue.Remove(indexValue.Length - 2, 2);

                DbContext.Instance.InsertIntoIndex(indexValue, id, index.IndexFileName, _model.TableName, databaseName);
            }
        }

        private bool CheckForeignKeyConstraint(ForeignKey foreignKey, string columnValue, string databaseName)
        {
            foreach (var reference in foreignKey.References)
            {
                if (!DbContext.Instance.TableContainsRow(columnValue, reference.ReferenceTableName, databaseName))
                {
                    return false;
                }
            }

            return true;
        }
    }
}
