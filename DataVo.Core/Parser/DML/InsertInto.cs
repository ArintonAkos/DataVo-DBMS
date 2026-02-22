using DataVo.Core.Logging;
using DataVo.Core.Models.Catalog;
using DataVo.Core.Models.DML;
using DataVo.Core.Parser.Actions;
using DataVo.Core.BTree;
using DataVo.Core.Cache;
using DataVo.Core.StorageEngine;
using System.Text.RegularExpressions;
using DataVo.Core.Parser.AST;

namespace DataVo.Core.Parser.DML
{
    internal class InsertInto : BaseDbAction
    {
        private readonly InsertIntoModel _model;

        public InsertInto(Match match)
        {
            _model = InsertIntoModel.FromMatch(match);
        }

        public InsertInto(InsertIntoStatement ast)
        {
            _model = InsertIntoModel.FromAst(ast);
        }

        public override void PerformAction(Guid session)
        {
            try
            {
                string databaseName = CacheStorage.Get(session)
                    ?? throw new Exception("No database in use!");

                int rowsAffected = ProcessAndInsertTableRows(databaseName);

                Messages.Add($"Rows affected: {rowsAffected}");
                // Logger.Info($"Rows affected: {rowsAffected}");
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

            List<Dictionary<string, dynamic>> insertData = [];

            int rowNumber = 0;
            int rowsAffected = 0;

            bool hasColumns = _model.Columns.Count > 0;

            foreach (var rawRow in _model.RawRows)
            {
                if (!hasColumns && rawRow.Count != tableColumns.Count)
                {
                    throw new Exception("The number of values provided in a row must be the same as " +
                                        "the number of columns in the table when columns are not specified.");
                }

                bool invalidRow = false;
                string id = string.Empty;
                var rowDict = new Dictionary<string, dynamic>();

                rowNumber++;

                for (int i = 0; i < tableColumns.Count; i++)
                {
                    Column tableColumn = tableColumns[i];

                    string rawValue;
                    if (hasColumns)
                    {
                        int colIndex = _model.Columns.IndexOf(tableColumn.Name);
                        if (colIndex == -1)
                        {
                            // Column not specified in INSERT, use null or default
                            rawValue = "null";
                        }
                        else
                        {
                            rawValue = rawRow[colIndex];
                        }
                    }
                    else
                    {
                        rawValue = rawRow[i];
                    }

                    tableColumn.Value = rawValue.Replace("'", "");

                    if (tableColumn.ParsedValue == null && rawValue.ToLowerInvariant() != "null")
                    {
                        invalidRow = true;
                        Messages.Add($"Type of argument doesn't match with column type in row {rowNumber}!");
                        Logger.Error($"Type of argument doesn't match with column type in row {rowNumber}!");
                        break;
                    }

                    rowDict[tableColumn.Name] = tableColumn.ParsedValue;

                    if (uniqueKeys.Contains(tableColumn.Name) &&
                        IndexManager.Instance.IndexContainsRow(tableColumn.Value, $"_UK_{tableColumn.Name}", _model.TableName, databaseName)
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
                    }
                }

                if (!invalidRow)
                {
                    if (!string.IsNullOrEmpty(id)) id = id.Remove(id.Length - 1);

                    if (primaryKeys.Count != 0 && IndexManager.Instance.IndexContainsRow(id, $"_PK_{_model.TableName}", _model.TableName, databaseName))
                    {
                        Messages.Add($"Primary key violation in row {rowNumber}!");
                        Logger.Error($"Primary key violation in row {rowNumber}!");
                        continue;
                    }

                    MakeInsertion(rowDict, indexFiles, tableColumns, databaseName);

                    rowsAffected++;
                }
            }

            return rowsAffected;
        }

        private void MakeInsertion(Dictionary<string, dynamic> rowDict, List<IndexFile> indexFiles, List<Column> tableColumns, string databaseName)
        {
            long assignedRowId = StorageContext.Instance.InsertOneIntoTable(rowDict, _model.TableName, databaseName);

            foreach (var index in indexFiles)
            {
                string indexValue = string.Empty;
                foreach (var indexAttribute in index.AttributeNames)
                {
                    indexValue += rowDict[indexAttribute] + "##";
                }
                indexValue = indexValue.Remove(indexValue.Length - 2, 2);

                IndexManager.Instance.InsertIntoIndex(indexValue, assignedRowId.ToString(), index.IndexFileName, _model.TableName, databaseName);
            }
        }

        private bool CheckForeignKeyConstraint(ForeignKey foreignKey, string columnValue, string databaseName)
        {
            foreach (var reference in foreignKey.References)
            {
                if (!IndexManager.Instance.IndexContainsRow(columnValue, $"_PK_{reference.ReferenceTableName}", reference.ReferenceTableName, databaseName))
                {
                    return false;
                }
            }

            return true;
        }
    }
}
