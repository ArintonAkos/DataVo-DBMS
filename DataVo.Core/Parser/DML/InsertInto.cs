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

            var tableColumnNameSet = tableColumns
                .Select(column => column.Name)
                .ToHashSet();
            var primaryKeySet = primaryKeys.ToHashSet();
            var uniqueKeySet = uniqueKeys.ToHashSet();
            var foreignKeysByAttribute = foreignKeys
                .GroupBy(foreignKey => foreignKey.AttributeName)
                .ToDictionary(group => group.Key, group => group.First());

            int rowNumber = 0;
            int rowsAffected = 0;

            bool hasColumns = _model.Columns.Count > 0;
            var insertColumnToIndex = hasColumns
                ? _model.Columns
                    .Select((columnName, index) => new { columnName, index })
                    .ToDictionary(entry => entry.columnName, entry => entry.index)
                : null;

            foreach (var columnName in _model.Columns)
            {
                if (!tableColumnNameSet.Contains(columnName))
                {
                    throw new Exception($"Column {columnName} doesn't exist in table {_model.TableName}!");
                }
            }

            foreach (var rawRow in _model.RawRows)
            {
                if (!hasColumns && rawRow.Count != tableColumns.Count)
                {
                    throw new Exception("The number of values provided in a row must be the same as " +
                                        "the number of columns in the table when columns are not specified.");
                }

                if (hasColumns && rawRow.Count != _model.Columns.Count)
                {
                    throw new Exception("The number of values provided in a row must be the same as " +
                                        "the number of columns provided inside the paranthesis after the table name attribute.");
                }

                bool invalidRow = false;
                var idParts = new List<string>(primaryKeys.Count);
                var rowDict = new Dictionary<string, dynamic>();

                rowNumber++;

                for (int i = 0; i < tableColumns.Count; i++)
                {
                    Column tableColumn = tableColumns[i];

                    string rawValue;
                    if (hasColumns)
                    {
                        if (insertColumnToIndex!.TryGetValue(tableColumn.Name, out int colIndex))
                        {
                            rawValue = rawRow[colIndex];
                        }
                        else
                        {
                            rawValue = "null";
                        }
                    }
                    else
                    {
                        rawValue = rawRow[i];
                    }

                    tableColumn.Value = rawValue.Replace("'", "");
                    dynamic? parsedValue = tableColumn.ParsedValue;

                    if (parsedValue == null && rawValue.ToLowerInvariant() != "null")
                    {
                        invalidRow = true;
                        Messages.Add($"Type of argument doesn't match with column type in row {rowNumber}!");
                        Logger.Error($"Type of argument doesn't match with column type in row {rowNumber}!");
                        break;
                    }

                    rowDict[tableColumn.Name] = parsedValue!;

                    if (uniqueKeySet.Contains(tableColumn.Name) &&
                        IndexManager.Instance.IndexContainsRow(tableColumn.Value, $"_UK_{tableColumn.Name}", _model.TableName, databaseName)
                    )
                    {
                        invalidRow = true;
                        Messages.Add($"Unique key violation in row {rowNumber}!");
                        Logger.Error($"Unique key violation in row {rowNumber}!");
                        break;
                    }

                    if (foreignKeysByAttribute.TryGetValue(tableColumn.Name, out ForeignKey? foreignKey))
                    {
                        if (!CheckForeignKeyConstraint(foreignKey, tableColumn.Value, databaseName))
                        {
                            invalidRow = true;
                            Messages.Add($"Foreign key violation in row {rowNumber}!");
                            Logger.Error($"Foreign key violation in row {rowNumber}!");
                            break;
                        }
                    }

                    if (primaryKeySet.Contains(tableColumn.Name))
                    {
                        idParts.Add(parsedValue?.ToString() ?? string.Empty);
                    }
                }

                if (!invalidRow)
                {
                    string id = string.Join("#", idParts);

                    if (primaryKeys.Count != 0 && IndexManager.Instance.IndexContainsRow(id, $"_PK_{_model.TableName}", _model.TableName, databaseName))
                    {
                        Messages.Add($"Primary key violation in row {rowNumber}!");
                        Logger.Error($"Primary key violation in row {rowNumber}!");
                        continue;
                    }

                    MakeInsertion(rowDict, indexFiles, databaseName);

                    rowsAffected++;
                }
            }

            return rowsAffected;
        }

        private void MakeInsertion(Dictionary<string, dynamic> rowDict, List<IndexFile> indexFiles, string databaseName)
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
