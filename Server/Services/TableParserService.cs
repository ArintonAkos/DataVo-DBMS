using Server.Models.Statement;
using Server.Models.Statement.Utils;

namespace Server.Services
{
    internal class TableParserService
    {
        public static Tuple<string, string?> ParseTableWithAlias(string rawString)
        {
            string tableName = rawString;
            string? tableAlias = null;

            if (rawString.ToLower().Contains("as"))
            {
                var splitString = rawString
                    .Split("as")
                    .Select(r => r.Trim())
                    .ToList();

                tableName = splitString[0];
                tableAlias = splitString[1];
            }

            return Tuple.Create(tableName, tableAlias);
        }

        public static Dictionary<string, List<string>>? ParseColumns(string columns, Dictionary<string, List<string>> tableColumns)
        {
            if (!columns.Contains(value: '*'))
            {
                Dictionary<string, List<string>> selectedColumns = new();

                var splitColumns = columns.Split(separator: ',');

                foreach (var column in splitColumns)
                {
                    string trimmedColumn = column.Trim();
                    string[] splitColumn = trimmedColumn.Split(separator: '.');

                    string? tableName = splitColumn.Length > 1 ? splitColumn[0] : null;
                    string columnName = splitColumn.Length > 1 ? splitColumn[1] : trimmedColumn;

                    if (tableName != null)
                    {
                        if (!tableColumns.ContainsKey(tableName))
                        {
                            throw new Exception($"Invalid table name: {tableName}");
                        }

                        if (!tableColumns[tableName].Contains(columnName))
                        {
                            throw new Exception($"Invalid column name: {columnName} for table {tableName}");
                        }

                        selectedColumns[tableName].Add($"{tableName}.{columnName}");
                    }
                    else
                    {
                        List<string> tablesWithThisColumnName = new();

                        foreach (var table in tableColumns)
                        {
                            if (table.Value.Contains(column))
                            {
                                tablesWithThisColumnName.Add(table.Key);
                            }
                        }

                        if (tablesWithThisColumnName.Count > 1)
                        {
                            throw new Exception($"Ambiguous column name: {column}");
                        }

                        if (tablesWithThisColumnName.Count == 0)
                        {
                            throw new Exception($"Invalid column name: {column}");
                        }

                        tableName = tablesWithThisColumnName[0];
                        selectedColumns[tableName].Add($"{tableName}.{columnName}");
                    }
                }

                return selectedColumns;
            }
            else
            {
                return null;
            }
        }

        public static Tuple<TableDetail, JoinModel.JoinColumn, JoinModel.JoinColumn, JoinModel.JoinCondition> ParseJoinStatement(string joinStatement)
        {
            // "JOIN {tableName} as {alias} ON {table1}.{column1} = {alias}.{column2}"
            var parts = joinStatement.Split(new string[] { " ", ".", "=" }, StringSplitOptions.RemoveEmptyEntries);
            
            var tableName = parts[1];
            var tableAlias = parts[3];
            var conditionTable1 = parts[5];
            var conditionColumn1 = parts[6];
            var conditionTable2 = parts[7];
            var conditionColumn2 = parts[8];

            TableDetail tableDetail = new(tableName, tableAlias);
            JoinModel.JoinColumn leftColumn = new(conditionTable1, conditionColumn1);
            JoinModel.JoinColumn rightColumn = new(conditionTable2, conditionColumn2);
            JoinModel.JoinCondition joinCondition = new(leftColumn, rightColumn);

            return Tuple.Create(tableDetail, leftColumn, rightColumn, joinCondition);
        }

        public static Tuple<Dictionary<string, TableDetail>, List<JoinModel.JoinCondition>> ParseJoinTablesAndConditions(string? joinStatement)
        {
            if (joinStatement is null)
            {
                return new(new(), new());
            }

            Dictionary<string, TableDetail> tableAliases = new();
            List<JoinModel.JoinCondition> conditions = new();

            string[] joins = joinStatement.Split("\n");

            foreach (var join in joins)
            {
                if (join.Length < 5)
                {
                    continue;
                }

                var result = ParseJoinStatement(join);

                tableAliases.Add(result.Item1.GetTableNameInUse(), result.Item1);
                conditions.Add(result.Item4);
            }

            return Tuple.Create(tableAliases, conditions);
        }


    }
}
