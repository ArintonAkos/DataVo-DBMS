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

        public static Tuple<Dictionary<string, TableDetail>, List<string>> ParseJoinTablesAndConditions(IEnumerable<string> joinTables, IEnumerable<string> joinConditions)
        {
            Dictionary<string, TableDetail> tableAliases = new();
            List<string> conditions = new();

            int i = 0;
            foreach (var joinTable in joinTables)
            {
                var tableWithAlias = ParseTableWithAlias(joinTable);
                string tableName = tableWithAlias.Item1;
                string? tableAlias = tableWithAlias.Item2;

                tableAliases.Add(tableAlias ?? tableName, new TableDetail(tableName, tableAlias));

                conditions.Add(joinConditions.ElementAt(i));
                i++;
            }

            return Tuple.Create(tableAliases, conditions);
        }


    }
}
