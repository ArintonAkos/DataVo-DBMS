using Server.Models.Statement.Utils;

namespace Server.Services
{
    internal class TableParserService
    {
        public static Tuple<string, string?> ParseTableWithAlias(string rawString)
        {
            string tableName = rawString;
            string? tableAlias = null;

            if (rawString.ToLower().Contains(" as "))
            {
                var splitString = rawString
                    .Split(" as ")
                    .Select(r => r.Trim())
                    .ToList();

                tableName = splitString[0];
                tableAlias = splitString[1];
            }

            return Tuple.Create(tableName, tableAlias);
        }

        public static Dictionary<string, List<string>>? ParseSelectColumns(string rawColumns, TableService tableService)
        {
            if (!rawColumns.Contains('*'))
            {
                Dictionary<string, List<string>> selectedColumns = new();

                var splitColumns = rawColumns.Split(',');

                foreach (var column in splitColumns)
                {
                    string trimmedColumn = column.Trim();
                    
                    Tuple<string, string> parseResult = tableService.ParseAndFindTableNameByColumn(trimmedColumn);
                    string tableName = parseResult.Item1;
                    string columnName = parseResult.Item2;

                    if (!selectedColumns.ContainsKey(tableName))
                    {
                        selectedColumns[tableName] = new();
                    }

                    selectedColumns[tableName].Add($"{tableName}.{columnName}");
                }

                return selectedColumns;
            }

            return null;
        }

        public static List<Column> ParseGroupByColumns(string rawColumns, TableService tableService)
        {
            List<Column> columns = new();
            string[] splitColumns = rawColumns.Split(',');

            foreach (var rawColumn in splitColumns)
            {
                string trimmedColumn = rawColumn.Trim();
                
                Tuple<string, string> parseResult = tableService.ParseAndFindTableNameByColumn(trimmedColumn);
                Column column = new(parseResult.Item1, parseResult.Item2);
                
                if (!columns.Any(c => c.ColumnName == column.ColumnName && c.TableName == column.TableName))
                {
                    columns.Add(column);
                }
            }

            return columns;
        }

        public static Tuple<TableDetail, string, string> ParseJoinStatement(string joinStatement)
        {
            var parts = joinStatement.Split(new string[] { " ", "=" }, StringSplitOptions.RemoveEmptyEntries);

            if (parts.Contains("as"))
            {
                var tableName = parts[0];
                var tableAlias = parts[2];
                var conditionColumn1 = parts[4];
                var conditionColumn2 = parts[5];

                TableDetail tableDetail = new(tableName, tableAlias);

                return Tuple.Create(tableDetail, conditionColumn1, conditionColumn2);
            }
            else
            {
                var tableName = parts[0];
                var conditionColumn1 = parts[2];
                var conditionColumn2 = parts[3];

                TableDetail tableDetail = new(tableName, null);

                return Tuple.Create(tableDetail, conditionColumn1, conditionColumn2);
            }
        }

        public static Tuple<Dictionary<string, TableDetail>, List<Tuple<string, string>>> ParseJoinTablesAndConditions(string? joinStatement)
        {
            if (joinStatement is null)
            {
                return new(new(), new());
            }

            Dictionary<string, TableDetail> tableAliases = new();
            List<Tuple<string, string>> conditions = new();

            string[] joins = joinStatement.Trim().Split(new string[] { "JOIN", "join" }, StringSplitOptions.RemoveEmptyEntries);

            foreach (var join in joins)
            {
                var result = ParseJoinStatement(join.Trim());

                tableAliases.Add(result.Item1.GetTableNameInUse(), result.Item1);
                conditions.Add(new(result.Item2, result.Item3));
            }

            return Tuple.Create(tableAliases, conditions);
        }
    }
}
