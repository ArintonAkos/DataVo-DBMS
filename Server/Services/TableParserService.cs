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

        public static Dictionary<string, List<string>>? ParseColumns(string rawColumns, TableService tableService)
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

        public static Tuple<TableDetail, string, string> ParseJoinStatement(string joinStatement)
        {
            var parts = joinStatement.Split(new string[] { " ", "=" }, StringSplitOptions.RemoveEmptyEntries);
            
            var tableName = parts[1];
            var tableAlias = parts[3];
            var conditionColumn1 = parts[5];
            var conditionColumn2 = parts[6];

            TableDetail tableDetail = new(tableName, tableAlias);

            return Tuple.Create(tableDetail, conditionColumn1, conditionColumn2);
        }

        public static Tuple<Dictionary<string, TableDetail>, List<Tuple<string, string>>> ParseJoinTablesAndConditions(string? joinStatement)
        {
            if (joinStatement is null)
            {
                return new(new(), new());
            }

            Dictionary<string, TableDetail> tableAliases = new();
            List<Tuple<string, string>> conditions = new();

            string[] joins = joinStatement.Split("\n");

            foreach (var join in joins)
            {
                if (join.Length < 5)
                {
                    continue;
                }

                var result = ParseJoinStatement(join);

                tableAliases.Add(result.Item1.GetTableNameInUse(), result.Item1);
                conditions.Add(new(result.Item2, result.Item3));
            }

            return Tuple.Create(tableAliases, conditions);
        }
    }
}
