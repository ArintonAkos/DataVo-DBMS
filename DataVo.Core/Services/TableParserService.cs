namespace DataVo.Core.Services
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
    }
}
