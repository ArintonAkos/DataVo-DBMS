using Server.Models.Catalog;
using Server.Models.Statement.Utils;

namespace Server.Services
{
    public class TableColumnService
    {
        public List<TableColumnIdentifier> GetTableColumnIdentifiers(List<string> columns)
        {
            return new();
            //return columns.Select(TableColumnIdentifier.FromFullyQualifiedName).ToList();
        }

        public bool ValidateTableColumnIdentifiers(string databaseName, List<TableColumnIdentifier> identifiers, Dictionary<string, List<Column>> tableColumns, Dictionary<string, string> tableAliases)
        {
            bool hasMissingColumnsSpecified = false;

            //foreach (var identifier in identifiers)
            //{
            //    if (tableAliases.ContainsKey(identifier.TableName) && tableAliases[identifier.TableName] != identifier.ColumnName)
            //    {
            //        throw new Exception($"Invalid table alias: {identifier.TableName}");
            //    }

            //    if (tableColumns.All(c => c.Name != identifier.ColumnName))
            //    {
            //        hasMissingColumnsSpecified = true;
            //    }
            //}

            return hasMissingColumnsSpecified;
        }
    }
}
