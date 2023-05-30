using Server.Models.Statement.Utils;

namespace Server.Services
{
    public class TableService
    {
        private readonly string _databaseName;
        public Dictionary<string, TableDetail> TableDetails { get; private set; } = new();

        public TableService(string databaseName)
        {
            _databaseName = databaseName;
        }

        public TableDetail GetTableDetailByAliasOrName(string aliasOrName)
        {
            foreach (var tableDetail in TableDetails.Values)
            {
                if (tableDetail.GetTableNameInUse() == aliasOrName)
                {
                    return tableDetail;
                }
            }

            throw new Exception("Table name or alias not found");
        }

        public string GetRealTableName(string aliasOrName)
        {
            return GetTableDetailByAliasOrName(aliasOrName).TableName;
        }

        public void AddTableDetail(TableDetail tableDetail)
        {
            if (TableDetails.ContainsKey(tableDetail.TableName))
            {
                throw new Exception("Duplicate table name found");
            }

            if (tableDetail.TableAlias != null && TableDetails.ContainsKey(tableDetail.TableAlias))
            {
                throw new Exception("Duplicate table alias found");
            }

            tableDetail.DatabaseName = _databaseName;

            TableDetails[tableDetail.GetTableNameInUse()] = tableDetail;
        }
    }
}
