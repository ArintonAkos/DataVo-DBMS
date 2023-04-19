using MongoDB.Bson;
using MongoDB.Driver;
using Server.Models.Catalog;

namespace Server.Server.MongoDB
{
    internal class DbContext : MongoClient
    {
        private DbContext() : base("mongodb://localhost:27017/")
        { }

        private static DbContext _instance;
        
        public static DbContext Instance
        {
            get
            {
                _instance ??= new DbContext();

                return _instance;
            }
        }

        public async void CreateTable(string tableName, string databaseName)
        {
            var database = GetDatabase(databaseName);
            await database.CreateCollectionAsync(tableName);
        }

        public async void DropTable(string tableName, string databaseName)
        {
            var database = GetDatabase(databaseName);
            await database.DropCollectionAsync(tableName);
        }

        public void InsertIntoTable(List<BsonDocument> values, string tableName, string databaseName)
        {
            if (values.Count == 0)
            {
                return;
            }

            try
            {
                var database = GetDatabase(databaseName);
                var table = database.GetCollection<BsonDocument>(tableName);
                table.InsertMany(values);
            }
            catch (Exception)
            {
                throw new Exception("Insert operation failed, mongodb threw an exception!");
            }
        }

        public async void DeleteFormTable(List<string> toBeDeletedIds, string tableName, string databaseName)
        {
            var database = GetDatabase(databaseName);
            var table = database.GetCollection<BsonDocument>(tableName);
            var filter = Builders<BsonDocument>.Filter.In("_id", toBeDeletedIds);           

            await table.DeleteManyAsync(filter);
        }

        public Dictionary<string, Dictionary<string, dynamic>> GetTableContents(string tableName, string databaseName)
        {
            List<string> primaryKeys = Catalog.GetTablePrimaryKeys(tableName, databaseName);
            List<Column> tableColumns = Catalog.GetTableColumns(tableName, databaseName);
            List<BsonDocument> bsonData = GetStoredData(tableName, databaseName);

            Dictionary<string, Dictionary<string, dynamic>> parsedTableData = new();

            foreach (BsonDocument data in bsonData)
            {
                string[] primaryKeyValues = data.GetElement("_id").Value.AsString.Split("#");
                string[] columnValues = data.GetElement("columns").Value.AsString.Split("#");
                Dictionary<string, dynamic> row = new();

                int primaryKeyIdx = 0;
                int columnValueIdx = 0;
                foreach (Column column in tableColumns)
                {
                    if (primaryKeys.Contains(column.Name))
                    {
                        column.Value = primaryKeyValues[primaryKeyIdx++];
                    }
                    else
                    {
                        column.Value = columnValues[columnValueIdx++];
                    }

                    row[column.Name] = column.ParsedValue!;
                }

                parsedTableData[data.GetElement("_id").Value.AsString] = row;
            }

            return parsedTableData;
        }

        private List<BsonDocument> GetStoredData(string tableName, string databaseName)
        {
            var database = GetDatabase(databaseName);
            var table = database.GetCollection<BsonDocument>(tableName);
            return table.Find(Builders<BsonDocument>.Filter.Empty).ToList();
        }
    }
}
