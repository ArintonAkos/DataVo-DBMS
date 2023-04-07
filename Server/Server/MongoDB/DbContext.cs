using MongoDB.Bson;
using MongoDB.Driver;

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

        public async void CreateTable(String tableName, String databaseName)
        {
            var database = GetDatabase(databaseName);
            await database.CreateCollectionAsync(tableName);
        }

        public async void DropTable(String tableName, String databaseName)
        {
            var database = GetDatabase(databaseName);
            await database.DropCollectionAsync(tableName);
        }

        public void InsertIntoTable(List<BsonDocument> values, String tableName, String databaseName)
        {
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

        public async void DeleteFormTable(List<String> toBeDeletedIds, String tableName, String databaseName)
        {
            if (toBeDeletedIds.Count == 0)
            {
                return;
            }

            var database = GetDatabase(databaseName);
            var table = database.GetCollection<BsonDocument>(tableName);
            var filter = Builders<BsonDocument>.Filter.Empty;
            toBeDeletedIds.ForEach(id =>
            {
                var additionalFilter = Builders<BsonDocument>.Filter.Eq("_id", id);
                filter |= additionalFilter; 
            });
            await table.DeleteManyAsync(filter);
        }

        public List<BsonDocument> GetStoredData(String tableName, String databaseName)
        {
            var database = GetDatabase(databaseName);
            var table = database.GetCollection<BsonDocument>(tableName);
            return table.Find(Builders<BsonDocument>.Filter.Empty)
                .ToList();
        }
    }
}
