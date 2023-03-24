using MongoDB.Bson;
using MongoDB.Driver;

namespace Server.Server.MongoDB
{
    internal class DbContext : MongoClient
    {
        private DbContext() : base("mongodb://localhost:27017/")
        {
        }

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

        public async void InsertIntoTable(BsonDocument values, String tableName, String databaseName)
        {
            try
            {
                var database = GetDatabase(databaseName);
                var table = database.GetCollection<BsonDocument>(tableName);
                await table.InsertOneAsync(values);
            }
            catch (Exception)
            {
                throw;
            }
        }
    }
}
