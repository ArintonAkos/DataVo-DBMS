using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
    }
}
