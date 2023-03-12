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

        public void ListDbs()
        {
            var dbList = ListDatabases().ToList();

            Console.WriteLine("The list of databases on this server is: ");
            foreach (var db in dbList)
            {
                Console.WriteLine(db);
            }
        }
    }
}
