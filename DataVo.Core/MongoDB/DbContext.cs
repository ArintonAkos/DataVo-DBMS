using MongoDB.Bson;
using MongoDB.Driver;
using DataVo.Core.Models.Catalog;

namespace DataVo.Core.MongoDB;

internal class DbContext
{
    private readonly MongoClient _client;

    public DbContext(string connectionString)
    {
        _client = new MongoClient(connectionString);
    }

    private static DbContext? _instance;
    public static DbContext Instance
    {
        get
        {
            if (_instance == null)
            {
                // Default fallback for old benchmarks that rely on the parameterless instancing
                _instance = new DbContext("mongodb://localhost:27017/");
            }
            return _instance;
        }
    }

    public async void CreateTable(string tableName, string databaseName)
    {
        var database = _client.GetDatabase(databaseName);
        await database.CreateCollectionAsync(tableName);
    }

    public async void DropTable(string tableName, string databaseName)
    {
        var database = _client.GetDatabase(databaseName);
        await database.DropCollectionAsync(tableName);
    }



    public void InsertOneIntoTable(BsonDocument value, string tableName, string databaseName)
    {
        try
        {
            var database = _client.GetDatabase(databaseName);
            IMongoCollection<BsonDocument>? table = database.GetCollection<BsonDocument>(tableName);
            table.InsertOne(value);
        }
        catch (Exception)
        {
            throw new Exception("Insert operation failed, mongodb threw an exception!");
        }
    }

    public void InsertIntoTable(List<BsonDocument> values, string tableName, string databaseName)
    {
        if (values.Count == 0)
        {
            return;
        }

        try
        {
            var database = _client.GetDatabase(databaseName);
            IMongoCollection<BsonDocument>? table = database.GetCollection<BsonDocument>(tableName);
            table.InsertMany(values);
        }
        catch (Exception)
        {
            throw new Exception("Insert operation failed, mongodb threw an exception!");
        }
    }

    public async void DeleteFormTable(List<string> toBeDeletedIds, string tableName, string databaseName)
    {
        var database = _client.GetDatabase(databaseName);
        IMongoCollection<BsonDocument>? table = database.GetCollection<BsonDocument>(tableName);
        FilterDefinition<BsonDocument>? filter = Builders<BsonDocument>.Filter.In("_id", toBeDeletedIds);

        await table.DeleteManyAsync(filter);
    }

    public HashSet<string> FilterUsingPrimaryKey(string columnValue, int columnIndex, string tableName,
        string databaseName)
    {
        var database = _client.GetDatabase(databaseName);
        IMongoCollection<BsonDocument>? table = database.GetCollection<BsonDocument>(tableName);

        string regex = "^";

        for (int i = 0; i < columnIndex; ++i)
        {
            regex += "[^#]+#";
        }

        regex += $"{columnValue}(#.*$|$)";

        FilterDefinition<BsonDocument>? filter = Builders<BsonDocument>.Filter.Regex("_id", regex);

        return table.Find(filter)
            .ToList()
            .Select(doc => doc.GetElement("_id").Value.AsString)
            .ToHashSet();
    }

    public bool TableContainsRow(string rowId, string tableName, string databaseName)
    {
        var database = _client.GetDatabase(databaseName);
        IMongoCollection<BsonDocument>? table = database.GetCollection<BsonDocument>(tableName);
        FilterDefinition<BsonDocument>? filter = Builders<BsonDocument>.Filter.Eq("_id", rowId);

        return table.Find(filter).Any();
    }

    public Dictionary<string, Dictionary<string, dynamic>> SelectFromTable(List<string>? ids, List<string> columns,
        string tableName, string databaseName)
    {
        if (!columns.Any())
        {
            columns = Catalog.GetTableColumns(tableName, databaseName).Select(c => c.Name).ToList();
        }

        Dictionary<string, Dictionary<string, dynamic>> selectedData = GetTableContents(ids, tableName, databaseName);

        foreach (Dictionary<string, dynamic> row in selectedData.Values)
        {
            var keysToRemove = row.Keys.Except(columns).ToList();
            foreach (string keyToRemove in keysToRemove)
            {
                row.Remove(keyToRemove);
            }
        }

        return selectedData;
    }

    public Dictionary<string, Dictionary<string, dynamic>> GetTableContents(string tableName, string databaseName)
    {
        return GetTableContents(null, tableName, databaseName);
    }

    public Dictionary<string, Dictionary<string, dynamic>> GetTableContents(List<string>? ids, string tableName, string databaseName)
    {
        List<string> primaryKeys = Catalog.GetTablePrimaryKeys(tableName, databaseName);
        List<Column> tableColumns = Catalog.GetTableColumns(tableName, databaseName);
        List<BsonDocument> bsonData = GetStoredData(ids, tableName, databaseName);

        Dictionary<string, Dictionary<string, dynamic>> parsedTableData = new();

        foreach (var data in bsonData)
        {
            string[] primaryKeyValues = data.GetElement("_id").Value.AsString.Split("#");
            string[] columnValues = data.GetElement("columns").Value.AsString.Split("#");
            Dictionary<string, dynamic> row = new();

            int primaryKeyIdx = 0;
            int columnValueIdx = 0;
            foreach (var column in tableColumns)
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

    private List<BsonDocument> GetStoredData(List<string>? ids, string tableName, string databaseName)
    {
        var database = _client.GetDatabase(databaseName);
        IMongoCollection<BsonDocument>? table = database.GetCollection<BsonDocument>(tableName);

        if (ids != null && ids.Count == 0)
        {
            return new();
        }

        var filter = ids != null  
            ? Builders<BsonDocument>.Filter.In("_id", ids)
            : Builders<BsonDocument>.Filter.Empty;

        return table.Find(filter).ToList();
    }

    public void DropDatabase(string database)
    {
        _client.DropDatabase(database);
    }
}