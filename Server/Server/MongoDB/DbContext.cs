using MongoDB.Bson;
using MongoDB.Driver;
using Server.Models.Catalog;

namespace Server.Server.MongoDB;

internal class DbContext : MongoClient
{
    private static DbContext _instance = null!;

    private DbContext() : base("mongodb://localhost:27017/")
    { }

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

    public async void CreateIndex(List<BsonDocument> values, string indexName, string tableName, string databaseName)
    {
        var database = GetDatabase(databaseName);

        string indexTableName = $"{tableName}_{indexName}_index";
        await database.CreateCollectionAsync(indexTableName);

        InsertIntoTable(values, indexTableName, databaseName);
    }

    public async void DropIndex(string indexName, string tableName, string databaseName)
    {
        var database = GetDatabase(databaseName);
        string indexTableName = $"{tableName}_{indexName}_index";
        await database.DropCollectionAsync(indexTableName);
    }

    public void InsertOneIntoTable(BsonDocument value, string tableName, string databaseName)
    {
        try
        {
            var database = GetDatabase(databaseName);
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
            var database = GetDatabase(databaseName);
            IMongoCollection<BsonDocument>? table = database.GetCollection<BsonDocument>(tableName);
            table.InsertMany(values);
        }
        catch (Exception)
        {
            throw new Exception("Insert operation failed, mongodb threw an exception!");
        }
    }

    public void InsertIntoIndex(string value, string rowId, string indexName, string tableName, string databaseName)
    {
        string indexTableName = $"{tableName}_{indexName}_index";

        var database = GetDatabase(databaseName);
        IMongoCollection<BsonDocument>? table = database.GetCollection<BsonDocument>(indexTableName);
        FilterDefinition<BsonDocument>? filter = Builders<BsonDocument>.Filter.Eq("_id", value);

        BsonDocument newBsonDoc = new()
        {
            new BsonElement("_id", value),
        };

        var currentValue = table.Find(filter).FirstOrDefault();
        if (currentValue == null)
        {
            newBsonDoc.Add("columns", rowId);
            table.InsertOne(newBsonDoc);

            return;
        }

        string newColumns = currentValue.GetElement("columns").Value.AsString + "##" + rowId;
        newBsonDoc.Add(new BsonElement("columns", newColumns));
        table.ReplaceOne(filter, newBsonDoc);
    }

    public async void DeleteFormTable(List<string> toBeDeletedIds, string tableName, string databaseName)
    {
        var database = GetDatabase(databaseName);
        IMongoCollection<BsonDocument>? table = database.GetCollection<BsonDocument>(tableName);
        FilterDefinition<BsonDocument>? filter = Builders<BsonDocument>.Filter.In("_id", toBeDeletedIds);

        await table.DeleteManyAsync(filter);
    }

    public void DeleteFromIndex(List<string> toBeDeletedIds, string indexName, string tableName, string databaseName)
    {
        string indexTableName = $"{tableName}_{indexName}_index";

        var database = GetDatabase(databaseName);
        IMongoCollection<BsonDocument>? table = database.GetCollection<BsonDocument>(indexTableName);

        var indexData = table.Find(Builders<BsonDocument>.Filter.Empty).ToList();
        foreach (var indexRow in indexData)
        {
            var columns = indexRow.GetElement("columns").Value.AsString.Split("##").ToList();
            bool needsUpdate = false;

            toBeDeletedIds.ForEach(id =>
            {
                if (!columns.Contains(id))
                {
                    return;
                }

                columns.Remove(id);
                needsUpdate = true;
            });

            if (needsUpdate)
            {
                FilterDefinition<BsonDocument>? filter =
                    Builders<BsonDocument>.Filter.Eq("_id", indexRow.GetElement("_id").Value.AsString);

                if (columns.Count == 0)
                {
                    table.DeleteOne(filter);
                    return;
                }

                string columnString = string.Join("##", columns);
                UpdateDefinition<BsonDocument>? update = Builders<BsonDocument>.Update.Set("columns", columnString);
                table.UpdateOne(filter, update);
            }
        }
    }

    public HashSet<string> FilterUsingPrimaryKey(string columnValue, int columnIndex, string tableName,
        string databaseName)
    {
        var database = GetDatabase(databaseName);
        IMongoCollection<BsonDocument>? table = database.GetCollection<BsonDocument>(tableName);

        string regex = "^";

        for (int i = 0; i < columnIndex; ++i)
        {
            regex += "[^#]+#";
        }

        regex += $"{columnValue}(.*$|$)";

        FilterDefinition<BsonDocument>? filter = Builders<BsonDocument>.Filter.Regex("_id", regex);

        return table.Find(filter)
            .ToList()
            .Select(doc => doc.GetElement("_id").Value.AsString)
            .ToHashSet();
    }

    public HashSet<string> FilterUsingIndex(string columnValue, string indexName, string tableName, string databaseName)
    {
        string indexTableName = $"{tableName}_{indexName}_index";
        var database = GetDatabase(databaseName);
        IMongoCollection<BsonDocument>? table = database.GetCollection<BsonDocument>(indexTableName);

        FilterDefinition<BsonDocument>? filter = Builders<BsonDocument>.Filter.Eq("_id", columnValue);

        HashSet<string> result = new();

        var values = table.Find(filter)
            .ToList();

        values.ForEach(doc =>
        {
            string[] values = doc.GetElement("columns").Value.AsString.Split("##");
            result.UnionWith(values);
        });

        return result;
    }

    public bool TableContainsRow(string rowId, string tableName, string databaseName)
    {
        var database = GetDatabase(databaseName);
        IMongoCollection<BsonDocument>? table = database.GetCollection<BsonDocument>(tableName);
        FilterDefinition<BsonDocument>? filter = Builders<BsonDocument>.Filter.Eq("_id", rowId);

        return table.Find(filter).Any();
    }

    public bool IndexContainsRow(string rowId, string indexName, string tableName, string databaseName)
    {
        string indexTableName = $"{tableName}_{indexName}_index";

        return TableContainsRow(rowId, indexTableName, databaseName);
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
        var database = GetDatabase(databaseName);
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
}