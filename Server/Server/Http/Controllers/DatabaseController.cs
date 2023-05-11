using Server.Models.Catalog;
using Server.Server.Http.Attributes;
using Server.Server.MongoDB;
using Server.Server.Responses.Controllers.Database;
using Server.Server.Responses.Parts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Server.Server.Http.Controllers;

[Route("database")]
public class DatabaseController
{
    [Method("GET")]
    [Route("list")]
    public static DatabaseListResponse ListDatabases()
    {
        return new()
        {
            Data = Catalog.GetDatabases().Select(database => new DatabaseModel
            {
                DatabaseName = database,
                Tables = GetTables(database)
            }).ToList()
        };
    }

    private static List<DatabaseModel.Table> GetTables(string databaseName)
    {
        var tables = new List<DatabaseModel.Table>();
        var tableNames = Catalog.GetTables(databaseName);

        foreach (var table in tableNames)
        {
            tables.Add(new()
            {
                Name = table,
                Columns = Catalog.GetTableColumns(table, databaseName),
                ForeignKeys = Catalog.GetTableForeignKeys(table, databaseName),
                UniqueKeys = Catalog.GetTableUniqueKeys(table, databaseName),
                IndexFiles = Catalog.GetTableIndexes(table, databaseName)
            });
        }

        return tables;
    }
}