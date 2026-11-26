// Native AOT smoke test (Phase 1 verification fence).
//
// Exercises the DataVo public surface that must stay AOT/trim-clean: the engine API, the typed insert
// fast lane, the borrowed (zero-alloc) reactive path, and the ADO.NET provider. EF Core is intentionally
// excluded (separate later phase). Built and published with PublishAot; any reflection/dynamic that
// survives trimming surfaces here as an AOT warning (build) or a runtime failure (this program).
//
// Exit code 0 = all checks passed; non-zero = a check failed (printed to stderr).

using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using DataVo.Data;

static void Require(bool condition, string message)
{
    if (!condition)
    {
        Console.Error.WriteLine($"FAIL: {message}");
        Environment.Exit(1);
    }

    Console.WriteLine($"  ok: {message}");
}

Console.WriteLine("DataVo AOT smoke — engine + typed insert + borrowed reactive + ADO.NET + VectorData");

// 1) Engine + typed insert fast lane.
Console.WriteLine("[1] engine + InsertTyped");
using (var ctx = new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory }))
{
    foreach (string sql in new[]
    {
        "CREATE DATABASE SmokeDb", "USE SmokeDb",
        "CREATE TABLE Orders (Id INT, Category VARCHAR(20), Stake INT)",
    })
    {
        Require(!ctx.Execute(sql).Last().IsError, $"execute: {sql}");
    }

    var schema = new ReactiveRowSchema("Id", "Category", "Stake");
    var cells = new CellValue[3];
    void InsertTyped(int id, string category, int stake)
    {
        cells[0] = CellValue.From(id);
        cells[1] = CellValue.From(category);
        cells[2] = CellValue.From(stake);
        ctx.InsertTyped("Orders", schema, cells);
    }

    // 2) Borrowed (zero-alloc) reactive subscription over an aggregate shape.
    Console.WriteLine("[2] SubscribeZeroAlloc (borrowed aggregate)");
    const string groupBySql = "SELECT Category, SUM(Stake) AS Total, COUNT(*) AS Cnt FROM Orders GROUP BY Category";
    long observedTotal = 0;
    int deliveries = 0;
    using (ctx.SubscribeZeroAlloc(groupBySql, (in QueryChangeRef change) =>
    {
        deliveries++;
        for (int i = 0; i < change.Added.Count; i++) observedTotal += change.Added[i]["Total"].AsInt64();
        for (int i = 0; i < change.Updated.Count; i++) observedTotal += change.Updated[i]["Total"].AsInt64();
    }))
    {
        InsertTyped(1, "sports", 100);
        InsertTyped(2, "sports", 50);
        InsertTyped(3, "politics", 25);
        ctx.DispatchPendingNotifications();
    }

    Require(deliveries > 0, $"borrowed reactive callback fired ({deliveries} deliveries)");
    Require(observedTotal > 0, $"borrowed aggregate produced totals ({observedTotal})");

    // 3) Read back via the engine SELECT path.
    Console.WriteLine("[3] SELECT read-back");
    var result = ctx.Execute("SELECT Id, Category, Stake FROM Orders").Last();
    Require(!result.IsError, "select executes");
    Require(result.Data.Count == 3, $"select returns 3 rows (got {result.Data.Count})");
}

// 4) ADO.NET provider: connection, command, reader.
Console.WriteLine("[4] ADO.NET provider");
using (var connection = new DataVoConnection("StorageMode=InMemory;DataSource=SmokeAdo"))
{
    connection.Open();

    using (var create = connection.CreateCommand())
    {
        create.CommandText = "CREATE TABLE Items (Id INT, Name VARCHAR(20));";
        create.ExecuteNonQuery();
    }

    using (var insert = connection.CreateCommand())
    {
        insert.CommandText = "INSERT INTO Items VALUES (1, 'alpha'), (2, 'beta');";
        int affected = insert.ExecuteNonQuery();
        Require(affected == 2, $"insert affected 2 rows (got {affected})");
    }

    using (var select = connection.CreateCommand())
    {
        select.CommandText = "SELECT Id, Name FROM Items;";
        using var reader = select.ExecuteReader();
        int rows = 0;
        while (reader.Read())
        {
            rows++;
            _ = reader.GetInt32(reader.GetOrdinal("Id"));
            _ = reader["Name"]?.ToString();
            _ = reader.GetFieldType(0); // exercises the trim-annotated override
        }

        Require(rows == 2, $"ADO.NET reader returned 2 rows (got {rows})");
    }

    using (var schemaCommand = connection.CreateCommand())
    {
        schemaCommand.CommandText = "SELECT Id, Name FROM Items;";
        using var reader = schemaCommand.ExecuteReader();
        System.Data.DataTable schema = reader.GetSchemaTable();
        Require(schema.Rows.Count == 2 && (Type)schema.Rows[0][System.Data.Common.SchemaTableColumn.DataType] == typeof(int), "reader GetSchemaTable describes the columns");
    }

    using (var tx = connection.BeginTransaction())
    {
        using var insert = connection.CreateCommand();
        insert.CommandText = "INSERT INTO Items VALUES (3, 'gamma');";
        insert.ExecuteNonQuery();
    } // disposed uncommitted: rolled back

    using (var count = connection.CreateCommand())
    {
        count.CommandText = "SELECT COUNT(*) FROM Items;";
        Require(Convert.ToInt64(count.ExecuteScalar()) == 2, "disposing an uncommitted transaction rolls it back");
    }

    Require(System.Data.Common.DbProviderFactories.GetFactory(connection) is DataVoFactory, "DbProviderFactory resolves to DataVoFactory");
    Require(!string.Equals(connection.ServerVersion, "DataVo 1.0", StringComparison.Ordinal), $"ServerVersion from the assembly ({connection.ServerVersion})");

    connection.Close();
}

DataVo.AotSmoke.GeneratedTableSmoke.Run();
DataVo.AotSmoke.VectorDataSmoke.Run();

Console.WriteLine("ALL SMOKE CHECKS PASSED");
return 0;
