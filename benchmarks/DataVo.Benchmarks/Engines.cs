using System.Data;
using DataVo.Core;
using DataVo.Core.Runtime.Reactive;
using DataVo.Core.StorageEngine.Config;
using DuckDB.NET.Data;
using Microsoft.Data.Sqlite;

namespace DataVo.Benchmarks;

/// <summary>Where a benchmark's database lives.</summary>
public enum Storage
{
    /// <summary>Entirely in memory: DataVo's in-memory engine, SQLite <c>:memory:</c>, DuckDB <c>:memory:</c>.</summary>
    Memory,

    /// <summary>In a file: DataVo's LSM engine, SQLite in WAL mode, a DuckDB database file.</summary>
    File,
}

/// <summary>A scratch directory for one benchmark's database files, removed when the benchmark ends.</summary>
public sealed class ScratchDirectory : IDisposable
{
    public ScratchDirectory() => Directory.CreateDirectory(Path);

    public string Path { get; } = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"datavo_bdn_{Guid.NewGuid():N}");

    public void Dispose()
    {
        if (Directory.Exists(Path))
        {
            Directory.Delete(Path, recursive: true);
        }
    }
}

/// <summary>Opens each engine with an empty <c>players</c> table and loads it the way that engine loads in bulk.</summary>
public static class Engines
{
    private static readonly ReactiveRowSchema Schema = new("Id", "Active", "Level", "Gold", "Ratio", "Name");

    public static DataVoContext OpenDataVo(Storage storage, string directory)
    {
        var db = new DataVoContext(new DataVoConfig
        {
            StorageMode = storage == Storage.Memory ? StorageMode.InMemory : StorageMode.Lsm,
            DiskStoragePath = System.IO.Path.Combine(directory, "datavo"),
        });
        Check(db, "CREATE DATABASE Bench");
        Check(db, "USE Bench");
        db.EnsurePlayerTable();
        return db;
    }

    /// <summary>Inserts rows <paramref name="first"/>..<paramref name="last"/> as one typed batch.</summary>
    public static void Load(DataVoContext db, int first, int last)
    {
        var rows = new List<CellValue[]>(last - first + 1);
        for (int id = first; id <= last; id++)
        {
            Player player = Player.Create(id);
            rows.Add(
            [
                CellValue.From(player.Id),
                CellValue.From(player.Active),
                CellValue.From(player.Level),
                CellValue.From(player.Gold),
                CellValue.From(player.Ratio),
                CellValue.From(player.Name),
            ]);
        }

        db.InsertTypedBatch("players", Schema, rows);
    }

    public static SqliteConnection OpenSqlite(Storage storage, string directory)
    {
        string source = storage == Storage.Memory ? ":memory:" : System.IO.Path.Combine(directory, "sqlite.db");
        var connection = new SqliteConnection($"Data Source={source};Pooling=False");
        connection.Open();
        if (storage == Storage.File)
        {
            Execute(connection, "PRAGMA journal_mode=WAL");
            Execute(connection, "PRAGMA synchronous=NORMAL");
        }

        Execute(connection, Player.CreateTableSql);
        return connection;
    }

    /// <summary>Inserts rows <paramref name="first"/>..<paramref name="last"/> with one prepared statement in one transaction.</summary>
    public static void Load(SqliteConnection connection, int first, int last)
    {
        using SqliteTransaction transaction = connection.BeginTransaction();
        using SqliteCommand insert = connection.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText = $"INSERT INTO players ({Player.Columns}) VALUES ($id, $active, $level, $gold, $ratio, $name)";
        SqliteParameter id = insert.Parameters.Add("$id", SqliteType.Integer);
        SqliteParameter active = insert.Parameters.Add("$active", SqliteType.Integer);
        SqliteParameter level = insert.Parameters.Add("$level", SqliteType.Integer);
        SqliteParameter gold = insert.Parameters.Add("$gold", SqliteType.Integer);
        SqliteParameter ratio = insert.Parameters.Add("$ratio", SqliteType.Real);
        SqliteParameter name = insert.Parameters.Add("$name", SqliteType.Text);
        insert.Prepare();
        for (int row = first; row <= last; row++)
        {
            Player player = Player.Create(row);
            id.Value = player.Id;
            active.Value = player.Active;
            level.Value = player.Level;
            gold.Value = player.Gold;
            ratio.Value = player.Ratio;
            name.Value = player.Name;
            insert.ExecuteNonQuery();
        }

        transaction.Commit();
    }

    public static DuckDBConnection OpenDuckDb(Storage storage, string directory)
    {
        string source = storage == Storage.Memory ? ":memory:" : System.IO.Path.Combine(directory, "duck.db");
        var connection = new DuckDBConnection($"Data Source={source}");
        connection.Open();
        if (Environment.GetEnvironmentVariable("DATAVO_BENCH_DUCKDB_THREADS") is { Length: > 0 } threads)
        {
            Execute(connection, $"SET threads = {int.Parse(threads, System.Globalization.CultureInfo.InvariantCulture)}");
        }

        Execute(connection, Player.CreateTableSql);
        return connection;
    }

    /// <summary>Inserts rows <paramref name="first"/>..<paramref name="last"/> through DuckDB's appender.</summary>
    public static void Load(DuckDBConnection connection, int first, int last)
    {
        using DuckDBAppender appender = connection.CreateAppender("players");
        for (int row = first; row <= last; row++)
        {
            Player player = Player.Create(row);
            appender.CreateRow()
                .AppendValue(player.Id)
                .AppendValue(player.Active)
                .AppendValue(player.Level)
                .AppendValue(player.Gold)
                .AppendValue(player.Ratio)
                .AppendValue(player.Name)
                .EndRow();
        }
    }

    /// <summary>Reads the current row of a <c>SELECT Id, Active, Level, Gold, Ratio, Name</c> into a model.</summary>
    public static Player Read(IDataRecord row) => new()
    {
        Id = row.GetInt32(0),
        Active = row.GetBoolean(1),
        Level = row.GetInt32(2),
        Gold = row.GetInt64(3),
        Ratio = row.GetDouble(4),
        Name = row.GetString(5),
    };

    public static void Execute(IDbConnection connection, string sql)
    {
        using IDbCommand command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private static void Check(DataVoContext db, string sql)
    {
        var result = db.Execute(sql)[^1];
        if (result.IsError)
        {
            throw new InvalidOperationException($"{sql}: {string.Join(" | ", result.Messages)}");
        }
    }
}
