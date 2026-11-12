using DataVo.Core.Annotations;

namespace DataVo.Benchmarks;

/// <summary>
/// The row every benchmark uses. Fixed-width columns come before the text column, the layout DataVo's manual
/// recommends, and the same column order is used for the SQLite and DuckDB tables.
/// </summary>
[DataVoTable("players")]
public sealed class Player
{
    [PrimaryKey] public int Id { get; set; }

    public bool Active { get; set; }

    [Columnar] public int Level { get; set; }

    [Columnar] public long Gold { get; set; }

    [Columnar] public double Ratio { get; set; }

    public string Name { get; set; } = "";

    /// <summary>The table as SQLite and DuckDB create it.</summary>
    public const string CreateTableSql =
        "CREATE TABLE players (Id INTEGER PRIMARY KEY, Active BOOLEAN NOT NULL, Level INTEGER NOT NULL, Gold BIGINT NOT NULL, Ratio DOUBLE NOT NULL, Name VARCHAR NOT NULL)";

    /// <summary>The columns, in table order.</summary>
    public const string Columns = "Id, Active, Level, Gold, Ratio, Name";

    /// <summary>
    /// Row <paramref name="id"/> of the data set. The same for every engine and every run: levels are spread
    /// evenly over 0..99 in no particular order, so a condition on <c>Level</c> selects a known share of rows that
    /// are scattered through the table.
    /// </summary>
    public static Player Create(int id)
    {
        uint mixed = unchecked((uint)id * 2654435761u);
        return new Player
        {
            Id = id,
            Active = (mixed & 1) == 0,
            Level = (int)(mixed % 100),
            Gold = (long)(mixed % 2_000_001) - 1_000_000,
            Ratio = (mixed % 10_007) / 10_007.0,
            Name = "player-" + id.ToString(System.Globalization.CultureInfo.InvariantCulture),
        };
    }
}
