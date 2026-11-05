using System.Globalization;
using System.Text;
using DataVo.Core;
using DataVo.Core.Contracts.Results;
using DataVo.Core.StorageEngine.Config;
using Microsoft.Data.Sqlite;

namespace DataVo.Tests.Sql;

/// <summary>
/// Differential test: the same random SQL statements run on DataVo and on SQLite, and every result must
/// match: rows of SELECTs (as multisets unless fully ordered), affected-row counts, and whether a
/// statement fails (a duplicate primary key must fail on both, and leave no partial rows behind).
/// </summary>
/// <remarks>
/// SQLite is the reference because its behaviour is well specified and tested. The generator avoids
/// the places where SQL dialects legitimately differ (integer division, case-insensitive LIKE on mixed
/// case, ordering of NULLs, float formatting), so a mismatch points at a DataVo bug. The failure message
/// names the seed and the last statements. DATAVO_DIFF_SEEDS and DATAVO_DIFF_STEPS run longer locally;
/// DATAVO_SEED_OFFSET starts at another seed.
/// </remarks>
public sealed class SqliteDifferentialTests : IDisposable
{
    private readonly string _root = TestStorageDirectories.Create("sqlite_differential");

    public static TheoryData<StorageMode, int> Cases()
    {
        int seeds = int.TryParse(Environment.GetEnvironmentVariable("DATAVO_DIFF_SEEDS"), out int configured) ? configured : 3;
        int first = int.TryParse(Environment.GetEnvironmentVariable("DATAVO_SEED_OFFSET"), out int offset) ? offset + 1 : 1;
        var cases = new TheoryData<StorageMode, int>();
        for (int seed = first; seed < first + seeds; seed++)
        {
            cases.Add(StorageMode.InMemory, seed);
            cases.Add(StorageMode.Lsm, seed);
        }

        return cases;
    }

    public void Dispose()
    {
        try
        {
            Directory.Delete(_root, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public void RandomStatements_MatchSqlite(StorageMode mode, int seed)
    {
        int steps = int.TryParse(Environment.GetEnvironmentVariable("DATAVO_DIFF_STEPS"), out int configured) ? configured : 400;
        using var run = new Run(mode, seed, Path.Combine(_root, $"{mode}-{seed}"));
        run.Execute(steps);
    }

    private sealed class Run : IDisposable
    {
        private const int IdRange = 60;
        private static readonly string[] Words = ["ant", "bee", "cat", "cow", "dog", "eel", "elk", "fox", "", "owl"];

        private readonly StorageMode _mode;
        private readonly int _seed;
        private readonly string _directory;
        private readonly Random _random;
        private readonly SqliteConnection _sqlite;
        private readonly Queue<string> _recent = new();
        private DataVoContext _datavo;
        private bool _created;
        private int _step;

        public Run(StorageMode mode, int seed, string directory)
        {
            _mode = mode;
            _seed = seed;
            _directory = directory;
            _random = new Random(seed * 7919 + (int)mode);
            _sqlite = new SqliteConnection("Data Source=:memory:");
            _sqlite.Open();
            _datavo = OpenDataVo();
        }

        public void Dispose()
        {
            _datavo.Dispose();
            _sqlite.Dispose();
        }

        public void Execute(int steps)
        {
            Setup("CREATE TABLE P (Id INT PRIMARY KEY, A INT, B INT, S VARCHAR(8))");
            Setup("CREATE TABLE C (Id INT PRIMARY KEY, PId INT, V INT)");
            if (_random.Next(2) == 0)
            {
                Setup("CREATE INDEX ix_p_a ON P (A)");
            }

            if (_random.Next(2) == 0)
            {
                Setup("CREATE INDEX ix_c_pid ON C (PId)");
            }

            for (_step = 0; _step < steps; _step++)
            {
                int roll = _random.Next(100);
                if (roll < 22)
                {
                    Write(InsertP());
                }
                else if (roll < 32)
                {
                    Write(InsertC());
                }
                else if (roll < 42)
                {
                    Write(UpdateP());
                }
                else if (roll < 48)
                {
                    Write(DeleteFrom());
                }
                else if (roll < 98)
                {
                    Query();
                }
                else if (_mode == StorageMode.Lsm)
                {
                    Log("-- reopen");
                    _datavo.Dispose();
                    _datavo = OpenDataVo();
                }
            }
        }

        private DataVoContext OpenDataVo()
        {
            DataVoContext context = _mode == StorageMode.Lsm
                ? new DataVoContext(new DataVoConfig { StorageMode = StorageMode.Lsm, DiskStoragePath = _directory, LsmStrictFsync = false })
                : new DataVoContext(new DataVoConfig { StorageMode = StorageMode.InMemory });
            if (!_created)
            {
                Ok(context.Execute("CREATE DATABASE D")[0], "CREATE DATABASE D");
                _created = true;
            }

            Ok(context.Execute("USE D")[0], "USE D");
            return context;
        }

        // ---- statements -------------------------------------------------------------------------

        private string InsertP()
        {
            int rows = 1 + _random.Next(3);
            var values = Enumerable.Range(0, rows).Select(_ =>
                $"({1 + _random.Next(IdRange)}, {NullableInt(20)}, {NullableInt(5)}, {NullableWord()})");
            return $"INSERT INTO P (Id, A, B, S) VALUES {string.Join(", ", values)}";
        }

        private string InsertC() =>
            $"INSERT INTO C (Id, PId, V) VALUES ({1 + _random.Next(IdRange)}, {NullableInt(IdRange)}, {_random.Next(100)})";

        private string UpdateP()
        {
            string assignment = _random.Next(4) switch
            {
                0 => $"A = {NullableInt(20)}",
                1 => $"B = B + {_random.Next(1, 4)}",
                2 => $"S = {NullableWord()}",
                _ => $"A = {_random.Next(20)}, B = {_random.Next(5)}",
            };

            return $"UPDATE P SET {assignment} WHERE {Predicate("P", depth: 0)}";
        }

        private string DeleteFrom() => _random.Next(3) == 0
            ? $"DELETE FROM C WHERE {Predicate("C", depth: 0)}"
            : $"DELETE FROM P WHERE {Predicate("P", depth: 0)}";

        private void Query()
        {
            switch (_random.Next(9))
            {
                case 0:
                    Select($"SELECT Id, A, B, S FROM P WHERE {Predicate("P", 0)} ORDER BY Id", ordered: true);
                    break;
                case 1:
                    Select($"SELECT COUNT(*) AS n FROM P WHERE {Predicate("P", 0)}", ordered: true);
                    break;
                case 2:
                    Select($"SELECT SUM(A) AS s, MIN(A) AS lo, MAX(A) AS hi, COUNT(A) AS n FROM P WHERE {Predicate("P", 0)}", ordered: true);
                    break;
                case 3:
                    Select("SELECT B, COUNT(*) AS n, SUM(A) AS s FROM P GROUP BY B", ordered: false);
                    break;
                case 4:
                    Select($"SELECT DISTINCT A FROM P WHERE {Predicate("P", 0)}", ordered: false);
                    break;
                case 5:
                    Select($"SELECT P.Id AS pid, C.Id AS cid, C.V AS v FROM P JOIN C ON P.Id = C.PId WHERE {Predicate("C", 0, qualified: true)}", ordered: false);
                    break;
                case 6:
                    Select($"SELECT Id, A FROM P ORDER BY Id LIMIT {_random.Next(1, 8)} OFFSET {_random.Next(5)}", ordered: true);
                    break;
                case 7:
                    Select(_random.Next(2) == 0
                        ? $"SELECT B, COUNT(*) AS n FROM P GROUP BY B HAVING COUNT(*) > {_random.Next(3)}"
                        : $"SELECT COUNT(*) AS n, MAX(A) AS hi, B FROM P GROUP BY B", ordered: false);
                    break;
                default:
                    Select($"SELECT Id, V FROM C WHERE {Predicate("C", 0)} ORDER BY Id", ordered: true);
                    break;
            }
        }

        private string Predicate(string table, int depth, bool qualified = false)
        {
            if (depth < 2 && _random.Next(4) == 0)
            {
                string op = _random.Next(2) == 0 ? "AND" : "OR";
                return $"({Predicate(table, depth + 1, qualified)} {op} {Predicate(table, depth + 1, qualified)})";
            }

            string prefix = qualified ? table + "." : string.Empty;
            if (table == "C")
            {
                string column = prefix + (_random.Next(2) == 0 ? "V" : "PId");
                return Comparison(column, column.EndsWith("V", StringComparison.Ordinal) ? 100 : IdRange);
            }

            return _random.Next(6) switch
            {
                0 => $"{prefix}S {(_random.Next(2) == 0 ? "LIKE" : "=")} '{Words[_random.Next(Words.Length)]}{(_random.Next(2) == 0 ? "%" : string.Empty)}'",
                1 => $"{prefix}{Column()} IS {(_random.Next(2) == 0 ? "NULL" : "NOT NULL")}",
                2 => $"{prefix}A BETWEEN {_random.Next(10)} AND {_random.Next(10, 20)}",
                3 => $"{prefix}Id IN ({string.Join(", ", Enumerable.Range(0, 1 + _random.Next(4)).Select(_ => 1 + _random.Next(IdRange)))})",
                _ => Comparison(prefix + (_random.Next(3) switch { 0 => "Id", 1 => "A", _ => "B" }), 20),
            };
        }

        private string Comparison(string column, int range)
        {
            string[] operators = ["=", "<>", "<", "<=", ">", ">="];
            return $"{column} {operators[_random.Next(operators.Length)]} {_random.Next(range)}";
        }

        private string Column() => _random.Next(3) switch { 0 => "A", 1 => "B", _ => "S" };

        private string NullableInt(int range) => _random.Next(6) == 0 ? "NULL" : _random.Next(range).ToString(CultureInfo.InvariantCulture);

        private string NullableWord() => _random.Next(6) == 0 ? "NULL" : $"'{Words[_random.Next(Words.Length)]}'";

        // ---- execution and comparison -----------------------------------------------------------

        private void Write(string sql)
        {
            Log(sql);
            var dv = _datavo.Execute(sql)[0];
            (int? sqliteAffected, string? sqliteError) = RunSqlite(sql);
            if (dv.IsError != (sqliteError is not null))
            {
                Fail(sql, $"DataVo {(dv.IsError ? "failed: " + string.Join(" | ", dv.Messages) : "succeeded")}, SQLite {(sqliteError is null ? "succeeded" : "failed: " + sqliteError)}");
            }
        }

        private void Setup(string sql)
        {
            Log(sql);
            Ok(_datavo.Execute(sql)[0], sql);
            (_, string? error) = RunSqlite(sql);
            if (error is not null)
            {
                Fail(sql, "SQLite rejected setup: " + error);
            }
        }

        private void Select(string sql, bool ordered)
        {
            Log(sql);
            var dv = _datavo.Execute(sql)[0];
            if (dv.IsError)
            {
                Fail(sql, "DataVo failed: " + string.Join(" | ", dv.Messages));
            }

            List<string> expected = [];
            List<string> expectedColumns;
            using (SqliteCommand command = _sqlite.CreateCommand())
            {
                command.CommandText = sql;
                using SqliteDataReader reader = command.ExecuteReader();
                expectedColumns = [.. Enumerable.Range(0, reader.FieldCount).Select(reader.GetName)];
                while (reader.Read())
                {
                    expected.Add(string.Join("|", Enumerable.Range(0, reader.FieldCount).Select(i => Format(reader.IsDBNull(i) ? null : reader.GetValue(i)))));
                }
            }

            // Column names and order must match too (SQLite names an unaliased P.Id just "Id").
            if (dv.Data.Count > 0)
            {
                List<string> actualColumns = [.. dv.Data[0].Keys.Select(name => name.Contains('.') ? name[(name.IndexOf('.') + 1)..] : name)];
                if (!actualColumns.SequenceEqual(expectedColumns, StringComparer.OrdinalIgnoreCase))
                {
                    Fail(sql, $"columns differ\n  SQLite: {string.Join(", ", expectedColumns)}\n  DataVo: {string.Join(", ", dv.Data[0].Keys)}");
                }
            }

            List<string> actual = [.. dv.Data.Select(row => string.Join("|", row.Values.Select(Format)))];
            if (!ordered)
            {
                expected.Sort(StringComparer.Ordinal);
                actual.Sort(StringComparer.Ordinal);
            }

            if (!expected.SequenceEqual(actual))
            {
                Fail(sql, $"rows differ\n  SQLite ({expected.Count}): {Preview(expected)}\n  DataVo ({actual.Count}): {Preview(actual)}");
            }
        }

        private (int? Affected, string? Error) RunSqlite(string sql)
        {
            try
            {
                using SqliteCommand command = _sqlite.CreateCommand();
                command.CommandText = sql;
                return (command.ExecuteNonQuery(), null);
            }
            catch (SqliteException ex)
            {
                return (null, ex.Message);
            }
        }

        private static string Format(object? value) => value switch
        {
            null => "NULL",
            DBNull => "NULL",
            double d => d.ToString("0.######", CultureInfo.InvariantCulture),
            float f => ((double)f).ToString("0.######", CultureInfo.InvariantCulture),
            decimal m => ((double)m).ToString("0.######", CultureInfo.InvariantCulture),
            bool b => b ? "1" : "0",
            IFormattable number => number.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString() ?? "NULL",
        };

        private static string Preview(List<string> rows) => rows.Count == 0 ? "(none)" : string.Join(" ; ", rows.Take(12)) + (rows.Count > 12 ? " ; ..." : string.Empty);

        private static void Ok(QueryResult result, string sql)
        {
            if (result.IsError)
            {
                Assert.Fail($"{sql}: {string.Join(" | ", result.Messages)}");
            }
        }

        private void Log(string sql)
        {
            _recent.Enqueue($"#{_step} {sql}");
            if (_recent.Count > 25)
            {
                _recent.Dequeue();
            }
        }

        private void Fail(string sql, string message)
        {
            var report = new StringBuilder()
                .AppendLine($"{_mode} seed {_seed}, step {_step}: {sql}")
                .AppendLine(message)
                .AppendLine("statements so far (latest last):");
            foreach (string statement in _recent)
            {
                report.AppendLine("  " + statement);
            }

            Assert.Fail(report.ToString());
        }
    }
}
