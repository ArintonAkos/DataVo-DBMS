namespace DataVo.CrashHarness;

/// <summary>
/// The "columnar" crash workload, shared by the harness (which writes it) and <c>CrashRecoveryTests</c> (which
/// replays its acknowledgements into a model): the two tables, and the cells a row holds for a given generation.
/// </summary>
/// <remarks>
/// Every write goes to <see cref="IndexedTable"/>, which has a <c>USING COLUMNAR</c> index, and to
/// <see cref="PlainTable"/>, which has none, in one transaction. The cells are a pure function of (id, generation),
/// so an acknowledgement line carries two numbers and the test rebuilds the exact row from them.
/// </remarks>
internal static class ColumnarCrashWorkload
{
    public const string IndexedTable = "Cols";
    public const string PlainTable = "Plain";
    public const string Columns = "Id BIGINT PRIMARY KEY, Level INT, Gold BIGINT, Score DOUBLE, Ratio FLOAT, Tag VARCHAR(16)";
    public const string IndexedColumns = "Level, Gold, Score, Ratio";

    /// <summary>Insert of row <c>A</c> with generation <c>B</c>.</summary>
    public const char Insert = 'I';

    /// <summary>Insert of rows <c>A</c> to <c>A + B - 1</c>, each with generation <c>A</c>, in one transaction.</summary>
    public const char BulkInsert = 'B';

    /// <summary>Single-row update of row <c>A</c> to generation <c>B</c>.</summary>
    public const char Update = 'U';

    /// <summary>Single-row delete of row <c>A</c>.</summary>
    public const char Delete = 'D';

    /// <summary>Multi-row delete of the rows with ids in [A, A + B).</summary>
    public const char DeleteRange = 'R';

    /// <summary>Multi-row update setting Gold to NULL for the rows with ids in [A, A + B).</summary>
    public const char NullGoldRange = 'N';

    /// <summary>The columnar-indexed cells of one row: NULLs and NaNs included on purpose.</summary>
    public readonly record struct Cells(int Level, long? Gold, double? Score, float? Ratio)
    {
        public string Tag => $"t{Level & 7}";
    }

    /// <summary>The cells of row <paramref name="id"/> at <paramref name="generation"/>.</summary>
    public static Cells CellsOf(long id, long generation)
    {
        ulong h = Mix(unchecked(((ulong)id * 0x9E37_79B9_7F4A_7C15UL) ^ (ulong)generation));
        int level = (int)(h % 200) - 50;
        long? gold = h % 11 == 0 ? null : (long)((h >> 8) % 2_000_001) - 1_000_000;
        double? score = ((h >> 16) % 17) switch
        {
            0 => double.NaN,
            1 => null,
            _ => (((h >> 20) % 100_003) / 7.0) - 5_000,
        };
        float? ratio = ((h >> 24) % 19) switch
        {
            0 => float.NaN,
            1 => null,
            _ => (((h >> 28) % 4_001) * 0.25f) - 500f,
        };
        return new Cells(level, gold, score, ratio);
    }

    // SplitMix64 finalizer: spreads (id, generation) over every bit.
    private static ulong Mix(ulong x)
    {
        x = unchecked((x ^ (x >> 30)) * 0xBF58_476D_1CE4_E5B9UL);
        x = unchecked((x ^ (x >> 27)) * 0x94D0_49BB_1331_11EBUL);
        return x ^ (x >> 31);
    }
}
