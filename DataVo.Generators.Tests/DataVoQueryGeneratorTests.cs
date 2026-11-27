using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Threading;
using DataVo.Core;
using DataVo.Generators;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.Diagnostics;
using Microsoft.CodeAnalysis.Text;

namespace DataVo.Generators.Tests;

public class DataVoQueryGeneratorTests
{
    [Fact]
    public void Generator_EmitsSelectSingleImplementation()
    {
        string source = """
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public sealed record PlayerProjection(int Id, string Name, int Level);

            public static partial class GameQueries
            {
                [DataVoQuery("SELECT Id, Name, Level FROM Players WHERE Id = @id")]
                public static partial PlayerProjection? GetPlayer(DataVoContext db, int id);
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.Contains("DataVoCompiledQueryPlan.SelectSingle", generated);
        Assert.Contains("new global::DataVo.Core.CompiledQueries.DataVoCompiledQueryParameter(\"id\", id)", generated);
        Assert.Contains("new global::PlayerProjection", generated);
    }

    [Fact]
    public void Generator_ReportsDiagnosticWhenParameterIsMissing()
    {
        string source = """
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public static partial class GameQueries
            {
                [DataVoQuery("SELECT Id FROM Players WHERE Id = @id")]
                public static partial int MissingParameter(DataVoContext db);
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);
        Diagnostic diagnostic = Assert.Single(result.Diagnostics);
        Assert.Equal("DATAVOQ002", diagnostic.Id);
    }

    [Fact]
    public void Generator_ReportsDiagnosticForUnsupportedJoin()
    {
        string source = """
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public static partial class GameQueries
            {
                [DataVoQuery("SELECT p.Id FROM Players p JOIN Guilds g ON p.GuildId = g.Id WHERE p.Id = @id")]
                public static partial int UnsupportedJoin(DataVoContext db, int id);
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);
        Diagnostic diagnostic = Assert.Single(result.Diagnostics);
        Assert.Equal("DATAVOQ001", diagnostic.Id);
    }

    [Fact]
    public void Generator_ReportsDiagnosticForUnsupportedScalarSelectReturnShape()
    {
        string source = """
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public static partial class GameQueries
            {
                [DataVoQuery("SELECT Id FROM Players WHERE Id = @id")]
                public static partial int GetPlayerId(DataVoContext db, int id);
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);

        // The SQL is valid; only the method's return type is not: that is DATAVOQ003, not "unsupported SQL".
        Diagnostic diagnostic = Assert.Single(result.Diagnostics);
        Assert.Equal("DATAVOQ003", diagnostic.Id);
        Assert.Contains("GetPlayerId", diagnostic.GetMessage(), StringComparison.Ordinal);
        Assert.Empty(result.Results.Single().GeneratedSources);
    }

    [Fact]
    public void Generator_EmitsSelectManyImplementation()
    {
        string source = """
            using System.Collections.Generic;
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public sealed record PlayerProjection(int Id, string Name, int Level);

            public static partial class GameQueries
            {
                [DataVoQuery("SELECT Id, Name, Level FROM Players WHERE Name = @name")]
                public static partial IReadOnlyList<PlayerProjection> GetPlayersByName(DataVoContext db, string name);
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.Contains("DataVoCompiledQueryPlan.SelectMany", generated);
        // PlayerProjection is a clean ctor-name match, so the generator now emits the typed projector.
        Assert.Contains("DataVoCompiledQuery.SelectManyTyped<global::PlayerProjection>", generated);
        Assert.Contains("new global::DataVo.Core.CompiledQueries.DataVoCompiledQueryParameter(\"name\", name)", generated);
    }

    [Fact]
    public void ShapeParser_ParsesInsert()
    {
        Assert.True(DataVo.Generators.Sql.DataVoQueryShapeParser.TryParse(
            "INSERT INTO Telemetry (Id, EventName, Frame) VALUES (@id, @eventName, @frame)",
            out var model));

        Assert.NotNull(model);
        Assert.Equal("Insert", model!.Kind);
        Assert.Equal(["Id", "EventName", "Frame"], model.InsertColumns);
        Assert.Equal(["id", "eventName", "frame"], model.InsertParameterNames);
    }

    [Fact]
    public void ShapeParser_ParsesUpdate()
    {
        Assert.True(DataVo.Generators.Sql.DataVoQueryShapeParser.TryParse(
            "UPDATE Players SET Level = @level WHERE Id = @id",
            out var model));

        Assert.NotNull(model);
        Assert.Equal("Update", model!.Kind);
        Assert.Equal("Players", model.TableName);
        Assert.Equal("Id", model.WhereColumn);
        Assert.Equal("id", model.WhereParameterName);
        Assert.Equal("level", model.Assignments["Level"]);
    }

    private const string OrderItemsSource = """
        using System.Collections.Generic;
        using DataVo.Core;
        using DataVo.Core.CompiledQueries;

        public sealed record OrderItemRow(int OrderId, string Sku);

        public static partial class OrderQueries
        {
            [DataVoQuery("SELECT OrderId, Sku FROM OrderItems WHERE OrderId = @orderId")]
            public static partial IReadOnlyList<OrderItemRow> LoadItems(DataVoContext db, int orderId);
        }
        """;

    [Fact]
    public void Generator_WithManifestIndex_EmitsSingleColumnIndexTaggedSelectMany()
    {
        const string manifest = """
            CREATE TABLE OrderItems (OrderItemId INT PRIMARY KEY, OrderId INT, Sku VARCHAR(50));
            CREATE INDEX ix_OrderItems_OrderId ON OrderItems (OrderId);
            """;

        GeneratorDriverRunResult result = RunGenerator(OrderItemsSource, manifest);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.Contains("global::DataVo.Core.CompiledQueries.CompiledAccessPath.SingleColumnIndex", generated);
        Assert.Contains("resolvedIndexName: \"ix_OrderItems_OrderId\"", generated);
    }

    [Fact]
    public void Generator_WithoutManifest_EmitsUntaggedSelectMany()
    {
        GeneratorDriverRunResult result = RunGenerator(OrderItemsSource);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.Contains("DataVoCompiledQueryPlan.SelectMany", generated);
        Assert.DoesNotContain("CompiledAccessPath.SingleColumnIndex", generated);
    }

    [Fact]
    public void Generator_ManifestColumnNotIndexed_EmitsUntaggedSelectMany()
    {
        const string manifest = "CREATE TABLE OrderItems (OrderItemId INT PRIMARY KEY, OrderId INT, Sku VARCHAR(50));";

        GeneratorDriverRunResult result = RunGenerator(OrderItemsSource, manifest);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.DoesNotContain("CompiledAccessPath.SingleColumnIndex", generated);
    }

    [Fact]
    public void Generator_ManifestFileNotMarked_IsIgnored_EmitsUntagged()
    {
        const string manifest = "CREATE INDEX ix_OrderItems_OrderId ON OrderItems (OrderId);";

        GeneratorDriverRunResult result = RunGenerator(OrderItemsSource, manifest, markAsManifest: false);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.DoesNotContain("CompiledAccessPath.SingleColumnIndex", generated);
    }

    [Fact]
    public void Generator_WithManifestIndex_EmitsSingleColumnIndexTaggedSelectSingle()
    {
        const string source = """
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public sealed record OrderItemRow(int OrderId, string Sku);

            public static partial class OrderQueries
            {
                [DataVoQuery("SELECT OrderId, Sku FROM OrderItems WHERE OrderId = @orderId")]
                public static partial OrderItemRow? GetItem(DataVoContext db, int orderId);
            }
            """;
        const string manifest = """
            CREATE TABLE OrderItems (OrderItemId INT PRIMARY KEY, OrderId INT, Sku VARCHAR(50));
            CREATE INDEX ix_OrderItems_OrderId ON OrderItems (OrderId);
            """;

        GeneratorDriverRunResult result = RunGenerator(source, manifest);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.Contains("DataVoCompiledQueryPlan.SelectSingle", generated);
        Assert.Contains("global::DataVo.Core.CompiledQueries.CompiledAccessPath.SingleColumnIndex", generated);
        Assert.Contains("resolvedIndexName: \"ix_OrderItems_OrderId\"", generated);
    }

    [Fact]
    public void Generator_CleanCtorMatch_EmitsTypedSelectMany()
    {
        string source = """
            using System.Collections.Generic;
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public sealed record PlayerProjection(int Id, string Name, int Level);

            public static partial class GameQueries
            {
                [DataVoQuery("SELECT Id, Name, Level FROM Players WHERE Name = @name")]
                public static partial IReadOnlyList<PlayerProjection> ByName(DataVoContext db, string name);
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.Contains("global::DataVo.Core.CompiledQueries.CompiledRowReader", generated);
        Assert.Contains("reader.GetInt32(\"Id\")", generated);
        Assert.Contains("reader.GetString(\"Name\")!", generated);
        Assert.Contains("reader.GetInt32(\"Level\")", generated);
        Assert.Contains("global::DataVo.Core.CompiledQueries.CompiledRowMapper<global::PlayerProjection>", generated);
        Assert.Contains("DataVoCompiledQuery.SelectManyTyped<global::PlayerProjection>", generated);
    }

    [Fact]
    public void Generator_CleanCtorMatch_EmitsTypedSelectSingle()
    {
        string source = """
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public sealed record PlayerProjection(int Id, string Name, int Level);

            public static partial class GameQueries
            {
                [DataVoQuery("SELECT Id, Name, Level FROM Players WHERE Id = @id")]
                public static partial PlayerProjection? Get(DataVoContext db, int id);
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.Contains("DataVoCompiledQuery.SelectSingleTyped<global::PlayerProjection>", generated);
        Assert.Contains("global::DataVo.Core.CompiledQueries.CompiledRowReader", generated);
    }

    [Fact]
    public void Generator_NonNameMatchedCtor_MapsColumnsByPosition()
    {
        // Ctor params (a, b, c) do not match projected columns (Id, Name, Level) by name: they bind by position, typed
        // by the parameters (the old *Id/Level/Frame name heuristic is gone).
        string source = """
            using System.Collections.Generic;
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public sealed record Misnamed(int a, string b, int c);

            public static partial class GameQueries
            {
                [DataVoQuery("SELECT Id, Name, Level FROM Players WHERE Name = @name")]
                public static partial IReadOnlyList<Misnamed> ByName(DataVoContext db, string name);
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.Contains("DataVoCompiledQuery.SelectManyTyped<global::Misnamed>", generated);
        Assert.Contains("new global::Misnamed(reader.GetInt32(\"Id\"), reader.GetString(\"Name\")!, reader.GetInt32(\"Level\"))", generated);
        AssertCompiles(source);
    }

    [Fact]
    public void Generator_PositionalCtorWithUnsupportedType_CastsByParameterType()
    {
        // A parameter type without a typed getter reads the boxed cell through CompiledRowReader.GetValue and casts it to
        // the parameter's type; the other arguments keep their typed getters (no dictionary mapper exists any more).
        string source = """
            using System.Collections.Generic;
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public sealed record Scored(long a, object b, double c);

            public static partial class GameQueries
            {
                [DataVoQuery("SELECT PlayerId, Payload, Score FROM Players WHERE Name = @name")]
                public static partial IReadOnlyList<Scored> ByName(DataVoContext db, string name);
            }
            """;

        string generated = Assert.Single(RunGenerator(source).Results.Single().GeneratedSources).SourceText.ToString();

        Assert.Contains("DataVoCompiledQuery.SelectManyTyped<global::Scored>", generated);
        Assert.Contains("reader.GetInt64(\"PlayerId\")", generated);
        Assert.Contains("(object)reader.GetValue(\"Payload\")!", generated);
        Assert.Contains("reader.GetDouble(\"Score\")", generated);
        Assert.DoesNotContain("row[", generated);
    }

    [Fact]
    public void Generator_CtorArityMismatch_ReportsRowTypeDiagnostic()
    {
        string source = """
            using System.Collections.Generic;
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public sealed record TwoColumns(int a, string b);

            public static partial class GameQueries
            {
                [DataVoQuery("SELECT Id, Name, Level FROM Players WHERE Name = @name")]
                public static partial IReadOnlyList<TwoColumns> ByName(DataVoContext db, string name);
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);

        Diagnostic diagnostic = Assert.Single(result.Diagnostics);
        Assert.Equal("DATAVOQ004", diagnostic.Id);
        Assert.Empty(result.Results.Single().GeneratedSources);
    }

    [Theory]
    [InlineData("public static partial class Q { [DataVoQuery(\"SELECT Id FROM P WHERE Id = @id\")] public partial Row? Get(DataVoContext db, int id); }", "static partial")]
    [InlineData("public partial class Q { [DataVoQuery(\"SELECT Id FROM P WHERE Id = @id\")] public static partial Row? Get(DataVoContext db, int id); }", "must be static")]
    [InlineData("public static class Q { [DataVoQuery(\"SELECT Id FROM P WHERE Id = @id\")] public static partial Row? Get(DataVoContext db, int id); }", "must be partial")]
    [InlineData("public static partial class Q { [DataVoQuery(\"SELECT Id FROM P WHERE Id = @id\")] public static partial Row? Get(int id, DataVoContext db); }", "DataVoContext")]
    [InlineData("public static partial class Q { [DataVoQuery(\"UPDATE P SET Name = @name WHERE Id = @id\")] public static partial void Set(DataVoContext db, string name, int id); }", "must return int")]
    [InlineData("public static partial class Q { [DataVoQuery(\"INSERT INTO P (Id) VALUES (@id)\")] public static partial int Add(DataVoContext db, int id); }", "IReadOnlyList")]
    public void Generator_InvalidMethodSignature_ReportsDatavoq003(string declaration, string reason)
    {
        string source = $"""
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public sealed record Row(int Id);

            {declaration}
            """;

        Diagnostic diagnostic = Assert.Single(RunGenerator(source).Diagnostics, static d => d.Id.StartsWith("DATAVOQ", StringComparison.Ordinal));
        Assert.Equal("DATAVOQ003", diagnostic.Id);
        Assert.Contains(reason, diagnostic.GetMessage(), StringComparison.Ordinal);
    }

    [Fact]
    public void Generator_PreservesAccessibility_NestedTypes_AndOverloads()
    {
        string source = """
            using System.Collections.Generic;
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            namespace First
            {
                public sealed record Row(int Id, string Name);

                public static partial class Outer
                {
                    internal static partial class Queries
                    {
                        [DataVoQuery("SELECT Id, Name FROM Players WHERE Id = @id")]
                        internal static partial Row? Get(DataVoContext db, int id);

                        [DataVoQuery("SELECT Id, Name FROM Players WHERE Name = @name")]
                        internal static partial IReadOnlyList<Row> Get(DataVoContext db, string name);
                    }
                }
            }

            namespace Second
            {
                public sealed record Row(int Id, string Name);

                public static partial class Outer
                {
                    internal static partial class Queries
                    {
                        [DataVoQuery("SELECT Id, Name FROM Players WHERE Id = @id")]
                        private static partial Row? Get(DataVoContext db, int id);

                        public static Row? Call(DataVoContext db) => Get(db, 1);
                    }
                }
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);

        Assert.Empty(result.Diagnostics);
        GeneratedSourceResult[] sources = [.. result.Results.Single().GeneratedSources];
        Assert.Equal(3, sources.Length);
        Assert.Equal(3, sources.Select(static source => source.HintName).Distinct().Count());
        string all = string.Concat(sources.Select(static source => source.SourceText.ToString()));
        Assert.Contains("internal static partial global::First.Row? Get(", all);
        Assert.Contains("private static partial global::Second.Row? Get(", all);
        Assert.Contains("public static partial class Outer", all);
        Assert.Contains("internal static partial class Queries", all);
        Assert.DoesNotContain("\"1.0\"", all);
        AssertCompiles(source);
    }

    [Fact]
    public void Generator_EmitsTypedGettersForFloatShortByteVector3AndMemory()
    {
        string source = """
            using System;
            using System.Collections.Generic;
            using System.Numerics;
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public sealed record Sample(int Id, float Ratio, short Small, byte Tiny, Vector3 Position, ReadOnlyMemory<float> Embedding, float? Maybe, Vector3? Spot);

            public static partial class SampleQueries
            {
                [DataVoQuery("SELECT Id, Ratio, Small, Tiny, Position, Embedding, Maybe, Spot FROM Samples WHERE Id = @id")]
                public static partial IReadOnlyList<Sample> Get(DataVoContext db, int id);
            }
            """;

        string generated = Assert.Single(RunGenerator(source).Results.Single().GeneratedSources).SourceText.ToString();

        Assert.Contains("SelectManyTyped<global::Sample>", generated);
        Assert.Contains("reader.GetSingle(\"Ratio\")", generated);
        Assert.Contains("reader.GetInt16(\"Small\")", generated);
        Assert.Contains("reader.GetByte(\"Tiny\")", generated);
        Assert.Contains("reader.GetVector3(\"Position\")", generated);
        Assert.Contains("reader.GetVectorMemory(\"Embedding\")", generated);
        Assert.Contains("reader.GetSingleOrNull(\"Maybe\")", generated);
        Assert.Contains("reader.GetVector3OrNull(\"Spot\")", generated);
        AssertCompiles(source);
    }

    [Fact]
    public void Generator_WithManifestPrimaryKey_EmitsPrimaryKeyAccessPath()
    {
        const string source = """
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public sealed record OrderItemRow(int OrderItemId, string Sku);

            public static partial class OrderQueries
            {
                [DataVoQuery("SELECT OrderItemId, Sku FROM OrderItems WHERE OrderItemId = @id")]
                public static partial OrderItemRow? Get(DataVoContext db, int id);
            }
            """;
        const string manifest = "CREATE TABLE OrderItems (OrderItemId INT PRIMARY KEY, OrderId INT, Sku VARCHAR(50));";

        string generated = Assert.Single(RunGenerator(source, manifest).Results.Single().GeneratedSources).SourceText.ToString();

        Assert.Contains("accessPath: global::DataVo.Core.CompiledQueries.CompiledAccessPath.PrimaryKey", generated);
        Assert.DoesNotContain("resolvedIndexName", generated);
    }

    [Fact]
    public void Generator_UnrelatedEdit_IsServedFromTheIncrementalCache()
    {
        CSharpCompilation compilation = CSharpCompilation.Create(
            "IncrementalTest",
            [CSharpSyntaxTree.ParseText(OrderItemsSource)],
            GetMetadataReferences(),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            [new DataVoQueryGenerator().AsSourceGenerator()],
            driverOptions: new GeneratorDriverOptions(IncrementalGeneratorOutputKind.None, trackIncrementalGeneratorSteps: true));

        driver = driver.RunGenerators(compilation);
        driver = driver.RunGenerators(compilation.AddSyntaxTrees(CSharpSyntaxTree.ParseText("namespace Other { public class Unrelated { } }")));

        IncrementalGeneratorRunStep[] outputs = [.. driver.GetRunResult().Results[0].TrackedOutputSteps.SelectMany(static step => step.Value)];
        Assert.NotEmpty(outputs);
        Assert.All(outputs.SelectMany(static step => step.Outputs), output => Assert.Equal(IncrementalStepRunReason.Cached, output.Reason));
    }

    [Fact]
    public void Generator_GuidCtorParam_EmitsTypedGetter()
    {
        string source = """
            using System;
            using System.Collections.Generic;
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public sealed record WithGuid(int Id, Guid Token);

            public static partial class GameQueries
            {
                [DataVoQuery("SELECT Id, Token FROM Sessions WHERE Id = @id")]
                public static partial IReadOnlyList<WithGuid> Get(DataVoContext db, int id);
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.Contains("CompiledRowReader", generated);
        Assert.Contains("SelectManyTyped", generated);
        Assert.Contains("reader.GetGuid(\"Token\")", generated);
    }

    [Fact]
    public void Generator_LongAndDecimalParams_EmitTypedGetters()
    {
        // Int64/Decimal have no storable column type, but the generator must still map them (emission only).
        string source = """
            using System.Collections.Generic;
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public sealed record Money(long Id, decimal Amount);

            public static partial class GameQueries
            {
                [DataVoQuery("SELECT Id, Amount FROM Ledger WHERE Id = @id")]
                public static partial IReadOnlyList<Money> Get(DataVoContext db, long id);
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.Contains("reader.GetInt64(\"Id\")", generated);
        Assert.Contains("reader.GetDecimal(\"Amount\")", generated);
    }

    [Fact]
    public void Generator_DateParams_EmitCanonicalAndExplicitModernGetters()
    {
        string source = """
            using System;
            using System.Collections.Generic;
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public sealed record Dates(
                DataVoDate Day,
                DataVoDate? OptionalDay,
                DateOnly ModernDay,
                DateOnly? OptionalModernDay);

            public static partial class DateQueries
            {
                [DataVoQuery("SELECT Day, OptionalDay, ModernDay, OptionalModernDay FROM Events WHERE Day = @day")]
                public static partial IReadOnlyList<Dates> Get(DataVoContext db, DataVoDate day);
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.Contains("reader.GetDate(\"Day\")", generated);
        Assert.Contains("reader.GetDateOrNull(\"OptionalDay\")", generated);
        Assert.Contains("reader.GetDateOnly(\"ModernDay\")", generated);
        Assert.Contains("reader.GetDateOnlyOrNull(\"OptionalModernDay\")", generated);
    }

    [Fact]
    public void Generator_DateTimeParams_EmitDateTimeAndOffsetGetters()
    {
        string source = """
            using System;
            using System.Collections.Generic;
            using DataVo.Core;
            using DataVo.Core.CompiledQueries;

            public sealed record Moments(
                DateTime At,
                DateTime? Closed,
                DateTimeOffset Logged,
                DateTimeOffset? Acknowledged);

            public static partial class MomentQueries
            {
                [DataVoQuery("SELECT At, Closed, Logged, Acknowledged FROM Events WHERE At = @after")]
                public static partial IReadOnlyList<Moments> Get(DataVoContext db, DateTime after);
            }
            """;

        GeneratorDriverRunResult result = RunGenerator(source);
        string generated = Assert.Single(result.Results.Single().GeneratedSources).SourceText.ToString();

        Assert.Contains("reader.GetDateTime(\"At\")", generated);
        Assert.Contains("reader.GetDateTimeOrNull(\"Closed\")", generated);
        Assert.Contains("reader.GetDateTimeOffset(\"Logged\")", generated);
        Assert.Contains("reader.GetDateTimeOffsetOrNull(\"Acknowledged\")", generated);
    }

    /// <summary>Runs the generator and asserts the program, with the generated code, compiles without errors.</summary>
    private static void AssertCompiles(string source)
    {
        CSharpCompilation compilation = CSharpCompilation.Create(
            "GeneratedCompiles",
            [CSharpSyntaxTree.ParseText(source)],
            GetMetadataReferences(),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));
        CSharpGeneratorDriver.Create(new DataVoQueryGenerator()).RunGeneratorsAndUpdateCompilation(compilation, out Compilation output, out _);
        Assert.Empty(output.GetDiagnostics().Where(static diagnostic => diagnostic.Severity == DiagnosticSeverity.Error));
    }

    private static GeneratorDriverRunResult RunGenerator(string source, string? manifest = null, bool markAsManifest = true)
    {
        CSharpCompilation compilation = CSharpCompilation.Create(
            "GeneratorTest",
            [CSharpSyntaxTree.ParseText(source)],
            GetMetadataReferences(),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        var additionalTexts = new List<AdditionalText>();
        var fileOptions = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        if (manifest is not null)
        {
            additionalTexts.Add(new InMemoryAdditionalText("schema.sql", manifest));
            if (markAsManifest)
            {
                fileOptions["build_metadata.AdditionalFiles.DataVoSchemaManifest"] = "true";
            }
        }

        var optionsProvider = new TestAnalyzerConfigOptionsProvider(new DictionaryOptions(fileOptions));

        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            [new DataVoQueryGenerator().AsSourceGenerator()],
            additionalTexts,
            parseOptions: null,
            optionsProvider: optionsProvider);
        driver = driver.RunGenerators(compilation);
        return driver.GetRunResult();
    }

    private sealed class InMemoryAdditionalText : AdditionalText
    {
        private readonly string _text;

        public InMemoryAdditionalText(string path, string text)
        {
            Path = path;
            _text = text;
        }

        public override string Path { get; }

        public override SourceText GetText(CancellationToken cancellationToken = default)
            => SourceText.From(_text, Encoding.UTF8);
    }

    private sealed class TestAnalyzerConfigOptionsProvider : AnalyzerConfigOptionsProvider
    {
        private static readonly DictionaryOptions EmptyOptions = new(new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase));
        private readonly AnalyzerConfigOptions _fileOptions;

        public TestAnalyzerConfigOptionsProvider(AnalyzerConfigOptions fileOptions) => _fileOptions = fileOptions;

        public override AnalyzerConfigOptions GlobalOptions => EmptyOptions;

        public override AnalyzerConfigOptions GetOptions(SyntaxTree tree) => EmptyOptions;

        public override AnalyzerConfigOptions GetOptions(AdditionalText textFile) => _fileOptions;
    }

    private sealed class DictionaryOptions : AnalyzerConfigOptions
    {
        private readonly Dictionary<string, string> _values;

        public DictionaryOptions(Dictionary<string, string> values) => _values = values;

        public override bool TryGetValue(string key, out string value) => _values.TryGetValue(key, out value!);
    }

    private static MetadataReference[] GetMetadataReferences()
    {
        string? tpa = AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES") as string;
        Assert.False(string.IsNullOrWhiteSpace(tpa));

        HashSet<string> allowed = ["System.Runtime", "mscorlib", "netstandard", "System.Collections", "System.Linq", "System.Private.CoreLib", "System.Numerics.Vectors", "System.Memory"];
        var references = tpa!
            .Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries)
            .Where(path => allowed.Contains(Path.GetFileNameWithoutExtension(path), StringComparer.OrdinalIgnoreCase))
            .Select(static path => MetadataReference.CreateFromFile(path))
            .ToList();

        references.Add(MetadataReference.CreateFromFile(typeof(DataVoContext).Assembly.Location));
        references.Add(MetadataReference.CreateFromFile(typeof(Enumerable).Assembly.Location));
        references.Add(MetadataReference.CreateFromFile(typeof(Attribute).Assembly.Location));
        return [.. references];
    }
}
