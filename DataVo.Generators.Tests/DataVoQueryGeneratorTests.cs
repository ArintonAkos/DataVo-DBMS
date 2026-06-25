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

        Diagnostic diagnostic = Assert.Single(result.Diagnostics);
        Assert.Equal("DATAVOQ001", diagnostic.Id);
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
        Assert.Contains("DataVoCompiledQuery.SelectMany<global::PlayerProjection>", generated);
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

        HashSet<string> allowed = ["System.Runtime", "mscorlib", "netstandard", "System.Collections", "System.Linq", "System.Private.CoreLib"];
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
