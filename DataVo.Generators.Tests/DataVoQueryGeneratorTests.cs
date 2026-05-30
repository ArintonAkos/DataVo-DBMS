using System.Reflection;
using DataVo.Core;
using DataVo.Generators;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

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

    private static GeneratorDriverRunResult RunGenerator(string source)
    {
        CSharpCompilation compilation = CSharpCompilation.Create(
            "GeneratorTest",
            [CSharpSyntaxTree.ParseText(source)],
            GetMetadataReferences(),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

        IIncrementalGenerator generator = new DataVoQueryGenerator();
        GeneratorDriver driver = CSharpGeneratorDriver.Create(generator);
        driver = driver.RunGenerators(compilation);
        return driver.GetRunResult();
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
