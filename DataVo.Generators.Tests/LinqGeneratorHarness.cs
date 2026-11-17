using DataVo.Core;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace DataVo.Generators.Tests;

/// <summary>Compiles a small program around one query expression and runs the table and interceptor generators over it.</summary>
internal static class LinqGeneratorHarness
{
    public const string Model = """
        using System;
        using System.Linq;
        using DataVo.Core;
        using DataVo.Core.Annotations;

        namespace App
        {
            [DataVoTable("players")]
            public sealed class Player
            {
                [PrimaryKey] public int Id { get; set; }
                [Indexed] public int Level { get; set; }
                [Indexed] public string Name { get; set; } = "";
                public string? Guild { get; set; }
                public int? Score { get; set; }
                public bool IsActive { get; set; }
                public float Speed { get; set; }
                public long Gold { get; set; }
                public double? Rating { get; set; }
                public DataVoDate Joined { get; set; }
                public DateOnly? Seen { get; set; }
            }

            [DataVoTable("cards")]
            public sealed record Card([PrimaryKey] int Id, string Title, int Cost, long? Bonus);

            [DataVoTable("marks")]
            public struct Mark
            {
                [PrimaryKey] public int Id { get; set; }
                public int Zone { get; set; }
            }

            [DataVoTable("scrolls")]
            public sealed class Scroll
            {
                [PrimaryKey] public Guid Id { get; init; }
                public string Text { get; init; } = "";
                public double Weight { get; init; }
            }

            public sealed record PlayerCard(int Id, string Name);

            public sealed class PlayerRow
            {
                public int Id { get; set; }
                public string Name { get; set; } = "";
                public long Wealth { get; init; }
            }

            public enum Rank { Low = 1, High = 2 }

            public static class Setters
            {
                public static readonly Func<DataVoSetters<Player>, DataVoSetters<Player>> Gold = static s => s;
            }

            public static class Fmt
            {
                public static string Tag(string name, int level) => name + level;
            }

            public static class Limits
            {
                public const int Max = 60;
                public static readonly int Soft = 40;
                private static readonly int Hidden = 1;
                public static int Peek() => Hidden;
            }

            public static class Queries
            {
                public static object? Run(DataVoContext db, int target, string? text, int? maybe)
                {
                    DataVoTable<Player> players = db.Table<Player>();
                    return BODY;
                }
            }
        }
        """;

    public static (string Generated, Diagnostic[] Diagnostics) Generate(string call)
    {
        Compile(call, LanguageVersion.Latest, out GeneratorRunResult result);
        return (string.Concat(result.GeneratedSources.Select(static source => source.SourceText.ToString())), [.. result.Diagnostics]);
    }

    public static Compilation Compile(string call, LanguageVersion version, out GeneratorRunResult linq, bool interceptors = true, string feature = "InterceptorsNamespaces")
    {
        CSharpParseOptions parse = ParseOptions(version, interceptors ? feature : null);
        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            [new DataVoTableGenerator().AsSourceGenerator(), new DataVoLinqInterceptorGenerator().AsSourceGenerator()],
            parseOptions: parse);
        driver = driver.RunGeneratorsAndUpdateCompilation(CreateCompilation(Model.Replace("BODY", call), parse), out Compilation output, out _);
        linq = driver.GetRunResult().Results[1];
        return output;
    }

    public static CSharpParseOptions ParseOptions(LanguageVersion version, string? feature)
    {
        var options = new CSharpParseOptions(version);
        return feature is null ? options : options.WithFeatures([new KeyValuePair<string, string>(feature, "Other.Namespace;DataVo.Generated")]);
    }

    public static CSharpCompilation CreateCompilation(string source, CSharpParseOptions parse)
    {
        string[] platform = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!).Split(Path.PathSeparator);
        MetadataReference[] references =
        [
            .. platform.Where(static path => Path.GetFileName(path).StartsWith("System.", StringComparison.Ordinal) || Path.GetFileName(path) == "netstandard.dll")
                .Select(static path => MetadataReference.CreateFromFile(path)),
            MetadataReference.CreateFromFile(typeof(DataVoContext).Assembly.Location),
        ];

        return CSharpCompilation.Create(
            "LinqGeneratorTest",
            [CSharpSyntaxTree.ParseText(source, parse, path: "Query.cs")],
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));
    }
}
