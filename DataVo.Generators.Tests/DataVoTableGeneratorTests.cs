using DataVo.Core;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace DataVo.Generators.Tests;

/// <summary>What the table generator emits, and what it refuses at compile time.</summary>
public class DataVoTableGeneratorTests
{
    private const string Header = "using System; using DataVo.Core; using DataVo.Core.Annotations;\nnamespace App;\n";

    private const string ValidModel = Header + """
        [DataVoTable("players")]
        public sealed class Player
        {
            [PrimaryKey] public int Id { get; set; }
            [Indexed] public int Level { get; set; }
            public string? Name { get; set; }
        }
        """;

    [Fact]
    public void ValidModel_EmitsEveryMethod()
    {
        string generated = GeneratedText(ValidModel);

        Assert.Contains("EnsurePlayerTable(this global::DataVo.Core.DataVoContext context)", generated);
        Assert.Contains("GetPlayerById(this global::DataVo.Core.DataVoContext context, int key)", generated);
        Assert.Contains("Insert(this global::DataVo.Core.DataVoContext context, global::App.Player entity)", generated);
        Assert.Contains("Update(this global::DataVo.Core.DataVoContext context, global::App.Player entity)", generated);
        Assert.Contains("ExecuteBorrowed", generated);
    }

    [Fact]
    public void Output_CompilesAsCSharp9WithNoErrors()
    {
        (Compilation output, _) = Run(ValidModel, LanguageVersion.CSharp9);

        Assert.Empty(output.GetDiagnostics().Where(static diagnostic => diagnostic.Severity == DiagnosticSeverity.Error));
    }

    [Theory]
    [InlineData("System.Reflection")]
    [InlineData("Activator")]
    [InlineData("typeof(")]
    [InlineData("dynamic ")]
    [InlineData("Expression<")]
    public void Output_UsesNoRuntimeCodeGeneration(string forbidden)
    {
        Assert.DoesNotContain(forbidden, GeneratedText(ValidModel));
    }

    [Fact]
    public void Output_ReadsRowsThroughAStaticDelegate()
    {
        Assert.Contains("ReadRow = static row =>", GeneratedText(ValidModel));
    }

    [Fact]
    public void InternalModel_GetsInternalExtensions()
    {
        string generated = GeneratedText(ValidModel.Replace("public sealed class Player", "internal sealed class Player"));

        Assert.Contains("internal static class PlayerDataVoExtensions", generated);
    }

    [Fact]
    public void ComputedProperty_IsNotAColumn()
    {
        string generated = GeneratedText(ValidModel.Replace("public string? Name { get; set; }", "public int Twice => Level * 2;"));

        Assert.DoesNotContain("Twice", generated);
    }

    [Theory]
    [InlineData("public ulong Big { get; set; }", "DATAVOT003")]
    [InlineData("public DateTime When { get; set; }", "DATAVOT003")]
    [InlineData("public decimal Price { get; set; }", "DATAVOT003")]
    [InlineData("[PrimaryKey] public int Second { get; set; }", "DATAVOT002")]
    public void Unsupported_ReportsAnError(string extraMember, string expectedId)
    {
        string source = ValidModel.Replace("public string? Name { get; set; }", extraMember);

        Assert.Contains(Run(source).Diagnostics, diagnostic => diagnostic.Id == expectedId && diagnostic.Severity == DiagnosticSeverity.Error);
        Assert.Empty(Run(source).GeneratedSources);
    }

    [Theory]
    [InlineData("[DataVoTable(\"t\")] public class M { public int Id { get; set; } }", "DATAVOT001")]
    [InlineData("[DataVoTable(\"t\")] public class M { [PrimaryKey] public int? Id { get; set; } }", "DATAVOT004")]
    [InlineData("[DataVoTable(\"t\")] public class M { public M(int other) { } [PrimaryKey] public int Id { get; set; } }", "DATAVOT005")]
    [InlineData("[DataVoTable(\"t\")] public class M<T> { [PrimaryKey] public int Id { get; set; } }", "DATAVOT006")]
    [InlineData("[DataVoTable(\"t\")] public abstract class M { [PrimaryKey] public int Id { get; set; } }", "DATAVOT006")]
    [InlineData("[DataVoTable(\"drop table\")] public class M { [PrimaryKey] public int Id { get; set; } }", "DATAVOT007")]
    public void InvalidModel_ReportsAnError(string model, string expectedId)
    {
        Assert.Contains(Run(Header + model).Diagnostics, diagnostic => diagnostic.Id == expectedId);
    }

    [Fact]
    public void Diagnostic_PointsAtTheOffendingProperty()
    {
        string source = ValidModel.Replace("public string? Name { get; set; }", "public ulong Big { get; set; }");

        Diagnostic diagnostic = Assert.Single(Run(source).Diagnostics);
        Assert.Contains("Big", diagnostic.GetMessage());
        FileLinePositionSpan position = diagnostic.Location.GetLineSpan();
        Assert.Equal("Model.cs", position.Path);
        Assert.Contains("Big", source.Split('\n')[position.StartLinePosition.Line]);
    }

    [Fact]
    public void UnrelatedEdit_DoesNotRegenerate()
    {
        CSharpCompilation compilation = CreateCompilation(ValidModel, LanguageVersion.Latest);
        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            [new DataVoTableGenerator().AsSourceGenerator()],
            driverOptions: new GeneratorDriverOptions(IncrementalGeneratorOutputKind.None, trackIncrementalGeneratorSteps: true));

        driver = driver.RunGenerators(compilation);
        driver = driver.RunGenerators(compilation.AddSyntaxTrees(CSharpSyntaxTree.ParseText("namespace Other; public class Unrelated { }")));

        IncrementalGeneratorRunStep[] outputs = [.. driver.GetRunResult().Results[0].TrackedOutputSteps.SelectMany(static step => step.Value)];
        Assert.NotEmpty(outputs);
        Assert.All(outputs.SelectMany(static step => step.Outputs), output => Assert.Equal(IncrementalStepRunReason.Cached, output.Reason));
    }

    private static string GeneratedText(string source) => Assert.Single(Run(source).GeneratedSources).SourceText.ToString();

    private static GeneratorRunResult Run(string source)
    {
        GeneratorDriver driver = CSharpGeneratorDriver.Create(new DataVoTableGenerator());
        return driver.RunGenerators(CreateCompilation(source, LanguageVersion.Latest)).GetRunResult().Results[0];
    }

    private static (Compilation Output, GeneratorDriverRunResult Result) Run(string source, LanguageVersion version)
    {
        var parseOptions = new CSharpParseOptions(version);
        GeneratorDriver driver = CSharpGeneratorDriver.Create([new DataVoTableGenerator().AsSourceGenerator()], parseOptions: parseOptions);
        driver = driver.RunGeneratorsAndUpdateCompilation(CreateCompilation(source.Replace("namespace App;\n", "namespace App {\n") + "\n}", version), out Compilation output, out _);
        return (output, driver.GetRunResult());
    }

    private static CSharpCompilation CreateCompilation(string source, LanguageVersion version)
    {
        string[] platform = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!).Split(Path.PathSeparator);
        MetadataReference[] references =
        [
            .. platform.Where(static path => Path.GetFileName(path).StartsWith("System.", StringComparison.Ordinal) || Path.GetFileName(path) == "netstandard.dll")
                .Select(static path => MetadataReference.CreateFromFile(path)),
            MetadataReference.CreateFromFile(typeof(DataVoContext).Assembly.Location),
        ];

        return CSharpCompilation.Create(
            "TableGeneratorTest",
            [CSharpSyntaxTree.ParseText(source, new CSharpParseOptions(version), path: "Model.cs")],
            references,
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary, nullableContextOptions: NullableContextOptions.Enable));
    }
}
