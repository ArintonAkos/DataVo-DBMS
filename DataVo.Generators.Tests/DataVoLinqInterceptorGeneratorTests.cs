using DataVo.Core;
using DataVo.Generators.Linq;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace DataVo.Generators.Tests;

/// <summary>What the interceptor generator translates, what it leaves to scan, and what it tells the developer.</summary>
public class DataVoLinqInterceptorGeneratorTests
{
    private const string Model = """
        using System;
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

    [Theory]
    [InlineData("players.FirstOrDefault(target, static (p, id) => p.Id == id)", "Id = @p0", "new global::DataVo.Core.DataVoQueryParameter(\"p0\", (int)(state))")]
    [InlineData("players.FirstOrDefault(p => p.Id == 42)", "Id = @p0", "(\"p0\", 42)")]
    [InlineData("players.SingleOrDefault(p => 42 == p.Id)", "Id = @p0", "(\"p0\", 42)")]
    [InlineData("players.FirstOrDefault(p => p.Id == Limits.Max)", "Id = @p0", "(\"p0\", 60)")]
    [InlineData("players.Where(p => p.Level >= Limits.Soft)", "Level >= @p0", "(int)(global::App.Limits.Soft)")]
    [InlineData("players.Where(target, static (p, min) => p.Level >= min)", "Level >= @p0", "(int)(state)")]
    [InlineData("players.Where(target, static (p, min) => min < p.Level)", "Level > @p0", "(int)(state)")]
    [InlineData("players.Where(p => p.Level > 10 && p.IsActive)", "Level > @p0 AND IsActive = @p1", "(\"p1\", true)")]
    [InlineData("players.Where(p => p.Level > 10 && !p.IsActive)", "Level > @p0 AND IsActive = @p1", "(\"p1\", false)")]
    [InlineData("players.Where(p => (p.Level < 5 || p.Level > 50) && p.IsActive)", "(Level < @p0 OR Level > @p1) AND IsActive = @p2", "(\"p2\", true)")]
    [InlineData("players.Where((Min: 1, Max: 9), static (p, r) => p.Level >= r.Min && p.Level <= r.Max)", "Level >= @p0 AND Level <= @p1", "(int)(state.Max)")]
    [InlineData("players.Where(p => p.Name == \"a\\\"b\")", "Name = @p0", "(\"p0\", \"a\\\"b\")")]
    [InlineData("players.Where(p => p.Gold > 5)", "Gold > @p0", "(\"p0\", 5L)")]
    [InlineData("players.Where(p => p.Speed > 1.5f)", "Speed > @p0", "(\"p0\", 1.5F)")]
    [InlineData("players.Where(p => p.Score == null)", "Score IS NULL", "ExecuteBorrowed()")]
    [InlineData("players.Where(p => p.Guild is not null)", "Guild IS NOT NULL", "ExecuteBorrowed()")]
    [InlineData("players.Where(p => p.Score != 3)", "(Score <> @p0 OR Score IS NULL)", "(\"p0\", 3)")]
    [InlineData("players.Where(p => p.Level != 3)", "Level <> @p0", "(\"p0\", 3)")]
    public void TranslatablePredicate_BecomesAStatement(string call, string where, string binding)
    {
        (string generated, Diagnostic[] diagnostics) = Generate(call);

        Assert.Empty(diagnostics);
        Assert.Contains($"base(\"{where.Replace("\"", "\\\"")}\"", generated);
        Assert.Contains(binding, generated);
        Assert.Contains("[global::System.Runtime.CompilerServices.InterceptsLocation(1, \"", generated);
    }

    [Theory]
    [InlineData("players.Where(target, static (p, min) => p.Gold >= min)", "(row.TryGetInt64(c0, out long v0) && v0 >= p0)", "kernel.p0 = (long)(state);")]
    [InlineData("players.Where(p => p.IsActive)", "(row.TryGetBoolean(c0, out bool v0) && v0 == p0)", "kernel.p0 = true;")]
    [InlineData("players.Where(p => !p.IsActive && p.Speed < 2.5f)", "(row.TryGetBoolean(c0, out bool v0) && v0 == p0) && (row.TryGetFloat(c1, out float v1) && v1 < p1)", "kernel.p1 = 2.5F;")]
    [InlineData("players.Where(p => p.Score != 7)", "(!row.TryGetInt32(c0, out int v0) || v0 != p0)", "public int p0;")]
    [InlineData("players.Where(p => p.Score == null || p.Gold > 5)", "row.IsNull(c0) || (row.TryGetInt64(c1, out long v1) && v1 > p0)", "new string[] { \"Score\", \"Gold\" }")]
    [InlineData("players.Where(p => p.Guild != null && (p.Gold < 1 || p.Gold > 9))", "!row.IsNull(c0) && ((row.TryGetInt64(c1, out long v1) && v1 < p0) || (row.TryGetInt64(c1, out long v2) && v2 > p1))", "new string[] { \"Guild\", \"Gold\" }")]
    public void ConditionOverUnindexedColumns_IsAlsoCompiledIntoTheScan(string call, string condition, string detail)
    {
        string generated = Generate(call).Generated;

        Assert.Contains($"return {condition};", generated);
        Assert.Contains(detail, generated);
        Assert.Contains("struct Kernel : global::DataVo.Core.IRowRefFilter", generated);
        Assert.Contains("count = table.CountMatching(Columns, Kinds, ref kernel, limit);", generated);
    }

    [Theory]
    [InlineData("players.Where(target, static (p, min) => p.Gold >= min)", "l", "(global::DataVo.Core.RowBytes.Int64(row, o0) >= p0)", "(cells[c0].AsInt64() >= p0)")]
    [InlineData("players.Where(p => !p.IsActive && p.Speed < 2.5f)", "bf",
        "(global::DataVo.Core.RowBytes.Boolean(row, o0) == p0) & (global::DataVo.Core.RowBytes.Single(row, o1) < p1)",
        "(cells[c0].AsBoolean() == p0) & ((float)cells[c1].AsDouble() < p1)")]
    [InlineData("players.Where(p => p.Score != 7)", "i", "(global::DataVo.Core.RowBytes.Int32(row, o0) != p0)", "(cells[c0].AsInt32() != p0)")]
    [InlineData("players.Where(p => p.Score == null || p.Gold > 5)", "nl", "false | (global::DataVo.Core.RowBytes.Int64(row, o1) > p0)", "false | (cells[c1].AsInt64() > p0)")]
    [InlineData("players.Where(p => p.Guild != null && (p.Gold < 1 || p.Gold > 9))", "nl",
        "true & ((global::DataVo.Core.RowBytes.Int64(row, o1) < p0) | (global::DataVo.Core.RowBytes.Int64(row, o1) > p1))",
        "true & ((cells[c1].AsInt64() < p0) | (cells[c1].AsInt64() > p1))")]
    [InlineData("players.Where(p => p.Score != null && p.Score > 3)", "i", "true & (global::DataVo.Core.RowBytes.Int32(row, o0) > p0)", "true & (cells[c0].AsInt32() > p0)")]
    public void CompiledCondition_AlsoReadsStoredBytesAndCellsDirectly_WithoutBranchesBetweenTests(string call, string kinds, string bytes, string cells)
    {
        string generated = Generate(call).Generated;

        Assert.Contains($"private const string Kinds = \"{kinds}\";", generated);
        Assert.Contains($"public bool MatchesBytes(global::System.ReadOnlySpan<byte> row)", generated);
        Assert.Contains($"return {bytes};", generated);
        Assert.Contains($"return {cells};", generated);
        Assert.Contains("public void BindOffsets(global::System.ReadOnlySpan<int> offsets)", generated);
    }

    [Theory]
    [InlineData("players.Where(p => p.IsActive).Sum(p => p.Gold)", "long", "SumInt64Column", "Gold")]
    [InlineData("players.Where(target, static (p, min) => p.Gold > min).Sum(static p => p.Speed)", "float", "SumSingleColumn", "Speed")]
    [InlineData("players.Where(p => p.IsActive).Sum(p => (p.Level))", "int", "SumInt32Column", "Level")]
    [InlineData("players.Where(p => p.Level > 3).Sum(p => p.Id)", "int", "SumInt32Column", "Id")]
    public void SumOfOneNumericColumn_IsRoutedToTheScan(string call, string type, string method, string column)
    {
        string generated = Generate(call).Generated;

        Assert.Contains($"public static {type} Sum0(this in global::DataVo.Core.DataVoQuery<global::App.Player, ", generated);
        Assert.Contains($"return query.{method}(SumColumn0, selector);", generated);
        Assert.Contains($"SumColumn0 = new string[] {{ \"{column}\" }};", generated);
    }

    [Theory]
    [InlineData("players.Where(p => p.IsActive).Sum(p => p.Gold * 2)")] // computed
    [InlineData("players.Where(p => p.IsActive).Sum(p => p.Score ?? 0)")] // nullable column
    [InlineData("players.Where(p => p.IsActive).Sum(p => (long)p.Level)")] // widened: not the column's own type
    public void OtherSumSelectors_AreLeftToRunAsWritten(string call)
    {
        (string generated, Diagnostic[] diagnostics) = Generate(call);

        Assert.DoesNotContain("SumColumn", generated);
        Assert.Empty(diagnostics);
    }

    [Fact]
    public void CompiledCondition_CanAlsoSum()
    {
        string generated = Generate("players.Where(p => p.IsActive)").Generated;

        Assert.Contains("protected override bool TrySum(", generated);
        Assert.Contains("return table.SumMatching(Columns, Kinds, ref kernel, column, floating, out whole, out real);", generated);
    }

    [Theory]
    [InlineData("players.Where(target, static (p, min) => p.Level >= min)")] // an index can seek
    [InlineData("players.Where(p => p.Id > 3 && p.IsActive)")] // the primary key can seek
    [InlineData("players.FirstOrDefault(p => p.Id == 42)")] // one row, read directly
    [InlineData("players.Where(p => p.Guild == \"red\")")] // text is compared by the SQL engine
    [InlineData("players.Where(p => p.IsActive && p.Guild == \"red\")")]
    public void ConditionTheSqlPathServesBetter_GetsNoScanKernel(string call)
    {
        string generated = Generate(call).Generated;

        Assert.Contains("InterceptsLocation", generated);
        Assert.DoesNotContain("IRowRefFilter", generated);
    }

    [Theory]
    [InlineData("players.FirstOrDefault(target, static (p, id) => p.Id == id)", true)]
    [InlineData("players.FirstOrDefault(target, static (p, id) => p.Id == id && p.IsActive)", true)]
    [InlineData("players.FirstOrDefault(target, static (p, id) => p.Id == id || p.IsActive)", false)]
    [InlineData("players.FirstOrDefault(target, static (p, id) => p.Id >= id)", false)]
    [InlineData("players.FirstOrDefault(target, static (p, lv) => p.Level == lv)", false)]
    public void OnlyAPinnedPrimaryKey_SkipsTheRowLimit(string call, bool unique)
    {
        Assert.Contains(unique ? ", true)" : ", false)", Generate(call).Generated);
    }

    [Theory]
    [InlineData("players.Where(text, static (p, n) => p.Guild == n)", "return (state) != null;")]
    [InlineData("players.Where(maybe, static (p, s) => p.Score == s)", "return (state).HasValue;")]
    [InlineData("players.Where((text, maybe), static (p, s) => p.Guild == s.text && p.Score > s.maybe)", "return (state.text) != null && (state.maybe).HasValue;")]
    public void NullableValue_IsGuardedSoNullFallsBackToTheLambda(string call, string guard)
    {
        Assert.Contains(guard, Generate(call).Generated);
    }

    [Theory]
    [InlineData("players.FirstOrDefault(p => p.Id == target)", "'target'")]
    [InlineData("players.Where(p => p.Level >= target && p.Guild == text)", "'target', 'text'")]
    [InlineData("players.Where(target, (p, min) => p.Level >= min && p.Id != target)", "'target'")]
    public void CapturedVariable_WarnsAndIsNotIntercepted(string call, string names)
    {
        (string generated, Diagnostic[] diagnostics) = Generate(call);

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal("DATAVOL001", diagnostic.Id);
        Assert.Equal(DiagnosticSeverity.Warning, diagnostic.Severity);
        Assert.Contains(names, diagnostic.GetMessage());
        Assert.Contains("static (row, v) => row.Column == v", diagnostic.GetMessage());
        Assert.Empty(generated);
    }

    [Theory]
    [InlineData("players.Where(p => p.Name.StartsWith(\"a\"))", "not a comparison")]
    [InlineData("players.Where(p => p.Level + 1 > 3)", "one column with one value")]
    [InlineData("players.Where(p => p.Level > p.Id)", "one column with one value")]
    [InlineData("players.Where(p => p.Speed > 1.5)", "compare it with a value of type float")]
    [InlineData("players.Where(target, static (p, t) => p.Level > t * 2)", "'t * 2'")]
    [InlineData("players.Where(p => p.Level > Limits.Peek())", "'Limits.Peek()'")]
    [InlineData("players.Where(p => p.Score > null)", "against null")]
    [InlineData("players.Where(p => { return p.Level > 1; })", "single expression")]
    [InlineData("players.Where(new { A = 1 }, static (p, s) => p.Level > s.A)", "cannot name the state type")]
    public void UntranslatablePredicate_WarnsAndIsNotIntercepted(string call, string reason)
    {
        (string generated, Diagnostic[] diagnostics) = Generate(call);

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal("DATAVOL002", diagnostic.Id);
        Assert.Equal(DiagnosticSeverity.Warning, diagnostic.Severity);
        Assert.Contains(reason, diagnostic.GetMessage());
        Assert.Empty(generated);
    }

    [Fact]
    public void Diagnostic_PointsAtTheLambda_SoAPragmaCanSilenceIt()
    {
        Diagnostic diagnostic = Assert.Single(Generate("players.FirstOrDefault(p => p.Id == target)").Diagnostics);

        Assert.True(diagnostic.Location.IsInSource);
        Assert.Equal("p => p.Id == target", diagnostic.Location.SourceTree!.GetText().ToString(diagnostic.Location.SourceSpan));
    }

    [Fact]
    public void DelegateBuiltElsewhere_IsLeftAloneSilently()
    {
        (string generated, Diagnostic[] diagnostics) = Generate("players.Where(new Func<Player, bool>(p => p.Level > 1))");

        Assert.Empty(diagnostics);
        Assert.Empty(generated);
    }

    [Fact]
    public void OtherWhereCalls_AreIgnored()
    {
        (string generated, Diagnostic[] diagnostics) = Generate("System.Linq.Enumerable.Where(new[] { 1, 2 }, static n => n > target)");

        Assert.Empty(diagnostics);
        Assert.Empty(generated);
    }

    [Fact]
    public void InterceptedProgram_CompilesWithNoErrorsOrWarnings()
    {
        Compilation output = Compile(
            "new object?[] { players.FirstOrDefault(target, static (p, id) => p.Id == id), players.SingleOrDefault(p => p.Id == 4), "
            + "players.Where((text, maybe), static (p, s) => p.Guild == s.text && p.Score > s.maybe).ToList(), "
            + "players.Where(p => p.Id > 1 && p.Id < 9 && p.Level > 1 && p.Level < 9 && p.Gold > 5).FirstOrDefault() }",
            LanguageVersion.Latest,
            out _);

        Assert.Empty(output.GetDiagnostics().Where(static diagnostic => diagnostic.Severity >= DiagnosticSeverity.Warning));
    }

    [Fact]
    public void MoreThanFourValues_AreBoundThroughAnArray()
    {
        string generated = Generate("players.Where(p => p.Id > 1 && p.Id < 9 && p.Level > 1 && p.Level < 9 && p.Gold > 5)").Generated;

        Assert.Contains("@p4", generated);
        Assert.Contains("new global::DataVo.Core.DataVoQueryParameter[]", generated);
    }

    [Theory]
    [InlineData("System.Reflection")]
    [InlineData("Activator")]
    [InlineData("typeof(")]
    [InlineData("dynamic ")]
    [InlineData("Expression<")]
    [InlineData(".Compile(")]
    public void Output_UsesNoRuntimeCodeGeneration(string forbidden)
    {
        Assert.DoesNotContain(forbidden, Generate("players.FirstOrDefault(target, static (p, id) => p.Id == id)").Generated);
    }

    [Fact]
    public void WithoutTheNamespaceEnabled_NothingIsEmitted_AndOneWarningSaysHowToEnableIt()
    {
        Compile(
            "new object?[] { players.FirstOrDefault(p => p.Id == 1), players.FirstOrDefault(p => p.Id == target) }",
            LanguageVersion.Latest,
            out GeneratorRunResult result,
            interceptors: false);

        Assert.Empty(result.GeneratedSources);
        Diagnostic diagnostic = Assert.Single(result.Diagnostics);
        Assert.Equal("DATAVOL003", diagnostic.Id);
        Assert.Contains("<InterceptorsNamespaces>$(InterceptorsNamespaces);DataVo.Generated</InterceptorsNamespaces>", diagnostic.GetMessage());
    }

    [Fact]
    public void OnACompilerWithoutInterceptors_NothingIsEmittedOrReported_AndTheProgramStillCompiles()
    {
        Compilation output = Compile(
            "new object?[] { players.FirstOrDefault(p => p.Id == 1), players.FirstOrDefault(p => p.Id == target) }",
            LanguageVersion.CSharp9,
            out GeneratorRunResult result,
            interceptors: false);

        Assert.Empty(result.GeneratedSources);
        Assert.Empty(result.Diagnostics);
        Assert.Empty(output.GetDiagnostics().Where(static diagnostic => diagnostic.Severity == DiagnosticSeverity.Error));
    }

    [Fact]
    public void PreviewFeatureName_AlsoEnablesInterceptors()
    {
        Compile("players.FirstOrDefault(p => p.Id == 1)", LanguageVersion.Latest, out GeneratorRunResult result, feature: "InterceptorsPreviewNamespaces");

        Assert.Single(result.GeneratedSources);
    }

    [Fact]
    public void OlderCompiler_GetsThePathLineAndColumnForm()
    {
        DataVoLinqAnalyzer.UsePathLocations = true;
        try
        {
            const string Call = "players.FirstOrDefault(p => p.Id == 1)";
            string generated = Generate(Call).Generated;

            // The compiler wants the 1-based line and column of the method name, not of the whole call.
            string[] lines = Model.Replace("BODY", Call).Split('\n');
            int line = Array.FindIndex(lines, static text => text.Contains("FirstOrDefault"));
            int column = lines[line].IndexOf("FirstOrDefault", StringComparison.Ordinal);
            Assert.Contains($"InterceptsLocation(\"Query.cs\", {line + 1}, {column + 1})]", generated);
        }
        finally
        {
            DataVoLinqAnalyzer.UsePathLocations = false;
        }
    }

    [Fact]
    public void UnrelatedEdit_DoesNotRegenerate()
    {
        CSharpParseOptions parse = ParseOptions(LanguageVersion.Latest, "InterceptorsNamespaces");
        CSharpCompilation compilation = CreateCompilation(Model.Replace("BODY", "players.FirstOrDefault(p => p.Id == 1)"), parse);
        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            [new DataVoLinqInterceptorGenerator().AsSourceGenerator()],
            parseOptions: parse,
            driverOptions: new GeneratorDriverOptions(IncrementalGeneratorOutputKind.None, trackIncrementalGeneratorSteps: true));

        driver = driver.RunGenerators(compilation);
        driver = driver.RunGenerators(compilation.AddSyntaxTrees(CSharpSyntaxTree.ParseText("namespace Other { public class Unrelated { } }", parse)));

        IncrementalGeneratorRunStep[] outputs = [.. driver.GetRunResult().Results[0].TrackedOutputSteps.SelectMany(static step => step.Value)];
        Assert.NotEmpty(outputs);
        Assert.All(outputs.SelectMany(static step => step.Outputs), output => Assert.Equal(IncrementalStepRunReason.Cached, output.Reason));
    }

    private static (string Generated, Diagnostic[] Diagnostics) Generate(string call)
    {
        Compile(call, LanguageVersion.Latest, out GeneratorRunResult result);
        return (string.Concat(result.GeneratedSources.Select(static source => source.SourceText.ToString())), [.. result.Diagnostics]);
    }

    private static Compilation Compile(string call, LanguageVersion version, out GeneratorRunResult linq, bool interceptors = true, string feature = "InterceptorsNamespaces")
    {
        CSharpParseOptions parse = ParseOptions(version, interceptors ? feature : null);
        GeneratorDriver driver = CSharpGeneratorDriver.Create(
            [new DataVoTableGenerator().AsSourceGenerator(), new DataVoLinqInterceptorGenerator().AsSourceGenerator()],
            parseOptions: parse);
        driver = driver.RunGeneratorsAndUpdateCompilation(CreateCompilation(Model.Replace("BODY", call), parse), out Compilation output, out _);
        linq = driver.GetRunResult().Results[1];
        return output;
    }

    private static CSharpParseOptions ParseOptions(LanguageVersion version, string? feature)
    {
        var options = new CSharpParseOptions(version);
        return feature is null ? options : options.WithFeatures([new KeyValuePair<string, string>(feature, "Other.Namespace;DataVo.Generated")]);
    }

    private static CSharpCompilation CreateCompilation(string source, CSharpParseOptions parse)
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
