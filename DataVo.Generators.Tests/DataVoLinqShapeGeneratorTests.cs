using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using static DataVo.Generators.Tests.LinqGeneratorHarness;

namespace DataVo.Generators.Tests;

/// <summary>
/// What the interceptor generator does with sort keys, <c>Select</c> projections and <c>ExecuteUpdate</c> setters:
/// what it compiles, what it leaves to run in memory, and what it refuses.
/// </summary>
public class DataVoLinqShapeGeneratorTests
{
    private const string Core = "global::DataVo.Core.";
    private const string Support = "global::DataVo.Core.Annotations.DataVoGeneratedTableSupport";

    /// <summary>A program that uses every compiled shape, on every receiver and model kind.</summary>
    private const string EveryShape =
        "new object?[] { "
        + "players.Where(p => p.IsActive).OrderByDescending(p => p.Gold).ThenBy(p => p.Name).Skip(1).Take(2).Select(p => new { p.Name, Rich = p.Gold > Limits.Max, Tag = Fmt.Tag(p.Name, p.Level) }).ToList(), "
        + "players.OrderBy(p => p.Level).Select(p => new PlayerCard(p.Id, p.Name.ToUpperInvariant())).FirstOrDefault(), "
        + "players.Select(p => new PlayerRow { Id = p.Id, Name = $\"{p.Name}#{p.Level:D3}\", Wealth = p.Gold * 2 }).ToList(), "
        + "players.Select(p => (p.Id, Total: p.Gold + p.Level, R: p.Level switch { > 50 => Rank.High, _ => Rank.Low }, L: p.Guild?.Length, C: p.Name.Count())).ToList(), "
        + "players.Take(3).Select(p => (p.Id, new { p.Score })).ToList(), "
        + "db.Table<Card>().Select(c => new { c.Title, c.Bonus }).ToList(), "
        + "db.Table<Mark>().OrderBy(m => m.Zone).Select(m => m.Zone).ToList(), "
        + "db.Table<Scroll>().OrderByDescending(s => s.Weight).Select(s => s.Weight * 2).ToList(), "
        + "players.Where(target, static (p, t) => p.Level > t).ExecuteUpdate(target, static (s, d) => s.Set(p => p.Level, p => p.Level + d).Set(p => p.Guild, \"x\").SetNull(p => p.Score)), "
        + "players.ExecuteUpdate(text, static (s, t) => s.Set(p => p.Guild, t!).Set(p => p.Seen, (DateOnly?)null)), "
        + "db.Table<Card>().ExecuteUpdate(s => s.Set(c => c.Cost, c => (c.Cost - 1) * 2)), "
        + "players.Where(p => p.Id > 3).ExecuteDelete() }";

    [Theory]
    [InlineData("players.OrderBy(p => p.Level)", "OrderBy0", "\"Level\", false", "OrderByColumn", "DataVoTable<global::App.Player> source", "int")]
    [InlineData("players.OrderByDescending(p => (p.Gold))", "OrderByDescending0", "\"Gold\", true", "OrderByColumn", "DataVoTable<global::App.Player> source", "long")]
    [InlineData("players.Where(p => p.IsActive).OrderBy(static p => p.Guild)", "OrderBy0", "\"Guild\", false", "OrderByColumn", "in global::DataVo.Core.DataVoQuery<global::App.Player, global::System.Func<global::App.Player, bool>> source", "string?")]
    [InlineData("players.OrderBy(p => p.Id).ThenByDescending(p => p.Score)", "ThenByDescending0", "\"Score\", true", "ThenByColumn", "in global::DataVo.Core.DataVoOrderedQuery<global::App.Player, global::System.Func<global::App.Player, bool>> source", "int?")]
    [InlineData("players.OrderBy(p => p.Id).ThenBy(p => p.Joined)", "ThenBy0", "\"Joined\", false", "ThenByColumn", "DataVoOrderedQuery", "global::DataVo.Core.DataVoDate")]
    public void SortKeyOnOneColumn_BecomesACompiledOrderKey(string call, string method, string key, string runtime, string receiver, string keyType)
    {
        (string generated, Diagnostic[] diagnostics) = Generate(call);

        Assert.Empty(diagnostics);
        Assert.Contains($"new {Core}DataVoOrderKey({key});", generated);
        Assert.Contains($" {method}(this ", generated);
        Assert.Contains(receiver, generated);
        Assert.Contains($"global::System.Func<global::App.Player, {keyType}> keySelector)", generated);
        Assert.Contains($"return source.{runtime}(OrderKey0, keySelector);", generated);
    }

    [Theory]
    [InlineData("players.OrderBy(p => p.Level * 2)", "is not one column")]
    [InlineData("players.OrderBy(p => p.Name.Length)", "is not one column")]
    [InlineData("players.OrderBy<long>(p => p.Level)", "converts column Level to long")]
    [InlineData("players.OrderBy(p => p)", "is not one column")]
    [InlineData("players.OrderBy(p => { return p.Level; })", "one expression")]
    [InlineData("players.OrderBy(p => p.Id).ThenBy(p => -p.Gold)", "is not one column")]
    public void OtherSortKeys_WarnAndSortInMemory(string call, string reason)
    {
        (string generated, Diagnostic[] diagnostics) = Generate(call);

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal("DATAVOL004", diagnostic.Id);
        Assert.Equal(DiagnosticSeverity.Warning, diagnostic.Severity);
        Assert.Contains(reason, diagnostic.GetMessage());
        Assert.DoesNotContain("DataVoOrderKey(\"", generated.Replace("OrderKey0 = new global::DataVo.Core.DataVoOrderKey(\"Id\"", string.Empty));
    }

    [Theory]
    [InlineData("players.OrderBy(p => p.Name, StringComparer.Ordinal)")]
    [InlineData("players.OrderBy(new Func<Player, int>(p => p.Level))")]
    [InlineData("System.Linq.Enumerable.OrderBy(new[] { 1, 2 }, static n => n)")]
    public void KeysWithAComparerOrBuiltElsewhere_AreLeftAloneSilently(string call)
    {
        (string generated, Diagnostic[] diagnostics) = Generate(call);

        Assert.Empty(diagnostics);
        Assert.DoesNotContain("DataVoOrderKey", generated);
    }

    [Theory]
    [InlineData("players.Select(p => p.Name)", "\"Name\"", $"return ({Support}.ReadString(row[\"Name\"], \"Name\"));")]
    [InlineData("players.Select(p => p.Score ?? -1)", "\"Score\"", $"return ({Support}.ReadNullableInt32(row[\"Score\"], \"Score\")) ?? (-1);")]
    [InlineData("players.Select(p => new PlayerCard(p.Id, p.Name))", "\"Id\", \"Name\"", "return new global::App.PlayerCard((")]
    [InlineData("players.Select(p => new PlayerRow { Id = p.Id, Wealth = p.Gold })", "\"Id\", \"Gold\"", "return new global::App.PlayerRow() { Id = (")]
    [InlineData("players.Select(p => (p.Id, Total: p.Gold * 2))", "\"Id\", \"Gold\"", ", Total: (")]
    [InlineData("players.Select(p => Fmt.Tag(p.Name, Limits.Max))", "\"Name\"", "return global::App.Fmt.Tag((")]
    [InlineData("players.Select(p => p.Name.Count())", "\"Name\"", "return global::System.Linq.Enumerable.Count((")]
    [InlineData("players.Select(p => p.Level > Limits.Soft ? Rank.High : Rank.Low)", "\"Level\"", "global::App.Limits.Soft ? ((global::App.Rank)(2)) : ((global::App.Rank)(1))")]
    [InlineData("players.Select(p => p.Guild?.Trim().Length)", "\"Guild\"", "?.Trim().Length")]
    [InlineData("players.Select(p => $\"{p.Name}:{p.Gold:N0}\")", "\"Name\", \"Gold\"", ":N0}\"")]
    [InlineData("players.Select(p => p.Seen)", "\"Seen\"", $"{Support}.ReadNullableDate(row[\"Seen\"], \"Seen\")?.ToDateOnly()")]
    [InlineData("players.Select(p => p.Level + p.Level)", "\"Level\" }", "Level")]
    [InlineData("players.Select(p => 42)", "\"Id\"", "return (42);")]
    [InlineData("players.Select(p => p.Level is > 10 and < 20 or 99)", "\"Level\"", " is > (10) and < (20) or (99))")]
    [InlineData("players.Select(p => (long)p.Level)", "\"Level\"", "return ((long)((")]
    public void ProjectionWithANameableResult_ReadsOnlyItsColumns(string call, string columns, string body)
    {
        (string generated, Diagnostic[] diagnostics) = Generate(call);

        Assert.Empty(diagnostics);
        Assert.Contains($"new string[] {{ {columns}", generated);
        Assert.Contains(body, generated);
        Assert.Contains("(global::DataVo.Core.DataVoRow row)", generated);
        Assert.Contains("return source.SelectColumns(Projection0, selector);", generated);
    }

    [Theory]
    [InlineData("players.Select(p => new { p.Id, Twice = p.Gold * 2 })", "Select0<T, TResult>(this global::DataVo.Core.DataVoTable<T> source", "new { Id = (")]
    [InlineData("players.Where(p => p.IsActive).Select(p => new { p.Name })", "Select0<T, TState, TResult>(this in global::DataVo.Core.DataVoQuery<T, TState> source", "new { Name = (")]
    [InlineData("players.OrderBy(p => p.Id).Take(2).Select(p => (p.Id, new { p.Score }))", "Select0<T, TState, TResult>(this in global::DataVo.Core.DataVoPagedQuery<T, TState> source", "new { Score = (")]
    public void AnonymousProjection_GetsAGenericInterceptor(string call, string signature, string body)
    {
        (string generated, Diagnostic[] diagnostics) = Generate(call);

        Assert.Empty(diagnostics);
        Assert.Contains(signature, generated);
        Assert.Contains("static row => (TResult)(object)(", generated);
        Assert.Contains(body, generated);
        Assert.Contains("return source.SelectColumns(Projection0<T, TResult>.Instance, selector);", generated);
    }

    [Theory]
    [InlineData("players.Select(p => p)", "uses the row 'p' itself")]
    [InlineData("players.Select(p => p.ToString())", "uses the row 'p' itself")]
    [InlineData("players.Select(p => p.Level * target)", "captures 'target'")]
    [InlineData("players.Select(p => p.Name.Select(c => c).Count())", "lambda inside the selector")]
    [InlineData("players.Select(p => { return p.Level; })", "one expression")]
    [InlineData("players.Select(p => new[] { p.Level }.Length + target)", "captures 'target'")]
    public void UncompilableProjection_WarnsAndBuildsModels(string call, string reason)
    {
        (string generated, Diagnostic[] diagnostics) = Generate(call);

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal("DATAVOL004", diagnostic.Id);
        Assert.Contains("Select", diagnostic.GetMessage());
        Assert.Contains(reason, diagnostic.GetMessage());
        Assert.DoesNotContain("SelectColumns", generated);
    }

    [Fact]
    public void ProjectionBuiltElsewhere_IsLeftAloneSilently()
    {
        (string generated, Diagnostic[] diagnostics) = Generate("players.Select(new Func<Player, int>(p => p.Level))");

        Assert.Empty(diagnostics);
        Assert.Empty(generated);
    }

    [Theory]
    [InlineData("players.ExecuteUpdate(s => s.Set(p => p.Gold, 5))", "Gold = @u0", "new global::DataVo.Core.DataVoQueryParameter(\"u0\", 5L)")]
    [InlineData("players.ExecuteUpdate(s => s.SetNull(p => p.Guild).Set(p => p.IsActive, false))", "Guild = NULL, IsActive = @u0", "(\"u0\", false)")]
    [InlineData("players.Where(p => p.IsActive).ExecuteUpdate(target, static (s, d) => s.Set(p => p.Level, p => (p.Level + d) * 2))", "Level = (Level + @u0) * @u1", "(\"u0\", (int)(state))")]
    [InlineData("players.ExecuteUpdate(text, static (s, t) => s.Set(p => p.Guild, t))", "Guild = @u0", "(state) == null ? global::DataVo.Core.DataVoQueryParameter.Null(\"u0\") : new global::DataVo.Core.DataVoQueryParameter(\"u0\", (string)(state))")]
    [InlineData("players.ExecuteUpdate(maybe, static (s, m) => s.Set(p => p.Score, m))", "Score = @u0", "(state) == null ? global::DataVo.Core.DataVoQueryParameter.Null(\"u0\") : new global::DataVo.Core.DataVoQueryParameter(\"u0\", (int)(state))")]
    [InlineData("players.ExecuteUpdate((A: 1.5f, B: 2L), static (s, v) => s.Set(p => p.Speed, p => p.Speed * v.A).Set(p => p.Gold, v.B))", "Speed = Speed * @u0, Gold = @u1", "(\"u1\", (long)(state.B))")]
    [InlineData("players.ExecuteUpdate(s => s.Set(p => p.Seen, DateOnly.MinValue))", "Seen = @u0", "global::DataVo.Core.DataVoQueryParameter.FromObject(\"u0\", (global::System.DateOnly?)(global::System.DateOnly.MinValue))")]
    [InlineData("players.ExecuteUpdate(s => s.Set(p => p.Level, Limits.Soft))", "Level = @u0", "(\"u0\", (int)(global::App.Limits.Soft))")]
    [InlineData("players.ExecuteUpdate(s => s.Set(p => p.Name, p => p.Guild ?? \"\"))", "", "")]
    public void UpdateSetters_BecomeOneParameterizedSetList(string call, string assignments, string binding)
    {
        (string generated, Diagnostic[] diagnostics) = Generate(call);
        if (assignments.Length == 0)
        {
            Assert.Equal("DATAVOL005", Assert.Single(diagnostics).Id); // ?? is not SQL arithmetic
            return;
        }

        Assert.Empty(diagnostics);
        Assert.Contains($"return source.ExecuteUpdateSet(\"{assignments}\", ", generated);
        Assert.Contains(binding, generated);
        Assert.Contains("public static int ExecuteUpdate0(this ", generated);
    }

    [Theory]
    [InlineData("players.ExecuteUpdate(s => s.Set(p => p.Level, target))", "capture 'target'")]
    [InlineData("players.ExecuteUpdate(s => s.Set(p => p.Id, 1))", "primary key Id")]
    [InlineData("players.ExecuteUpdate(s => s.Set(p => p.Level, 1).Set(p => p.Level, 2))", "sets column Level twice")]
    [InlineData("players.ExecuteUpdate(s => s.SetNull(p => p.Level))", "column Level cannot hold null")]
    [InlineData("players.ExecuteUpdate(s => s.Set(p => p.Level, p => p.Level / 2))", "is not a column, a value")]
    [InlineData("players.ExecuteUpdate(s => s.Set(p => p.Gold, p => p.Gold + p.Level))", "mixes column Level")]
    [InlineData("players.ExecuteUpdate(s => s.Set<long>(p => p.Level, 1L))", "use the column's own type int")]
    [InlineData("players.ExecuteUpdate(s => s.Set(p => p.Name, p => p.Name + \"!\"))", "is not a column, a value")]
    [InlineData("players.ExecuteUpdate(s => s)", "sets no column")]
    [InlineData("players.ExecuteUpdate(s => s.Set(p => p.Level + 1, 1))", "does not name a column")]
    [InlineData("players.ExecuteUpdate(new { A = 1 }, static (s, v) => s.Set(p => p.Level, v.A))", "cannot name")]
    [InlineData("players.ExecuteUpdate(Setters.Gold)", "as a lambda")]
    public void UntranslatableUpdate_FailsTheBuild(string call, string reason)
    {
        (string generated, Diagnostic[] diagnostics) = Generate(call);

        Diagnostic diagnostic = Assert.Single(diagnostics);
        Assert.Equal("DATAVOL005", diagnostic.Id);
        Assert.Equal(DiagnosticSeverity.Error, diagnostic.Severity);
        Assert.Contains(reason, diagnostic.GetMessage());
        Assert.DoesNotContain("ExecuteUpdateSet", generated);
    }

    [Fact]
    public void EveryShape_CompilesWithNoErrorsOrWarnings()
    {
        Compilation output = Compile(EveryShape, LanguageVersion.Latest, out GeneratorRunResult result);

        Assert.Empty(result.Diagnostics);
        Assert.Empty(output.GetDiagnostics().Where(static diagnostic => diagnostic.Severity >= DiagnosticSeverity.Warning));
    }

    [Fact]
    public void EveryShape_CompilesWithThePathLocationForm()
    {
        Linq.DataVoLinqAnalyzer.UsePathLocations = true;
        try
        {
            Compilation output = Compile(EveryShape, LanguageVersion.CSharp12, out GeneratorRunResult result);

            Assert.Contains("InterceptsLocation(\"Query.cs\", ", result.GeneratedSources[0].SourceText.ToString());
            Assert.Empty(output.GetDiagnostics().Where(static diagnostic => diagnostic.Severity == DiagnosticSeverity.Error));
        }
        finally
        {
            Linq.DataVoLinqAnalyzer.UsePathLocations = false;
        }
    }

    [Theory]
    [InlineData("System.Reflection")]
    [InlineData("Activator")]
    [InlineData("typeof(")]
    [InlineData("dynamic ")]
    [InlineData("Expression<")]
    [InlineData("System.Linq.Expressions")]
    [InlineData(".Compile(")]
    public void EveryShape_UsesNoRuntimeCodeGeneration(string forbidden)
    {
        Assert.DoesNotContain(forbidden, Generate(EveryShape).Generated);
    }

    [Fact]
    public void OnACompilerWithoutInterceptors_EveryShapeCompiles_WithNothingEmittedOrReported()
    {
        Compilation output = Compile(EveryShape, LanguageVersion.CSharp9, out GeneratorRunResult result, interceptors: false);

        Assert.Empty(result.GeneratedSources);
        Assert.Empty(result.Diagnostics);
        Assert.Empty(output.GetDiagnostics().Where(static diagnostic => diagnostic.Severity == DiagnosticSeverity.Error));
    }

    [Fact]
    public void WithInterceptorsDisabled_EachUpdateFailsTheBuild_AndOneWarningSaysHowToEnableThem()
    {
        Compile(
            "new object?[] { players.OrderBy(p => p.Level).ToList(), players.ExecuteUpdate(s => s.Set(p => p.Gold, 1)), players.Where(p => p.IsActive).ExecuteUpdate(s => s.SetNull(p => p.Guild)) }",
            LanguageVersion.Latest,
            out GeneratorRunResult result,
            interceptors: false);

        Assert.Empty(result.GeneratedSources);
        Assert.Single(result.Diagnostics, static diagnostic => diagnostic.Id == "DATAVOL003");
        Diagnostic[] updates = [.. result.Diagnostics.Where(static diagnostic => diagnostic.Id == "DATAVOL005")];
        Assert.Equal(2, updates.Length);
        Assert.All(updates, static diagnostic => Assert.Contains("interceptors are not enabled", diagnostic.GetMessage()));
    }

    [Fact]
    public void UnrelatedEdit_DoesNotRegenerateShapes()
    {
        CSharpParseOptions parse = ParseOptions(LanguageVersion.Latest, "InterceptorsNamespaces");
        CSharpCompilation compilation = CreateCompilation(Model.Replace("BODY", EveryShape), parse);
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
}
