using System.Text.RegularExpressions;

namespace Research.Benchmark.Tests;

/// <summary>
/// Every page that documents the Arena live-views benchmark must give its reproduction commands and state the
/// boundaries between the desktop .NET lane and the Unity macOS IL2CPP lane. Commands are checked verbatim; the
/// lane-boundary facts are checked by meaning, so a page may phrase them in its own words.
/// </summary>
public sealed class ArenaLiveViewsDocumentationTests
{
    private static readonly string[] DocumentationPaths =
    [
        "docs/features/unity-and-godot.md",
        "docs/manual/performance/reproducing-benchmarks.md",
        "demos/Research.Benchmark/README.md",
    ];

    private static readonly string[] RequiredText =
    [
        "--scenario arena-live-views",
        "bash scripts/run-unity-macos-il2cpp-arena-cross-engine.sh",
        "gameplay",
        "ivm-stress",
        "Desktop .NET lane",
        "Unity macOS IL2CPP lane",
    ];

    private static readonly (string Fact, Regex Pattern)[] RequiredFacts =
    [
        // The Unity cross-engine lane runs DataVo and SQLite only (see the engine loop in
        // scripts/run-unity-macos-il2cpp-arena-cross-engine.sh), so DuckDB results exist only on desktop .NET.
        ("DuckDB runs only in the desktop .NET lane",
            new Regex(@"DuckDB runs only in the desktop \.NET lane", RegexOptions.IgnoreCase)),
        ("results of the two lanes are not comparable",
            new Regex(@"do not compare Unity IL2CPP (values|timings) directly with desktop \.NET (values|timings)", RegexOptions.IgnoreCase)),
    ];

    [Fact]
    public void ArenaDocumentation_StatesReproductionCommandsAndLaneBoundaries()
    {
        string repositoryRoot = FindRepositoryRoot();

        foreach (string documentationPath in DocumentationPaths)
        {
            string documentation = File.ReadAllText(Path.Combine(repositoryRoot, documentationPath));

            foreach (string text in RequiredText)
            {
                Assert.Contains(text, documentation, StringComparison.Ordinal);
            }

            foreach ((string fact, Regex pattern) in RequiredFacts)
            {
                Assert.True(pattern.IsMatch(documentation), $"{documentationPath} does not state that {fact}.");
            }
        }
    }

    private static string FindRepositoryRoot()
    {
        for (DirectoryInfo? directory = new(Directory.GetCurrentDirectory()); directory is not null; directory = directory.Parent)
        {
            if (Directory.Exists(Path.Combine(directory.FullName, ".git")))
            {
                return directory.FullName;
            }
        }

        throw new DirectoryNotFoundException("Could not find the repository root.");
    }
}
