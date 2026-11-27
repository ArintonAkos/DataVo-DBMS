using System.Text.RegularExpressions;

namespace Research.Benchmark.Tests;

/// <summary>
/// Verifies that active documentation pages covering the Arena live-views benchmark and Unity evaluation
/// state the reproduction commands and the boundaries between the desktop .NET and Unity macOS IL2CPP lanes.
/// Commands are checked verbatim; lane-boundary facts are checked by meaning.
/// </summary>
public sealed class ArenaLiveViewsDocumentationTests
{
    private const string UnityRoadmapPath = "docs/manual/getting-started/roadmap.md";

    private static readonly string[] BenchmarkDocumentationPaths =
    [
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

    private static readonly string[] RequiredUnityRoadmapBoundaries =
    [
        "Unity is an **evaluation target**, not a supported runtime.",
        "| Unity 6000.5.4f1 Editor (macOS) | Narrow smoke pass |",
        "| macOS IL2CPP Player | Narrow smoke pass |",
        "| Windows x64 IL2CPP | Unverified |",
        "| Direct Burst calls | Unsupported by design |",
    ];

    /// <summary>
    /// Ensures every Arena benchmark reproduction page states the exact CLI commands and lane-comparison rules.
    /// </summary>
    [Fact]
    public void ArenaDocumentation_StatesReproductionCommandsAndLaneBoundaries()
    {
        string repositoryRoot = FindRepositoryRoot();

        foreach (string documentationPath in BenchmarkDocumentationPaths)
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

    /// <summary>
    /// Ensures the status and roadmap manual page keeps the Unity evaluation boundaries up to date.
    /// </summary>
    [Fact]
    public void UnityEvaluationRoadmap_StatesUnityLaneBoundaries()
    {
        string repositoryRoot = FindRepositoryRoot();
        string roadmap = File.ReadAllText(Path.Combine(repositoryRoot, UnityRoadmapPath));

        foreach (string boundary in RequiredUnityRoadmapBoundaries)
        {
            Assert.Contains(boundary, roadmap, StringComparison.Ordinal);
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
