namespace Research.Benchmark.Tests;

public sealed class ArenaLiveViewsDocumentationTests
{
    [Fact]
    public void ArenaDocumentation_StatesReproductionCommandsAndLaneBoundaries()
    {
        string repositoryRoot = FindRepositoryRoot();
        string[] documentationPaths =
        [
            "docs/features/unity-and-godot.md",
            "docs/manual/performance/reproducing-benchmarks.md",
            "demos/Research.Benchmark/README.md"
        ];

        string[] requiredText =
        [
            "--scenario arena-live-views",
            "bash scripts/run-unity-macos-il2cpp-arena-cross-engine.sh",
            "gameplay",
            "ivm-stress",
            "Desktop .NET lane",
            "Unity macOS IL2CPP lane",
            "DuckDB is desktop-only",
            "do not compare Unity IL2CPP values directly with desktop .NET values"
        ];

        foreach (string documentationPath in documentationPaths)
        {
            string documentation = File.ReadAllText(Path.Combine(repositoryRoot, documentationPath));

            foreach (string text in requiredText)
            {
                Assert.Contains(text, documentation, StringComparison.Ordinal);
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
