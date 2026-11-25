namespace DataVo.Generators.Tests;

/// <summary>
/// Test classes that run the LINQ interceptor generator. Some of them switch the process-wide
/// <c>DataVoLinqAnalyzer.UsePathLocations</c> flag, so they must not run in parallel with each other.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class LinqGeneratorCollection
{
    /// <summary>The collection name used by <c>[Collection(LinqGeneratorCollection.Name)]</c>.</summary>
    public const string Name = "LinqGenerator";
}
