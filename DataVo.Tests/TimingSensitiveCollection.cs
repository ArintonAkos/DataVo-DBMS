namespace DataVo.Tests;

/// <summary>
/// Tests that assert progress inside fixed time windows. They run alone so the load they create themselves is the
/// only load on the machine, and a stalled window points at the engine rather than at other test classes.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class TimingSensitiveCollection
{
    /// <summary>The collection name used by <c>[Collection(TimingSensitiveCollection.Name)]</c>.</summary>
    public const string Name = "TimingSensitive";
}
