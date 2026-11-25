namespace DataVo.Tests;

/// <summary>
/// Tests that assert an exact allocation count with <see cref="GC.GetAllocatedBytesForCurrentThread"/>. They run
/// alone, after the parallel collections, so background tier-up and the thread pool work of other tests do not land
/// inside a measured window; each test also runs its workload at least twice before measuring.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class AllocationSensitiveCollection
{
    /// <summary>The collection name used by <c>[Collection(AllocationSensitiveCollection.Name)]</c>.</summary>
    public const string Name = "AllocationSensitive";
}
