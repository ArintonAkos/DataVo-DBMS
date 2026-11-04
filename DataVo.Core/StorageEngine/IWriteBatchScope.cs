namespace DataVo.Core.StorageEngine;

/// <summary>
/// One statement's (or one transaction commit's) durable write unit. Disposing a completed scope
/// commits and publishes its mutations; disposing it without <see cref="Complete"/> discards them.
/// </summary>
internal interface IWriteBatchScope : IDisposable
{
    /// <summary>Marks the unit as successful, so disposal commits it instead of discarding it.</summary>
    void Complete();
}
