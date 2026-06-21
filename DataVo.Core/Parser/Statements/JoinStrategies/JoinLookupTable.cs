namespace DataVo.Core.Parser.Statements.JoinStrategies;

using DataVo.Core.Models.Statement.Utils;

/// <summary>
/// Represents a structured hash map used to optimize join condition lookups.
/// Maps boxed column-value keys to a list of matching records for rapid retrieval.
/// </summary>
/// <remarks>
/// Keys are compared with the default object equality comparer (the same semantics the previous
/// <c>Dictionary&lt;dynamic, ...&gt;</c> used at runtime), which keeps this Native-AOT safe (no DLR).
/// </remarks>
public class JoinLookupTable : Dictionary<object, List<Record>>
{
    /// <summary>
    /// Appends a new record to the lookup table under the given key.
    /// Safely initializes the generic list if the key is not already present.
    /// </summary>
    /// <param name="key">The lookup identifier (e.g., boxed column value) used for grouping.</param>
    /// <param name="record">The actual data record associated with the mapped key.</param>
    public void AddRecord(object key, Record record)
    {
        if (!ContainsKey(key))
        {
            this[key] = new List<Record>();
        }

        this[key].Add(record);
    }
}
