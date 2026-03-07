namespace DataVo.Core.Parser.Statements.JoinStrategies;

using DataVo.Core.Models.Statement.Utils;

/// <summary>
/// Represents a structured hash map used to optimize join condition lookups.
/// Maps dynamic keys to a list of matching records for rapid retrieval.
/// </summary>
public class JoinLookupTable : Dictionary<dynamic, List<Record>>
{
    /// <summary>
    /// Appends a new record to the lookup table under the given key.
    /// Safely initializes the generic list if the key is not already present.
    /// </summary>
    /// <param name="key">The dynamic lookup identifier (e.g., column value) used for grouping.</param>
    /// <param name="record">The actual data record associated with the mapped key.</param>
    public void AddRecord(dynamic key, Record record)
    {
        if (!ContainsKey(key))
        {
            this[key] = new List<Record>();
        }

        this[key].Add(record);
    }
}
