namespace DataVo.Core.Models.Statement.Utils;

/// <summary>
/// Represents the collection of rows fetched from a table, keyed by their physical RowKey RowId.
/// </summary>
public class TableData : Dictionary<RowKey, Record>
{
}
