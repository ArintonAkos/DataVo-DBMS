namespace DataVo.Core.Models.Statement.Utils;

/// <summary>
/// Represents the collection of rows fetched from a table, keyed by their physical long RowId.
/// </summary>
public class TableData : Dictionary<long, Record>
{
}
