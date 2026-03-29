namespace DataVo.Core.Models.Statement.Utils;

public class Record(long rowId, Dictionary<string, object?> values)
{
    public long RowId { get; set; } = rowId;
    public Dictionary<string, object?> Values { get; set; } = values;

    public object? this[string columnName]
    {
        get => Values[columnName];
        set => Values[columnName] = value;
    }

    public bool ContainsKey(string key) => Values.ContainsKey(key);
}
