namespace DataVo.Core.Models.Statement.Utils;

public class Record(long rowId, Dictionary<string, dynamic> values)
{
    public long RowId { get; set; } = rowId;
    public Dictionary<string, dynamic> Values { get; set; } = values;

    public dynamic this[string columnName]
    {
        get => Values[columnName];
        set => Values[columnName] = value;
    }

    public bool ContainsKey(string key) => Values.ContainsKey(key);
}
