namespace DataVo.Core.Parser.Statements.JoinStrategies;

using DataVo.Core.Models.Statement.Utils;

public class JoinLookupTable : Dictionary<dynamic, List<Record>>
{
    public void AddRecord(dynamic key, Record record)
    {
        if (!ContainsKey(key))
        {
            this[key] = new List<Record>();
        }
        this[key].Add(record);
    }
}
