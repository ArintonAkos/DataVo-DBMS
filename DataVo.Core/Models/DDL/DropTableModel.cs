using System.Text.RegularExpressions;
using DataVo.Core.Utils;

namespace DataVo.Core.Models.DDL;

internal class DropTableModel
{
    public DropTableModel(string databaseName) => TableName = databaseName;

    public string TableName { get; set; }

    public static DropTableModel FromMatch(Match match) => new(match.NthGroup(n: 1).Value);
}