using Server.Models.Catalog;
using Server.Models.Statement;

namespace Server.Parser.Utils;

public class IndexManager
{
    private readonly string _databaseName;
    private readonly string _tableName;

    public IndexManager(string databaseName, string tableName)
    {
        _databaseName = databaseName;
        _tableName = tableName;
    }

    public string GetBestIndex(List<string> columns, IEnumerable<Node> expressionNodes)
    {
        List<IndexFile> availableIndexes =
            Catalog.GetTableIndexes(_tableName, _databaseName);

        if (!expressionNodes.Any())
        {
            return string.Empty;
        }

        int maxColumnsMatched = 0;
        string bestIndexFileName = string.Empty;

        foreach (var index in availableIndexes)
        {
            int columnsMatched = columns.Intersect(index.AttributeNames).Count();

            if (columnsMatched > maxColumnsMatched)
            {
                maxColumnsMatched = columnsMatched;
                bestIndexFileName = index.IndexFileName;
            }
        }

        return bestIndexFileName;
    }
}