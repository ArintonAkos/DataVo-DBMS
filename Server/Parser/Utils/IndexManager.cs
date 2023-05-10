using Server.Server.MongoDB;

namespace Server.Parser.Utils;

public class IndexManager
{
    public static string GetBestIndex(List<string> columns)
    {
        // This method should return the best index for the given columns.
        // You can implement your own logic to determine the best index based on your needs.
        // For now, this method returns the first available index.

        List<string> availableIndexes = DbContext.Instance.GetIndexes(columns);

        if (availableIndexes.Count > 0) return availableIndexes.First();

        return string.Empty;
    }
}