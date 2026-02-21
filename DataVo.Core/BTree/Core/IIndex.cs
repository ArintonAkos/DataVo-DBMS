namespace DataVo.Core.BTree.Core;

public interface IIndex
{
    void Insert(string key, string rowId);
    void DeleteValues(List<string> rowIds);
    List<string> Search(string key);
    bool ContainsValue(string rowId);
    void Save(string filePath);
}
