#if NETSTANDARD2_1
namespace DataVo.Core.Indexing;

public partial class IndexManager
{
    private readonly object _lock = new();
}
#endif
