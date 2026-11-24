#if !NET9_0_OR_GREATER
namespace DataVo.Core.Indexing;

public partial class IndexManager
{
    private readonly object _lock = new();
}
#endif
