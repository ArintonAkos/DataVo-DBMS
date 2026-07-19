#if NET10_0_OR_GREATER
namespace DataVo.Core.Indexing;

public partial class IndexManager
{
    private readonly System.Threading.Lock _lock = new();
}
#endif
