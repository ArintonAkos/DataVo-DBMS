#if !NET8_0_OR_GREATER
namespace DataVo.Core.Indexing;

internal sealed class GuidSimdEqualityComparer : IEqualityComparer<Guid>
{
    public static readonly GuidSimdEqualityComparer Instance = new();

    private GuidSimdEqualityComparer()
    {
    }

    public bool Equals(Guid x, Guid y) => x.Equals(y);

    public int GetHashCode(Guid obj) => obj.GetHashCode();
}
#endif
