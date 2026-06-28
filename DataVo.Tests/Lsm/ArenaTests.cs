using DataVo.Core.StorageEngine.Lsm;

namespace DataVo.Tests.Lsm;

public class ArenaTests
{
    [Fact]
    public void Allocate_ReturnsRequestedSize_AndAccumulatesBytes()
    {
        using var arena = new Arena(slabSize: 1024);

        Span<byte> a = arena.Allocate(10);
        Span<byte> b = arena.Allocate(20);

        Assert.Equal(10, a.Length);
        Assert.Equal(20, b.Length);
        Assert.Equal(30, arena.BytesAllocated);
    }

    [Fact]
    public void Allocate_PreservesWrittenBytesAcrossCalls()
    {
        using var arena = new Arena(slabSize: 1024);

        Span<byte> a = arena.Allocate(4);
        a[0] = 1; a[1] = 2; a[2] = 3; a[3] = 4;
        Span<byte> b = arena.Allocate(4);
        b[0] = 9;

        // The first allocation's bytes are not disturbed by the second.
        Assert.Equal(1, a[0]);
        Assert.Equal(4, a[3]);
        Assert.Equal(9, b[0]);
    }

    [Fact]
    public void Allocate_CrossingSlabBoundary_StillSucceeds()
    {
        using var arena = new Arena(slabSize: 16);

        Span<byte> first = arena.Allocate(12);
        Span<byte> second = arena.Allocate(12); // forces a new slab

        Assert.Equal(12, first.Length);
        Assert.Equal(12, second.Length);
        Assert.Equal(24, arena.BytesAllocated);
    }

    [Fact]
    public void Allocate_OversizedRequest_GetsDedicatedSlab()
    {
        using var arena = new Arena(slabSize: 16);

        Span<byte> big = arena.Allocate(100);

        Assert.Equal(100, big.Length);
        Assert.Equal(100, arena.BytesAllocated);
    }

    [Fact]
    public void Reset_ZeroesCounter_AndArenaIsReusable()
    {
        using var arena = new Arena(slabSize: 64);
        arena.Allocate(40);

        arena.Reset();

        Assert.Equal(0, arena.BytesAllocated);
        Span<byte> after = arena.Allocate(8);
        Assert.Equal(8, after.Length);
        Assert.Equal(8, arena.BytesAllocated);
    }

    [Fact]
    public void Reset_WithMultipleSlabs_ReturnsAllAndRearmsOne()
    {
        using var arena = new Arena(slabSize: 16);
        arena.Allocate(12);
        arena.Allocate(12); // forces a second slab

        arena.Reset();

        Assert.Equal(0, arena.BytesAllocated);
        Assert.Equal(8, arena.Allocate(8).Length); // arena is live with exactly one fresh slab
        Assert.Equal(8, arena.BytesAllocated);
    }

    [Fact]
    public void Allocate_WithinSlab_IsAllocationFreeInSteadyState()
    {
        using var arena = new Arena(slabSize: 1 << 20);

        // Warm: prime the pool and JIT.
        for (int i = 0; i < 200; i++)
        {
            arena.Reset();
            _ = arena.Allocate(32);
        }

        arena.Reset();
        const int n = 10_000;
        long before = GC.GetAllocatedBytesForCurrentThread();
        for (int i = 0; i < n; i++)
        {
            _ = arena.Allocate(32); // 10_000 * 32 = 320 KB, well within the 1 MB slab
        }
        long perOp = (GC.GetAllocatedBytesForCurrentThread() - before) / n;

        Assert.True(perOp == 0, $"Arena.Allocate within a slab allocated {perOp} B/op (expected 0)");
    }
}
