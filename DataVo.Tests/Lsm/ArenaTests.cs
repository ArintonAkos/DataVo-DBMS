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

    [Fact]
    public void Allocate_AfterDispose_Throws()
    {
        var arena = new Arena(slabSize: 64);
        arena.Dispose();
        Assert.Throws<ObjectDisposedException>(() => arena.Allocate(8));
    }

    [Fact]
    public void Reset_AfterDispose_Throws()
    {
        var arena = new Arena(slabSize: 64);
        arena.Dispose();
        Assert.Throws<ObjectDisposedException>(() => arena.Reset());
    }

    [Fact]
    public void Dispose_IsIdempotent()
    {
        var arena = new Arena(slabSize: 64);
        arena.Allocate(8);
        arena.Dispose();
        arena.Dispose(); // must not throw or double-return to the pool
    }

    [Fact]
    public void Allocate_WithHandle_ResolvesBackToSameBytes()
    {
        using var arena = new Arena(slabSize: 1024);

        Span<byte> a = arena.Allocate(4, out long ha);
        a[0] = 11; a[1] = 22; a[2] = 33; a[3] = 44;
        Span<byte> b = arena.Allocate(8, out long hb);
        b[0] = 99;

        Span<byte> ra = arena.Resolve(ha, 4);
        Span<byte> rb = arena.Resolve(hb, 8);
        Assert.Equal(11, ra[0]);
        Assert.Equal(44, ra[3]);
        Assert.Equal(99, rb[0]);
        Assert.NotEqual(ha, hb);
    }

    [Fact]
    public void Resolve_StaysValid_AcrossSlabBoundary()
    {
        using var arena = new Arena(slabSize: 16);

        Span<byte> first = arena.Allocate(12, out long h1);
        first[0] = 7;
        Span<byte> second = arena.Allocate(12, out long h2); // forces a new slab
        second[0] = 8;

        // The first handle still resolves correctly after the second allocation moved to a new slab.
        Assert.Equal(7, arena.Resolve(h1, 12)[0]);
        Assert.Equal(8, arena.Resolve(h2, 12)[0]);
        Assert.NotEqual(h1, h2);
    }

    [Fact]
    public void Allocate_WithHandle_AfterDispose_Throws()
    {
        var arena = new Arena(slabSize: 64);
        arena.Dispose();
        Assert.Throws<ObjectDisposedException>(() => arena.Allocate(8, out _));
    }
}
