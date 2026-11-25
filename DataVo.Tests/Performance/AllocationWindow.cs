using System.Runtime;

namespace DataVo.Tests.Performance;

/// <summary>
/// Measures the bytes allocated on the calling thread between <see cref="Start"/> and <see cref="Stop"/>, and
/// records what else happened in that window (methods JIT-compiled on this thread, collections) so a failing
/// zero-allocation assertion says where the bytes came from.
/// </summary>
/// <example>
/// <code>
/// var window = new AllocationWindow();
/// window.Start();
/// long total = MeasuredPass(reader);
/// long allocated = window.Stop();
/// output.WriteLine(window.ToString());
/// Assert.Equal(0L, allocated);
/// </code>
/// </example>
internal sealed class AllocationWindow
{
    private long _startBytes;
    private long _startCompiledMethods;
    private int _startGen0;
    private int _startGen2;

    /// <summary>Creates a window and runs <see cref="Start"/> and <see cref="Stop"/> once so neither is compiled inside a measurement.</summary>
    public AllocationWindow()
    {
        Start();
        Stop();
    }

    /// <summary>Bytes allocated on this thread in the last window.</summary>
    public long Bytes { get; private set; }

    /// <summary>Methods the JIT compiled on this thread in the last window.</summary>
    public long CompiledMethods { get; private set; }

    /// <summary>Gen0 collections (any thread) in the last window.</summary>
    public int Gen0Collections { get; private set; }

    /// <summary>Gen2 collections (any thread) in the last window.</summary>
    public int Gen2Collections { get; private set; }

    /// <summary>Opens the window.</summary>
    public void Start()
    {
        _startCompiledMethods = JitInfo.GetCompiledMethodCount(currentThread: true);
        _startGen0 = GC.CollectionCount(0);
        _startGen2 = GC.CollectionCount(2);
        _startBytes = GC.GetAllocatedBytesForCurrentThread();
    }

    /// <summary>Closes the window and returns the bytes allocated on this thread since <see cref="Start"/>.</summary>
    public long Stop()
    {
        long bytes = GC.GetAllocatedBytesForCurrentThread() - _startBytes;
        CompiledMethods = JitInfo.GetCompiledMethodCount(currentThread: true) - _startCompiledMethods;
        Gen0Collections = GC.CollectionCount(0) - _startGen0;
        Gen2Collections = GC.CollectionCount(2) - _startGen2;
        Bytes = bytes;
        return bytes;
    }

    /// <inheritdoc/>
    public override string ToString() =>
        $"{Bytes:N0} B allocated; {CompiledMethods} method(s) JIT-compiled on this thread; "
        + $"{Gen0Collections} gen0 and {Gen2Collections} gen2 collection(s) during the window";
}
