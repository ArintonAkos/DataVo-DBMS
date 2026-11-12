using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace DataVo.Core.Memory.Flat;

/// <summary>
/// A fixed block of <see cref="Slots"/> equal-width slots laid end to end. With a stride of one value it is a
/// column vector; with a stride of one record it is a run of packed rows. The block never moves or grows, so a
/// reader may keep a span over it while a writer fills later slots.
/// </summary>
/// <remarks>
/// On .NET the block lives on the pinned heap and starts on a 64-byte boundary, so no slot run straddles a cache
/// line it does not have to and vector loads are aligned. The memory is owned by the garbage collector: a
/// reader that still holds the chunk can never touch freed memory.
/// </remarks>
internal sealed class FlatChunk
{
    /// <summary>log2 of <see cref="Slots"/>.</summary>
    public const int Shift = 12;

    /// <summary>The number of slots in every chunk.</summary>
    public const int Slots = 1 << Shift;

    /// <summary>Slot index to position in its chunk: <c>index &amp; SlotMask</c>.</summary>
    public const int SlotMask = Slots - 1;

    /// <summary>The boundary a chunk's first byte is aligned to.</summary>
    public const int Alignment = 64;

    private readonly byte[] _memory;
    private readonly int _start;

    /// <summary>Allocates a zeroed chunk.</summary>
    /// <param name="stride">The width of one slot in bytes.</param>
    public FlatChunk(int stride)
    {
        if (stride <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(stride));
        }

        Stride = stride;
        int length = checked(Slots * stride);
#if NET10_0_OR_GREATER
        _memory = GC.AllocateArray<byte>(length + Alignment - 1, pinned: true);
        _start = AlignmentGap(_memory);
#else
        _memory = new byte[length];
        _start = 0;
#endif
    }

    /// <summary>The width of one slot in bytes.</summary>
    public int Stride { get; }

    /// <summary>All slots as bytes.</summary>
    public Span<byte> Bytes => new(_memory, _start, Slots * Stride);

    /// <summary>All slots as values of <typeparamref name="T"/>, whose size must be the stride.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Span<T> As<T>()
        where T : unmanaged
    {
        if (Unsafe.SizeOf<T>() != Stride)
        {
            ThrowStride();
        }

        return MemoryMarshal.Cast<byte, T>(Bytes);
    }

    /// <summary>The bytes of one slot.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public Span<byte> Slot(int index) => new(_memory, _start + index * Stride, Stride);

#if NET10_0_OR_GREATER
    /// <summary>Whether the first slot sits on an <see cref="Alignment"/>-byte boundary.</summary>
    public unsafe bool IsAligned => (nuint)Unsafe.AsPointer(ref _memory[_start]) % Alignment == 0;

    // The array is pinned, so its address is fixed for its lifetime.
    private static unsafe int AlignmentGap(byte[] memory)
    {
        nuint address = (nuint)Unsafe.AsPointer(ref MemoryMarshal.GetArrayDataReference(memory));
        return (int)((Alignment - (address % Alignment)) % Alignment);
    }
#endif

    private static void ThrowStride() => throw new InvalidOperationException("The value type does not match the chunk's stride.");
}
