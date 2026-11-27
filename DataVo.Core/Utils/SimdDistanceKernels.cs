#if NET8_0_OR_GREATER
using System.Numerics.Tensors;
using System.Runtime.Intrinsics.Arm;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;
#else
using System.Numerics;
#endif

namespace DataVo.Core.Utils;

/// <summary>
/// Dot product, cosine and Euclidean distance over float vectors, with AVX-512, AVX and AdvSimd (NEON) paths.
/// </summary>
/// <remarks>
/// The span entry points are compiled fully optimized on first use
/// (<see cref="Compat.MethodImplCompat.AggressiveOptimization"/>) and the per-ISA workers are inlined into them,
/// so a distance computation never runs as tier-0 code. A vector search calls them once per stored
/// vector, so under tiered compilation the first searches of a process ran their SIMD loops as unoptimized
/// tier-0 code until the runtime promoted them, which waits for a quiet period in JIT activity: 50,000 x 384
/// exact searches took 11-14 ms instead of about 2 ms until then, and sometimes for a whole benchmark run.
/// </remarks>
internal static class SimdDistanceKernels
{
    public static float CosineDistance(float[] a, float[] b)
    {
        return CosineDistance(a.AsSpan(), b.AsSpan());
    }

    public static float Dot(float[] a, float[] b)
    {
        return Dot(a.AsSpan(), b.AsSpan());
    }

    [System.Runtime.CompilerServices.MethodImpl(DataVo.Core.Compat.MethodImplCompat.AggressiveOptimization)]
    public static float Dot(ReadOnlySpan<float> a, ReadOnlySpan<float> b)
    {
        if (a.Length != b.Length)
        {
            throw new ArgumentException($"Vector dimensions do not match ({a.Length} vs {b.Length}).");
        }

#if NET8_0_OR_GREATER
        if (TryDotAvx(a, b, out float avxResult))
        {
            return avxResult;
        }

        if (TryDotAdvSimd(a, b, out float advSimdResult))
        {
            return advSimdResult;
        }

        return TensorPrimitives.Dot(a, b);
#else
        return PortableDot(a, b);
#endif
    }

    [System.Runtime.CompilerServices.MethodImpl(DataVo.Core.Compat.MethodImplCompat.AggressiveOptimization)]
    public static float CosineDistance(ReadOnlySpan<float> a, ReadOnlySpan<float> b)
    {
        if (a.Length != b.Length)
        {
            throw new ArgumentException($"Vector dimensions do not match ({a.Length} vs {b.Length}).");
        }

#if NET8_0_OR_GREATER
        if (TryCosineDistanceAvx(a, b, out float avxResult))
        {
            return avxResult;
        }

        if (TryCosineDistanceAdvSimd(a, b, out float advSimdResult))
        {
            return advSimdResult;
        }

        return TensorCosineDistance(a, b);
#else
        return PortableCosineDistance(a, b);
#endif
    }

    public static float EuclideanDistance(float[] a, float[] b)
    {
        return EuclideanDistance(a.AsSpan(), b.AsSpan());
    }

    /// <summary>
    /// Computes double-precision Euclidean (L2) distance between two float vectors of equal length.
    /// </summary>
    /// <param name="left">The left vector.</param>
    /// <param name="right">The right vector.</param>
    /// <returns>The Euclidean distance as a <see cref="double"/>.</returns>
    public static double EuclideanDistanceDouble(ReadOnlySpan<float> left, ReadOnlySpan<float> right)
    {
        double sum = 0d;
        for (int i = 0; i < left.Length; i++)
        {
            double diff = left[i] - right[i];
            sum += diff * diff;
        }

        return Math.Sqrt(sum);
    }

    /// <summary>
    /// Computes double-precision cosine distance (<c>1 - cosineSimilarity</c>) between two float vectors of equal length,
    /// returning <c>1.0</c> when either vector has zero magnitude.
    /// </summary>
    /// <param name="left">The left vector.</param>
    /// <param name="right">The right vector.</param>
    /// <returns>The cosine distance as a <see cref="double"/>.</returns>
    public static double CosineDistanceDouble(ReadOnlySpan<float> left, ReadOnlySpan<float> right)
    {
        double dot = 0d;
        double magLeft = 0d;
        double magRight = 0d;

        for (int i = 0; i < left.Length; i++)
        {
            dot += left[i] * right[i];
            magLeft += left[i] * left[i];
            magRight += right[i] * right[i];
        }

        if (magLeft <= 0d || magRight <= 0d)
        {
            return 1d;
        }

        double similarity = dot / (Math.Sqrt(magLeft) * Math.Sqrt(magRight));
        return 1d - similarity;
    }

    [System.Runtime.CompilerServices.MethodImpl(DataVo.Core.Compat.MethodImplCompat.AggressiveOptimization)]
    public static float EuclideanDistance(ReadOnlySpan<float> a, ReadOnlySpan<float> b)
    {
        if (a.Length != b.Length)
        {
            throw new ArgumentException($"Vector dimensions do not match ({a.Length} vs {b.Length}).");
        }

#if NET8_0_OR_GREATER
        if (TryEuclideanDistanceAvx(a, b, out float avxResult))
        {
            return avxResult;
        }

        if (TryEuclideanDistanceAdvSimd(a, b, out float advSimdResult))
        {
            return advSimdResult;
        }

        // Cross-platform hardware acceleration: the runtime lowers this to ARM NEON or x86 AVX.
        return TensorPrimitives.Distance(a, b);
#else
        return PortableEuclideanDistance(a, b);
#endif
    }

#if NET8_0_OR_GREATER
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static unsafe bool TryDotAvx(ReadOnlySpan<float> a, ReadOnlySpan<float> b, out float result)
    {
        result = 0f;
        if (!Avx.IsSupported || a.Length < Vector256<float>.Count)
        {
            return false;
        }

        Vector256<float> sum = Vector256<float>.Zero;
        int i = 0;
        int width = Vector256<float>.Count;

        fixed (float* pa = a)
        fixed (float* pb = b)
        {
            int unrolledStep = width * 2;
            for (; i <= a.Length - unrolledStep; i += unrolledStep)
            {
                Vector256<float> va0 = Avx.LoadVector256(pa + i);
                Vector256<float> vb0 = Avx.LoadVector256(pb + i);
                Vector256<float> va1 = Avx.LoadVector256(pa + i + width);
                Vector256<float> vb1 = Avx.LoadVector256(pb + i + width);

                if (Fma.IsSupported)
                {
                    sum = Fma.MultiplyAdd(va0, vb0, sum);
                    sum = Fma.MultiplyAdd(va1, vb1, sum);
                }
                else
                {
                    sum = Avx.Add(sum, Avx.Multiply(va0, vb0));
                    sum = Avx.Add(sum, Avx.Multiply(va1, vb1));
                }
            }

            for (; i <= a.Length - width; i += width)
            {
                Vector256<float> va = Avx.LoadVector256(pa + i);
                Vector256<float> vb = Avx.LoadVector256(pb + i);
                sum = Fma.IsSupported
                    ? Fma.MultiplyAdd(va, vb, sum)
                    : Avx.Add(sum, Avx.Multiply(va, vb));
            }
        }

        float total = HorizontalSum(sum);
        for (; i < a.Length; i++)
        {
            total += a[i] * b[i];
        }

        result = total;
        return true;
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static unsafe bool TryDotAdvSimd(ReadOnlySpan<float> a, ReadOnlySpan<float> b, out float result)
    {
        result = 0f;
        if (!AdvSimd.IsSupported || a.Length < Vector128<float>.Count)
        {
            return false;
        }

        Vector128<float> sum = Vector128<float>.Zero;
        int i = 0;
        int width = Vector128<float>.Count;

        fixed (float* pa = a)
        fixed (float* pb = b)
        {
            int unrolledStep = width * 2;
            for (; i <= a.Length - unrolledStep; i += unrolledStep)
            {
                Vector128<float> va0 = AdvSimd.LoadVector128(pa + i);
                Vector128<float> vb0 = AdvSimd.LoadVector128(pb + i);
                Vector128<float> va1 = AdvSimd.LoadVector128(pa + i + width);
                Vector128<float> vb1 = AdvSimd.LoadVector128(pb + i + width);

                sum = AdvSimd.Add(sum, AdvSimd.Multiply(va0, vb0));
                sum = AdvSimd.Add(sum, AdvSimd.Multiply(va1, vb1));
            }

            for (; i <= a.Length - width; i += width)
            {
                Vector128<float> va = AdvSimd.LoadVector128(pa + i);
                Vector128<float> vb = AdvSimd.LoadVector128(pb + i);
                sum = AdvSimd.Add(sum, AdvSimd.Multiply(va, vb));
            }
        }

        float total = HorizontalSum(sum);
        for (; i < a.Length; i++)
        {
            total += a[i] * b[i];
        }

        result = total;
        return true;
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static unsafe bool TryCosineDistanceAvx(ReadOnlySpan<float> a, ReadOnlySpan<float> b, out float distance)
    {
        distance = 0f;
        if (!Avx.IsSupported || a.Length < Vector256<float>.Count)
        {
            return false;
        }

        if (Avx512F.IsSupported && a.Length >= Vector512<float>.Count)
        {
            return TryCosineDistanceAvx512(a, b, out distance);
        }

        Vector256<float> dot = Vector256<float>.Zero;
        Vector256<float> magnitudeA = Vector256<float>.Zero;
        Vector256<float> magnitudeB = Vector256<float>.Zero;
        int i = 0;
        int width = Vector256<float>.Count;

        fixed (float* pa = a)
        fixed (float* pb = b)
        {
            int unrolledStep = width * 2;
            for (; i <= a.Length - unrolledStep; i += unrolledStep)
            {
                Vector256<float> va0 = Avx.LoadVector256(pa + i);
                Vector256<float> vb0 = Avx.LoadVector256(pb + i);
                Vector256<float> va1 = Avx.LoadVector256(pa + i + width);
                Vector256<float> vb1 = Avx.LoadVector256(pb + i + width);

                if (Fma.IsSupported)
                {
                    dot = Fma.MultiplyAdd(va0, vb0, dot);
                    dot = Fma.MultiplyAdd(va1, vb1, dot);

                    magnitudeA = Fma.MultiplyAdd(va0, va0, magnitudeA);
                    magnitudeA = Fma.MultiplyAdd(va1, va1, magnitudeA);

                    magnitudeB = Fma.MultiplyAdd(vb0, vb0, magnitudeB);
                    magnitudeB = Fma.MultiplyAdd(vb1, vb1, magnitudeB);
                }
                else
                {
                    dot = Avx.Add(dot, Avx.Multiply(va0, vb0));
                    dot = Avx.Add(dot, Avx.Multiply(va1, vb1));

                    magnitudeA = Avx.Add(magnitudeA, Avx.Multiply(va0, va0));
                    magnitudeA = Avx.Add(magnitudeA, Avx.Multiply(va1, va1));

                    magnitudeB = Avx.Add(magnitudeB, Avx.Multiply(vb0, vb0));
                    magnitudeB = Avx.Add(magnitudeB, Avx.Multiply(vb1, vb1));
                }
            }

            for (; i <= a.Length - width; i += width)
            {
                Vector256<float> va = Avx.LoadVector256(pa + i);
                Vector256<float> vb = Avx.LoadVector256(pb + i);

                if (Fma.IsSupported)
                {
                    dot = Fma.MultiplyAdd(va, vb, dot);
                    magnitudeA = Fma.MultiplyAdd(va, va, magnitudeA);
                    magnitudeB = Fma.MultiplyAdd(vb, vb, magnitudeB);
                }
                else
                {
                    dot = Avx.Add(dot, Avx.Multiply(va, vb));
                    magnitudeA = Avx.Add(magnitudeA, Avx.Multiply(va, va));
                    magnitudeB = Avx.Add(magnitudeB, Avx.Multiply(vb, vb));
                }
            }
        }

        float dotSum = HorizontalSum(dot);
        float magASum = HorizontalSum(magnitudeA);
        float magBSum = HorizontalSum(magnitudeB);

        for (; i < a.Length; i++)
        {
            dotSum += a[i] * b[i];
            magASum += a[i] * a[i];
            magBSum += b[i] * b[i];
        }

        if (magASum <= 0f || magBSum <= 0f)
        {
            distance = 1f;
            return true;
        }

        float similarity = dotSum / (MathF.Sqrt(magASum) * MathF.Sqrt(magBSum));
        distance = 1f - similarity;
        return true;
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static unsafe bool TryEuclideanDistanceAvx(ReadOnlySpan<float> a, ReadOnlySpan<float> b, out float distance)
    {
        distance = 0f;
        if (!Avx.IsSupported || a.Length < Vector256<float>.Count)
        {
            return false;
        }

        if (Avx512F.IsSupported && a.Length >= Vector512<float>.Count)
        {
            return TryEuclideanDistanceAvx512(a, b, out distance);
        }

        Vector256<float> sum = Vector256<float>.Zero;

        int i = 0;
        int width = Vector256<float>.Count;
        fixed (float* pa = a)
        fixed (float* pb = b)
        {
            int unrolledStep = width * 2;
            for (; i <= a.Length - unrolledStep; i += unrolledStep)
            {
                Vector256<float> va0 = Avx.LoadVector256(pa + i);
                Vector256<float> vb0 = Avx.LoadVector256(pb + i);
                Vector256<float> va1 = Avx.LoadVector256(pa + i + width);
                Vector256<float> vb1 = Avx.LoadVector256(pb + i + width);

                Vector256<float> diff0 = Avx.Subtract(va0, vb0);
                Vector256<float> diff1 = Avx.Subtract(va1, vb1);

                if (Fma.IsSupported)
                {
                    sum = Fma.MultiplyAdd(diff0, diff0, sum);
                    sum = Fma.MultiplyAdd(diff1, diff1, sum);
                }
                else
                {
                    sum = Avx.Add(sum, Avx.Multiply(diff0, diff0));
                    sum = Avx.Add(sum, Avx.Multiply(diff1, diff1));
                }
            }

            for (; i <= a.Length - width; i += width)
            {
                Vector256<float> va = Avx.LoadVector256(pa + i);
                Vector256<float> vb = Avx.LoadVector256(pb + i);
                Vector256<float> diff = Avx.Subtract(va, vb);

                if (Fma.IsSupported)
                {
                    sum = Fma.MultiplyAdd(diff, diff, sum);
                }
                else
                {
                    sum = Avx.Add(sum, Avx.Multiply(diff, diff));
                }
            }
        }

        float sumSquares = HorizontalSum(sum);
        for (; i < a.Length; i++)
        {
            float diff = a[i] - b[i];
            sumSquares += diff * diff;
        }

        distance = MathF.Sqrt(sumSquares);
        return true;
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static unsafe bool TryCosineDistanceAdvSimd(ReadOnlySpan<float> a, ReadOnlySpan<float> b, out float distance)
    {
        distance = 0f;
        if (!AdvSimd.IsSupported || a.Length < Vector128<float>.Count)
        {
            return false;
        }

        Vector128<float> dot = Vector128<float>.Zero;
        Vector128<float> magnitudeA = Vector128<float>.Zero;
        Vector128<float> magnitudeB = Vector128<float>.Zero;
        int i = 0;
        int width = Vector128<float>.Count;

        fixed (float* pa = a)
        fixed (float* pb = b)
        {
            int unrolledStep = width * 2;
            for (; i <= a.Length - unrolledStep; i += unrolledStep)
            {
                Vector128<float> va0 = AdvSimd.LoadVector128(pa + i);
                Vector128<float> vb0 = AdvSimd.LoadVector128(pb + i);
                Vector128<float> va1 = AdvSimd.LoadVector128(pa + i + width);
                Vector128<float> vb1 = AdvSimd.LoadVector128(pb + i + width);

                dot = AdvSimd.Add(dot, AdvSimd.Multiply(va0, vb0));
                dot = AdvSimd.Add(dot, AdvSimd.Multiply(va1, vb1));
                magnitudeA = AdvSimd.Add(magnitudeA, AdvSimd.Multiply(va0, va0));
                magnitudeA = AdvSimd.Add(magnitudeA, AdvSimd.Multiply(va1, va1));
                magnitudeB = AdvSimd.Add(magnitudeB, AdvSimd.Multiply(vb0, vb0));
                magnitudeB = AdvSimd.Add(magnitudeB, AdvSimd.Multiply(vb1, vb1));
            }

            for (; i <= a.Length - width; i += width)
            {
                Vector128<float> va = AdvSimd.LoadVector128(pa + i);
                Vector128<float> vb = AdvSimd.LoadVector128(pb + i);

                dot = AdvSimd.Add(dot, AdvSimd.Multiply(va, vb));
                magnitudeA = AdvSimd.Add(magnitudeA, AdvSimd.Multiply(va, va));
                magnitudeB = AdvSimd.Add(magnitudeB, AdvSimd.Multiply(vb, vb));
            }
        }

        float dotSum = HorizontalSum(dot);
        float magASum = HorizontalSum(magnitudeA);
        float magBSum = HorizontalSum(magnitudeB);

        for (; i < a.Length; i++)
        {
            dotSum += a[i] * b[i];
            magASum += a[i] * a[i];
            magBSum += b[i] * b[i];
        }

        if (magASum <= 0f || magBSum <= 0f)
        {
            distance = 1f;
            return true;
        }

        float similarity = dotSum / (MathF.Sqrt(magASum) * MathF.Sqrt(magBSum));
        distance = 1f - similarity;
        return true;
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static unsafe bool TryEuclideanDistanceAdvSimd(ReadOnlySpan<float> a, ReadOnlySpan<float> b, out float distance)
    {
        distance = 0f;
        if (!AdvSimd.IsSupported || a.Length < Vector128<float>.Count)
        {
            return false;
        }

        Vector128<float> sum = Vector128<float>.Zero;
        int i = 0;
        int width = Vector128<float>.Count;

        fixed (float* pa = a)
        fixed (float* pb = b)
        {
            int unrolledStep = width * 2;
            for (; i <= a.Length - unrolledStep; i += unrolledStep)
            {
                Vector128<float> va0 = AdvSimd.LoadVector128(pa + i);
                Vector128<float> vb0 = AdvSimd.LoadVector128(pb + i);
                Vector128<float> va1 = AdvSimd.LoadVector128(pa + i + width);
                Vector128<float> vb1 = AdvSimd.LoadVector128(pb + i + width);

                Vector128<float> diff0 = AdvSimd.Subtract(va0, vb0);
                Vector128<float> diff1 = AdvSimd.Subtract(va1, vb1);
                sum = AdvSimd.Add(sum, AdvSimd.Multiply(diff0, diff0));
                sum = AdvSimd.Add(sum, AdvSimd.Multiply(diff1, diff1));
            }

            for (; i <= a.Length - width; i += width)
            {
                Vector128<float> va = AdvSimd.LoadVector128(pa + i);
                Vector128<float> vb = AdvSimd.LoadVector128(pb + i);
                Vector128<float> diff = AdvSimd.Subtract(va, vb);
                sum = AdvSimd.Add(sum, AdvSimd.Multiply(diff, diff));
            }
        }

        float sumSquares = HorizontalSum(sum);
        for (; i < a.Length; i++)
        {
            float diff = a[i] - b[i];
            sumSquares += diff * diff;
        }

        distance = MathF.Sqrt(sumSquares);
        return true;
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static unsafe bool TryCosineDistanceAvx512(ReadOnlySpan<float> a, ReadOnlySpan<float> b, out float distance)
    {
        distance = 0f;
        Vector512<float> dot = Vector512<float>.Zero;
        Vector512<float> magnitudeA = Vector512<float>.Zero;
        Vector512<float> magnitudeB = Vector512<float>.Zero;
        int i = 0;
        int width = Vector512<float>.Count;

        fixed (float* pa = a)
        fixed (float* pb = b)
        {
            for (; i <= a.Length - width; i += width)
            {
                Vector512<float> va = Avx512F.LoadVector512(pa + i);
                Vector512<float> vb = Avx512F.LoadVector512(pb + i);

                dot += va * vb;
                magnitudeA += va * va;
                magnitudeB += vb * vb;
            }
        }

        float dotSum = HorizontalSum(dot);
        float magASum = HorizontalSum(magnitudeA);
        float magBSum = HorizontalSum(magnitudeB);

        for (; i < a.Length; i++)
        {
            dotSum += a[i] * b[i];
            magASum += a[i] * a[i];
            magBSum += b[i] * b[i];
        }

        if (magASum <= 0f || magBSum <= 0f)
        {
            distance = 1f;
            return true;
        }

        float similarity = dotSum / (MathF.Sqrt(magASum) * MathF.Sqrt(magBSum));
        distance = 1f - similarity;
        return true;
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static unsafe bool TryEuclideanDistanceAvx512(ReadOnlySpan<float> a, ReadOnlySpan<float> b, out float distance)
    {
        distance = 0f;
        Vector512<float> sum = Vector512<float>.Zero;
        int i = 0;
        int width = Vector512<float>.Count;

        fixed (float* pa = a)
        fixed (float* pb = b)
        {
            for (; i <= a.Length - width; i += width)
            {
                Vector512<float> va = Avx512F.LoadVector512(pa + i);
                Vector512<float> vb = Avx512F.LoadVector512(pb + i);
                Vector512<float> diff = va - vb;
                sum += diff * diff;
            }
        }

        float sumSquares = HorizontalSum(sum);
        for (; i < a.Length; i++)
        {
            float diff = a[i] - b[i];
            sumSquares += diff * diff;
        }

        distance = MathF.Sqrt(sumSquares);
        return true;
    }

    /// <summary>
    /// Cosine distance via <see cref="TensorPrimitives"/>, used wherever the x86 AVX path is unavailable
    /// (notably ARM, where the runtime lowers these primitives to NEON). Computes the same
    /// <c>1 - dot / (‖a‖·‖b‖)</c> as the intrinsic and scalar paths, and preserves the zero-vector contract
    /// (distance 1, never NaN) that a raw <c>CosineSimilarity</c> divide-by-zero would violate.
    /// </summary>
    private static float TensorCosineDistance(ReadOnlySpan<float> a, ReadOnlySpan<float> b)
    {
        float normA = TensorPrimitives.Norm(a);
        float normB = TensorPrimitives.Norm(b);

        if (normA <= 0f || normB <= 0f)
        {
            return 1f;
        }

        float similarity = TensorPrimitives.Dot(a, b) / (normA * normB);
        return 1f - similarity;
    }

    private static unsafe float HorizontalSum(Vector256<float> vector)
    {
        Span<float> buffer = stackalloc float[Vector256<float>.Count];
        fixed (float* pBuffer = buffer)
        {
            Avx.Store(pBuffer, vector);
        }

        float sum = 0f;
        for (int i = 0; i < buffer.Length; i++)
        {
            sum += buffer[i];
        }

        return sum;
    }

    private static float HorizontalSum(Vector512<float> vector)
    {
        Span<float> buffer = stackalloc float[Vector512<float>.Count];
        vector.CopyTo(buffer);

        float sum = 0f;
        for (int i = 0; i < buffer.Length; i++)
        {
            sum += buffer[i];
        }

        return sum;
    }

    private static float HorizontalSum(Vector128<float> vector)
    {
        Span<float> buffer = stackalloc float[Vector128<float>.Count];
        vector.CopyTo(buffer);

        float sum = 0f;
        for (int i = 0; i < buffer.Length; i++)
        {
            sum += buffer[i];
        }

        return sum;
    }
#else
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static float PortableDot(ReadOnlySpan<float> a, ReadOnlySpan<float> b)
    {
        if (Vector.IsHardwareAccelerated && a.Length >= Vector<float>.Count)
        {
            int i = 0;
            Vector<float> sum = Vector<float>.Zero;
            int width = Vector<float>.Count;
            Span<float> left = stackalloc float[width];
            Span<float> right = stackalloc float[width];

            for (; i <= a.Length - width; i += width)
            {
                a.Slice(i, width).CopyTo(left);
                b.Slice(i, width).CopyTo(right);
                sum += new Vector<float>(left) * new Vector<float>(right);
            }

            float total = 0f;
            for (int lane = 0; lane < Vector<float>.Count; lane++)
            {
                total += sum[lane];
            }

            for (; i < a.Length; i++)
            {
                total += a[i] * b[i];
            }

            return total;
        }

        float scalar = 0f;
        for (int i = 0; i < a.Length; i++)
        {
            scalar += a[i] * b[i];
        }

        return scalar;
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static float PortableCosineDistance(ReadOnlySpan<float> a, ReadOnlySpan<float> b)
    {
        if (Vector.IsHardwareAccelerated && a.Length >= Vector<float>.Count)
        {
            int i = 0;
            int width = Vector<float>.Count;
            Vector<float> dotVector = Vector<float>.Zero;
            Vector<float> magnitudeAVector = Vector<float>.Zero;
            Vector<float> magnitudeBVector = Vector<float>.Zero;
            Span<float> left = stackalloc float[width];
            Span<float> right = stackalloc float[width];

            for (; i <= a.Length - width; i += width)
            {
                a.Slice(i, width).CopyTo(left);
                b.Slice(i, width).CopyTo(right);
                Vector<float> va = new(left);
                Vector<float> vb = new(right);
                dotVector += va * vb;
                magnitudeAVector += va * va;
                magnitudeBVector += vb * vb;
            }

            float dotSum = 0f;
            float simdMagnitudeA = 0f;
            float simdMagnitudeB = 0f;
            for (int lane = 0; lane < Vector<float>.Count; lane++)
            {
                dotSum += dotVector[lane];
                simdMagnitudeA += magnitudeAVector[lane];
                simdMagnitudeB += magnitudeBVector[lane];
            }

            for (; i < a.Length; i++)
            {
                float av = a[i];
                float bv = b[i];
                dotSum += av * bv;
                simdMagnitudeA += av * av;
                simdMagnitudeB += bv * bv;
            }

            if (simdMagnitudeA <= 0f || simdMagnitudeB <= 0f)
            {
                return 1f;
            }

            float simdSimilarity = dotSum / (MathF.Sqrt(simdMagnitudeA) * MathF.Sqrt(simdMagnitudeB));
            return 1f - simdSimilarity;
        }

        float dot = 0f;
        float magnitudeA = 0f;
        float magnitudeB = 0f;

        for (int i = 0; i < a.Length; i++)
        {
            float av = a[i];
            float bv = b[i];
            dot += av * bv;
            magnitudeA += av * av;
            magnitudeB += bv * bv;
        }

        if (magnitudeA <= 0f || magnitudeB <= 0f)
        {
            return 1f;
        }

        float similarity = dot / (MathF.Sqrt(magnitudeA) * MathF.Sqrt(magnitudeB));
        return 1f - similarity;
    }

    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    private static float PortableEuclideanDistance(ReadOnlySpan<float> a, ReadOnlySpan<float> b)
    {
        if (Vector.IsHardwareAccelerated && a.Length >= Vector<float>.Count)
        {
            int i = 0;
            int width = Vector<float>.Count;
            Vector<float> sum = Vector<float>.Zero;
            Span<float> left = stackalloc float[width];
            Span<float> right = stackalloc float[width];

            for (; i <= a.Length - width; i += width)
            {
                a.Slice(i, width).CopyTo(left);
                b.Slice(i, width).CopyTo(right);
                Vector<float> diff = new Vector<float>(left) - new Vector<float>(right);
                sum += diff * diff;
            }

            float total = 0f;
            for (int lane = 0; lane < Vector<float>.Count; lane++)
            {
                total += sum[lane];
            }

            for (; i < a.Length; i++)
            {
                float diff = a[i] - b[i];
                total += diff * diff;
            }

            return MathF.Sqrt(total);
        }

        float sumSquares = 0f;
        for (int i = 0; i < a.Length; i++)
        {
            float diff = a[i] - b[i];
            sumSquares += diff * diff;
        }

        return MathF.Sqrt(sumSquares);
    }
#endif
}
