using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace DataVo.Core.Utils;

internal static class SimdDistanceKernels
{
    public static float CosineDistance(float[] a, float[] b)
    {
        return CosineDistance(a.AsSpan(), b.AsSpan());
    }

    public static float CosineDistance(ReadOnlySpan<float> a, ReadOnlySpan<float> b)
    {
        if (a.Length != b.Length)
        {
            throw new ArgumentException($"Vector dimensions do not match ({a.Length} vs {b.Length}).");
        }

        if (TryCosineDistanceAvx(a, b, out float avxResult))
        {
            return avxResult;
        }

        return ScalarCosineDistance(a, b);
    }

    public static float EuclideanDistance(float[] a, float[] b)
    {
        return EuclideanDistance(a.AsSpan(), b.AsSpan());
    }

    public static float EuclideanDistance(ReadOnlySpan<float> a, ReadOnlySpan<float> b)
    {
        if (a.Length != b.Length)
        {
            throw new ArgumentException($"Vector dimensions do not match ({a.Length} vs {b.Length}).");
        }

        if (TryEuclideanDistanceAvx(a, b, out float avxResult))
        {
            return avxResult;
        }

        return ScalarEuclideanDistance(a, b);
    }

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

    private static float ScalarCosineDistance(ReadOnlySpan<float> a, ReadOnlySpan<float> b)
    {
        float dot = 0f;
        float magA = 0f;
        float magB = 0f;

        for (int i = 0; i < a.Length; i++)
        {
            dot += a[i] * b[i];
            magA += a[i] * a[i];
            magB += b[i] * b[i];
        }

        if (magA <= 0f || magB <= 0f)
        {
            return 1f;
        }

        float similarity = dot / (MathF.Sqrt(magA) * MathF.Sqrt(magB));
        return 1f - similarity;
    }

    private static float ScalarEuclideanDistance(ReadOnlySpan<float> a, ReadOnlySpan<float> b)
    {
        float sum = 0f;
        for (int i = 0; i < a.Length; i++)
        {
            float diff = a[i] - b[i];
            sum += diff * diff;
        }

        return MathF.Sqrt(sum);
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
}