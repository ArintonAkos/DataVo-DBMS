using DataVo.Core.Utils;

namespace DataVo.Tests.Indexing;

/// <summary>
/// Characterization tests that pin <see cref="SimdDistanceKernels"/> output against an independent
/// double-precision reference. These guard the cosine/euclidean contract so the distance kernel can be
/// re-implemented (x86 intrinsics, ARM NEON via TensorPrimitives, scalar) without changing results.
/// </summary>
public class SimdDistanceKernelsTests
{
    [Theory]
    [InlineData(3)]    // below SIMD width — exercises the fallback directly
    [InlineData(8)]
    [InlineData(16)]
    [InlineData(1536)] // the Scenario C embedding width
    public void CosineDistance_MatchesIndependentReference(int dimension)
    {
        float[] a = RandomVector(new Random(20260623 + dimension), dimension);
        float[] b = RandomVector(new Random(990001 + dimension), dimension);

        float actual = SimdDistanceKernels.CosineDistance(a, b);
        double expected = ReferenceCosineDistance(a, b);

        AssertClose(expected, actual);
    }

    [Theory]
    [InlineData(3)]
    [InlineData(8)]
    [InlineData(1536)]
    public void EuclideanDistance_MatchesIndependentReference(int dimension)
    {
        float[] a = RandomVector(new Random(424242 + dimension), dimension);
        float[] b = RandomVector(new Random(131313 + dimension), dimension);

        float actual = SimdDistanceKernels.EuclideanDistance(a, b);
        double expected = ReferenceEuclideanDistance(a, b);

        AssertClose(expected, actual);
    }

    [Fact]
    public void CosineDistance_IdenticalVectors_IsApproximatelyZero()
    {
        float[] a = RandomVector(new Random(7), 1536);
        AssertClose(0.0, SimdDistanceKernels.CosineDistance(a, a));
    }

    [Fact]
    public void CosineDistance_ZeroVector_ReturnsOne_NotNaN()
    {
        // A zero-magnitude vector must yield distance 1 (every access path guards this). The
        // TensorPrimitives path would otherwise divide by a zero norm and produce NaN, so this pins
        // the guard explicitly.
        float[] zero = new float[1536];
        float[] other = RandomVector(new Random(11), 1536);

        Assert.Equal(1f, SimdDistanceKernels.CosineDistance(zero, other));
        Assert.Equal(1f, SimdDistanceKernels.CosineDistance(other, zero));
    }

    [Fact]
    public void Distance_LengthMismatch_Throws()
    {
        Assert.Throws<ArgumentException>(() => SimdDistanceKernels.CosineDistance(new float[4], new float[5]));
        Assert.Throws<ArgumentException>(() => SimdDistanceKernels.EuclideanDistance(new float[4], new float[5]));
    }

    private static void AssertClose(double expected, float actual)
    {
        // Combined absolute + relative tolerance: float accumulation over 1536 terms diverges from the
        // double reference by more than a fixed epsilon, but a real bug (e.g. similarity vs distance)
        // is off by ~1.0, far outside this band.
        double tolerance = 1e-3 + Math.Abs(expected) * 1e-3;
        Assert.True(
            Math.Abs(expected - actual) <= tolerance,
            $"expected {expected:R} but got {actual:R} (tolerance {tolerance:R}).");
    }

    private static float[] RandomVector(Random rng, int dimension)
    {
        float[] vector = new float[dimension];
        for (int i = 0; i < dimension; i++)
        {
            vector[i] = (float)(rng.NextDouble() * 2.0 - 1.0);
        }

        return vector;
    }

    private static double ReferenceCosineDistance(float[] a, float[] b)
    {
        double dot = 0, magA = 0, magB = 0;
        for (int i = 0; i < a.Length; i++)
        {
            dot += (double)a[i] * b[i];
            magA += (double)a[i] * a[i];
            magB += (double)b[i] * b[i];
        }

        if (magA <= 0 || magB <= 0)
        {
            return 1.0;
        }

        return 1.0 - dot / (Math.Sqrt(magA) * Math.Sqrt(magB));
    }

    private static double ReferenceEuclideanDistance(float[] a, float[] b)
    {
        double sum = 0;
        for (int i = 0; i < a.Length; i++)
        {
            double diff = (double)a[i] - b[i];
            sum += diff * diff;
        }

        return Math.Sqrt(sum);
    }
}
