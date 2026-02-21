using System;
using System.Collections.Generic;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Server.Server.Engines.SIMD;

/// <summary>
/// Hardware-accelerated full table scanner using .NET Intrinsics (AVX2 / Vector256).
/// This provides massive speedups for sequentially scanning unindexed arrays of numeric data.
/// </summary>
public static class SimdScanner
{
    private static readonly bool IsSupported = Avx2.IsSupported;

    /// <summary>
    /// Searches an array of integers for a target value and returns a list of matching indices.
    /// This is 4x-8x faster than a standard for-loop scan by using AVX2 instructions.
    /// </summary>
    public static unsafe List<int> FilterNumericEquals(int[] data, int targetValue)
    {
        var results = new List<int>();
        int length = data.Length;
        int i = 0;

        if (IsSupported)
        {
            // Vector256<int> holds 8 x 32-bit integers
            int vectorSize = Vector256<int>.Count; // 8
            Vector256<int> targetVector = Vector256.Create(targetValue);

            fixed (int* ptr = data)
            {
                for (; i <= length - vectorSize; i += vectorSize)
                {
                    // Load 8 contiguous integers from memory
                    Vector256<int> dataVector = Avx.LoadVector256(ptr + i);

                    // Compare: sets each 32-bit slot to all 1s (0xFFFFFFFF) if equal, else 0
                    Vector256<int> cmp = Avx2.CompareEqual(dataVector, targetVector);

                    // MoveMostSignificantBit extracts the MSB of each 32-bit slot into an 8-bit mask
                    int mask = Avx2.MoveMask(cmp.AsByte());

                    if (mask != 0)
                    {
                        // Some value(s) matched. mask gives exactly which 32-bit slots matched.
                        // We check the 4 bytes belonging to each 32-bit integer.
                        for (int j = 0; j < vectorSize; j++)
                        {
                            // A match in slot 'j' means the 4 bytes at j*4 .. j*4+3 are 1s
                            if ((mask & (1 << (j * 4))) != 0)
                            {
                                results.Add(i + j);
                            }
                        }
                    }
                }
            }
        }

        // Handle the remainder sequentially
        for (; i < length; i++)
        {
            if (data[i] == targetValue)
            {
                results.Add(i);
            }
        }

        return results;
    }

    /// <summary>
    /// Computes the sum of a numeric array using SIMD instructions.
    /// </summary>
    public static unsafe long SumNumeric(int[] data)
    {
        long sum = 0;
        int length = data.Length;
        int i = 0;

        if (IsSupported)
        {
            int vectorSize = Vector256<int>.Count; // 8
            Vector256<long> sumVector = Vector256<long>.Zero;

            fixed (int* ptr = data)
            {
                for (; i <= length - vectorSize; i += vectorSize)
                {
                    // Load 8 x 32-bit ints
                    Vector256<int> v = Avx.LoadVector256(ptr + i);
                    
                    // Convert lower 4 ints to 64-bit longs and upper 4 ints to 64-bit longs
                    Vector256<long> lower = Avx2.ConvertToVector256Int64(v.GetLower());
                    Vector256<long> upper = Avx2.ConvertToVector256Int64(Avx2.ExtractVector128(v, 1));
                    
                    sumVector = Avx2.Add(sumVector, lower);
                    sumVector = Avx2.Add(sumVector, upper);
                }
            }

            // Horizontally add the 4 x 64-bit sums in the vector
            // sumVector[0] + sumVector[1] + sumVector[2] + sumVector[3]
            var sumArray = new long[4];
            sumVector.CopyTo(sumArray);
            sum = sumArray[0] + sumArray[1] + sumArray[2] + sumArray[3];
        }

        // Handle the remainder sequentially
        for (; i < length; i++)
        {
            sum += data[i];
        }

        return sum;
    }
}
