using System.Globalization;

namespace DataVo.Core.Utils;

internal static class VectorParser
{
    public static bool TryParseVector(string? input, out float[] vector)
    {
        vector = [];
        if (string.IsNullOrWhiteSpace(input))
        {
            return false;
        }

        string candidate = input.Trim();

        if ((candidate.StartsWith("'", StringComparison.Ordinal) && candidate.EndsWith("'", StringComparison.Ordinal))
            || (candidate.StartsWith("\"", StringComparison.Ordinal) && candidate.EndsWith("\"", StringComparison.Ordinal)))
        {
            candidate = candidate[1..^1].Trim();
        }

        if (candidate.StartsWith("[", StringComparison.Ordinal) && candidate.EndsWith("]", StringComparison.Ordinal))
        {
            candidate = candidate[1..^1];
        }

        if (string.IsNullOrWhiteSpace(candidate))
        {
            return false;
        }

        string[] parts = candidate.Split([','], StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0)
        {
            return false;
        }

        vector = new float[parts.Length];
        for (int i = 0; i < parts.Length; i++)
        {
            if (!float.TryParse(parts[i].Trim(), NumberStyles.Float, CultureInfo.InvariantCulture, out float value))
            {
                vector = [];
                return false;
            }

            vector[i] = value;
        }

        return true;
    }

    public static bool TryCoerceToVector(object? value, out float[] vector)
    {
        vector = [];

        if (value == null)
        {
            return false;
        }

        if (value is float[] floatArray)
        {
            vector = [.. floatArray];
            return true;
        }

        if (value is double[] doubleArray)
        {
            vector = doubleArray.Select(v => (float)v).ToArray();
            return true;
        }

        if (value is IEnumerable<float> floatEnumerable)
        {
            vector = floatEnumerable.ToArray();
            return vector.Length > 0;
        }

        if (value is IEnumerable<double> doubleEnumerable)
        {
            vector = doubleEnumerable.Select(v => (float)v).ToArray();
            return vector.Length > 0;
        }

        return TryParseVector(value.ToString(), out vector);
    }

    public static string SerializeVector(float[] vector)
    {
        return $"[{string.Join(",", vector.Select(v => v.ToString(CultureInfo.InvariantCulture)))}]";
    }

    public static float CosineDistance(float[] a, float[] b)
    {
        ValidateComparable(a, b);

        float dot = 0;
        float magA = 0;
        float magB = 0;

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

        float similarity = dot / (float)(Math.Sqrt(magA) * Math.Sqrt(magB));
        return 1f - similarity;
    }

    public static float EuclideanDistance(float[] a, float[] b)
    {
        ValidateComparable(a, b);

        float sum = 0;
        for (int i = 0; i < a.Length; i++)
        {
            float diff = a[i] - b[i];
            sum += diff * diff;
        }

        return (float)Math.Sqrt(sum);
    }

    private static void ValidateComparable(float[] a, float[] b)
    {
        if (a.Length == 0 || b.Length == 0)
        {
            throw new ArgumentException("Vectors cannot be empty.");
        }

        if (a.Length != b.Length)
        {
            throw new ArgumentException($"Vector dimensions do not match ({a.Length} vs {b.Length}).");
        }
    }
}
