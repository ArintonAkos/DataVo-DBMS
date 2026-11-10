using System.Numerics;
using System.Runtime.CompilerServices;

namespace DataVo.Core;

/// <summary>
/// Allocation-free 3D distance and containment tests, shared by the spatial queries on
/// <see cref="DataVoTable{T}"/>.
/// </summary>
/// <remarks>
/// <para>
/// The <see cref="Vector3"/> overloads use the hardware-accelerated <see cref="System.Numerics"/> operations, which
/// the .NET JIT compiles to SIMD instructions and which run as scalar code on Unity's Mono and IL2CPP.
/// </para>
/// <para>
/// The span and coordinate overloads, which the table queries use to test stored rows, work in
/// <see cref="double"/> precision: a point exactly on a sphere's surface counts as inside even when its
/// coordinates are large, where single-precision rounding could push it out.
/// </para>
/// </remarks>
/// <example>
/// <code>
/// float d = Spatial3.Distance(a, b);
/// bool near = Spatial3.IsWithinRadius(enemy.Position, player.Position, 25f);
/// </code>
/// </example>
public static class Spatial3
{
    /// <summary>The number of components in a 3D coordinate.</summary>
    public const int Dimensions = 3;

    /// <summary>Gets the Euclidean distance between two points.</summary>
    /// <param name="a">The first point.</param>
    /// <param name="b">The second point.</param>
    /// <returns>The distance.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static float Distance(Vector3 a, Vector3 b) => Vector3.Distance(a, b);

    /// <summary>Gets the squared Euclidean distance between two points, which orders points like the distance without a square root.</summary>
    /// <param name="a">The first point.</param>
    /// <param name="b">The second point.</param>
    /// <returns>The squared distance.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static float DistanceSquared(Vector3 a, Vector3 b) => Vector3.DistanceSquared(a, b);

    /// <summary>Gets the squared Euclidean distance between two points in double precision.</summary>
    /// <param name="a">The first point.</param>
    /// <param name="b">The second point.</param>
    /// <returns>The squared distance.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static double DistanceSquaredPrecise(Vector3 a, Vector3 b) =>
        DistanceSquaredPrecise(a.X, a.Y, a.Z, b);

    /// <summary>Gets the squared Euclidean distance between a stored coordinate and a point, in double precision.</summary>
    /// <param name="coordinates">The stored X, Y and Z.</param>
    /// <param name="point">The point.</param>
    /// <returns>The squared distance.</returns>
    /// <exception cref="ArgumentException"><paramref name="coordinates"/> does not hold exactly three values.</exception>
    public static double DistanceSquaredPrecise(ReadOnlySpan<float> coordinates, Vector3 point)
    {
        if (coordinates.Length != Dimensions)
        {
            throw NotThreeDimensional(coordinates.Length);
        }

        return DistanceSquaredPrecise(coordinates[0], coordinates[1], coordinates[2], point);
    }

    /// <summary>Gets whether a point is within a distance of a center; a point exactly on the sphere counts.</summary>
    /// <param name="point">The point to test.</param>
    /// <param name="center">The sphere's center.</param>
    /// <param name="radius">The sphere's radius.</param>
    /// <returns><see langword="true"/> when the point is inside or on the sphere.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool IsWithinRadius(Vector3 point, Vector3 center, float radius) =>
        DistanceSquaredPrecise(point, center) <= (double)radius * radius;

    /// <summary>Gets whether a stored coordinate is within a distance of a center; a point exactly on the sphere counts.</summary>
    /// <param name="coordinates">The stored X, Y and Z.</param>
    /// <param name="center">The sphere's center.</param>
    /// <param name="radius">The sphere's radius.</param>
    /// <returns><see langword="true"/> when the point is inside or on the sphere.</returns>
    /// <exception cref="ArgumentException"><paramref name="coordinates"/> does not hold exactly three values.</exception>
    public static bool IsWithinRadius(ReadOnlySpan<float> coordinates, Vector3 center, float radius) =>
        DistanceSquaredPrecise(coordinates, center) <= (double)radius * radius;

    /// <summary>Gets whether a stored coordinate is inside a box or on its surface.</summary>
    /// <param name="coordinates">The stored X, Y and Z.</param>
    /// <param name="box">The box.</param>
    /// <returns><see langword="true"/> when the point is inside.</returns>
    /// <exception cref="ArgumentException"><paramref name="coordinates"/> does not hold exactly three values.</exception>
    public static bool IsInside(ReadOnlySpan<float> coordinates, in BoundingBox3 box)
    {
        if (coordinates.Length != Dimensions)
        {
            throw NotThreeDimensional(coordinates.Length);
        }

        return box.Contains(coordinates[0], coordinates[1], coordinates[2]);
    }

    /// <summary>Reads a stored coordinate as a <see cref="Vector3"/>.</summary>
    /// <param name="coordinates">The stored X, Y and Z.</param>
    /// <returns>The point.</returns>
    /// <exception cref="ArgumentException"><paramref name="coordinates"/> does not hold exactly three values.</exception>
    public static Vector3 ToVector3(ReadOnlySpan<float> coordinates)
    {
        if (coordinates.Length != Dimensions)
        {
            throw NotThreeDimensional(coordinates.Length);
        }

        return new Vector3(coordinates[0], coordinates[1], coordinates[2]);
    }

    /// <summary>Writes a point's X, Y and Z into a span of at least three floats.</summary>
    /// <param name="point">The point.</param>
    /// <param name="destination">Where to write the coordinates.</param>
    /// <exception cref="ArgumentException"><paramref name="destination"/> is shorter than three values.</exception>
    public static void CopyTo(Vector3 point, Span<float> destination)
    {
        if (destination.Length < Dimensions)
        {
            throw new ArgumentException("The destination needs room for three coordinates.", nameof(destination));
        }

        destination[0] = point.X;
        destination[1] = point.Y;
        destination[2] = point.Z;
    }

    internal static bool HasNaN(Vector3 point) => float.IsNaN(point.X) || float.IsNaN(point.Y) || float.IsNaN(point.Z);

    internal static bool IsFinite(Vector3 point) =>
        !HasNaN(point) && !float.IsInfinity(point.X) && !float.IsInfinity(point.Y) && !float.IsInfinity(point.Z);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static double DistanceSquaredPrecise(float x, float y, float z, Vector3 point)
    {
        double dx = (double)x - point.X;
        double dy = (double)y - point.Y;
        double dz = (double)z - point.Z;
        return (dx * dx) + (dy * dy) + (dz * dz);
    }

    private static ArgumentException NotThreeDimensional(int length) =>
        new($"A 3D coordinate has three values, but this one has {length}.");
}
