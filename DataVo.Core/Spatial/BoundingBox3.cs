using System.Numerics;
using System.Runtime.CompilerServices;

namespace DataVo.Core;

/// <summary>
/// An axis-aligned box in 3D space, given by its minimum and maximum corners. Both corners are inside the box.
/// </summary>
/// <remarks>
/// A value type with no heap state: building one and testing points against it never allocates. Its math uses
/// <see cref="Vector3"/>, which the .NET JIT maps to SIMD registers; on runtimes without that (Unity's Mono and
/// IL2CPP) the same code runs as plain scalar arithmetic.
/// </remarks>
/// <example>
/// <code>
/// var room = new BoundingBox3(new Vector3(0, 0, 0), new Vector3(10, 3, 10));
/// bool inside = room.Contains(player.Position);
/// </code>
/// </example>
public readonly struct BoundingBox3 : IEquatable<BoundingBox3>
{
    /// <summary>Creates a box from two opposite corners, in any order.</summary>
    /// <param name="cornerA">One corner.</param>
    /// <param name="cornerB">The opposite corner.</param>
    /// <exception cref="ArgumentException">A coordinate is NaN.</exception>
    public BoundingBox3(Vector3 cornerA, Vector3 cornerB)
    {
        if (Spatial3.HasNaN(cornerA) || Spatial3.HasNaN(cornerB))
        {
            throw new ArgumentException("A bounding box corner cannot have a NaN coordinate.");
        }

        Min = Vector3.Min(cornerA, cornerB);
        Max = Vector3.Max(cornerA, cornerB);
    }

    /// <summary>Gets the corner with the smallest coordinates.</summary>
    public Vector3 Min { get; }

    /// <summary>Gets the corner with the largest coordinates.</summary>
    public Vector3 Max { get; }

    /// <summary>Gets the point halfway between the corners.</summary>
    public Vector3 Center => (Min + Max) * 0.5f;

    /// <summary>Gets the edge lengths along X, Y and Z.</summary>
    public Vector3 Size => Max - Min;

    /// <summary>Creates a box from its center and half its size along each axis.</summary>
    /// <param name="center">The center of the box.</param>
    /// <param name="halfExtents">Half the edge length along each axis; the sign is ignored.</param>
    /// <returns>The box.</returns>
    public static BoundingBox3 FromCenter(Vector3 center, Vector3 halfExtents)
    {
        Vector3 half = Vector3.Abs(halfExtents);
        return new BoundingBox3(center - half, center + half);
    }

    /// <summary>Creates the smallest box that holds a sphere.</summary>
    /// <param name="center">The sphere's center.</param>
    /// <param name="radius">The sphere's radius.</param>
    /// <returns>The box.</returns>
    public static BoundingBox3 AroundSphere(Vector3 center, float radius) =>
        FromCenter(center, new Vector3(Math.Abs(radius)));

    /// <summary>Gets whether a point is inside the box or on its surface.</summary>
    /// <param name="point">The point to test.</param>
    /// <returns><see langword="true"/> when the point is inside.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Contains(Vector3 point) =>
        point.X >= Min.X && point.X <= Max.X
        && point.Y >= Min.Y && point.Y <= Max.Y
        && point.Z >= Min.Z && point.Z <= Max.Z;

    /// <summary>Gets whether a point given as three coordinates is inside the box or on its surface.</summary>
    /// <param name="x">The X coordinate.</param>
    /// <param name="y">The Y coordinate.</param>
    /// <param name="z">The Z coordinate.</param>
    /// <returns><see langword="true"/> when the point is inside.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Contains(float x, float y, float z) =>
        x >= Min.X && x <= Max.X
        && y >= Min.Y && y <= Max.Y
        && z >= Min.Z && z <= Max.Z;

    /// <summary>Gets whether another box lies entirely inside this one.</summary>
    /// <param name="other">The box to test.</param>
    /// <returns><see langword="true"/> when <paramref name="other"/> is inside.</returns>
    public bool Contains(BoundingBox3 other) => Contains(other.Min) && Contains(other.Max);

    /// <summary>Gets whether two boxes overlap or touch.</summary>
    /// <param name="other">The other box.</param>
    /// <returns><see langword="true"/> when they share at least one point.</returns>
    public bool Intersects(BoundingBox3 other) =>
        Min.X <= other.Max.X && Max.X >= other.Min.X
        && Min.Y <= other.Max.Y && Max.Y >= other.Min.Y
        && Min.Z <= other.Max.Z && Max.Z >= other.Min.Z;

    /// <summary>Gets whether the box and a sphere share at least one point.</summary>
    /// <param name="center">The sphere's center.</param>
    /// <param name="radius">The sphere's radius.</param>
    /// <returns><see langword="true"/> when they overlap or touch.</returns>
    public bool Intersects(Vector3 center, float radius) => DistanceSquared(center) <= radius * radius;

    /// <summary>Gets the point in the box closest to a given point.</summary>
    /// <param name="point">The point.</param>
    /// <returns>The point itself when inside, otherwise the nearest point on the surface.</returns>
    public Vector3 ClosestPoint(Vector3 point) => Vector3.Clamp(point, Min, Max);

    /// <summary>Gets the squared distance from a point to the box; zero when the point is inside.</summary>
    /// <param name="point">The point.</param>
    /// <returns>The squared distance.</returns>
    public float DistanceSquared(Vector3 point) => Vector3.DistanceSquared(point, ClosestPoint(point));

    /// <summary>Returns a box grown to hold a point.</summary>
    /// <param name="point">The point to include.</param>
    /// <returns>The grown box.</returns>
    public BoundingBox3 Encapsulate(Vector3 point) => new(Vector3.Min(Min, point), Vector3.Max(Max, point));

    /// <inheritdoc/>
    public bool Equals(BoundingBox3 other) => Min.Equals(other.Min) && Max.Equals(other.Max);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is BoundingBox3 other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode() => HashCode.Combine(Min, Max);

    /// <summary>Compares two boxes for equality.</summary>
    public static bool operator ==(BoundingBox3 left, BoundingBox3 right) => left.Equals(right);

    /// <summary>Compares two boxes for inequality.</summary>
    public static bool operator !=(BoundingBox3 left, BoundingBox3 right) => !left.Equals(right);

    /// <inheritdoc/>
    public override string ToString() => $"[{Min} .. {Max}]";
}
