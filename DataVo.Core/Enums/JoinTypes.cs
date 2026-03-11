namespace DataVo.Core.Enums;

/// <summary>
/// Defines the canonical join type names used across parsing and execution.
/// </summary>
public static class JoinTypes
{
    public const string INNER = "INNER";
    public const string LEFT = "LEFT";
    public const string RIGHT = "RIGHT";
    public const string FULL = "FULL";
    public const string CROSS = "CROSS";

    /// <summary>
    /// Gets the set of supported join type names.
    /// </summary>
    public static readonly HashSet<string> All = new(StringComparer.OrdinalIgnoreCase)
    {
        INNER,
        LEFT,
        RIGHT,
        FULL,
        CROSS
    };
}