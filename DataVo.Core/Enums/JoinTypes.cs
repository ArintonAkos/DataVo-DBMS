namespace DataVo.Core.Enums;

/// <summary>
/// Defines the canonical join type names used across parsing and execution.
/// </summary>
public static class JoinTypes
{
    /// <summary>
    /// Represents the SQL INNER keyword.
    /// </summary>
    public const string INNER = "INNER";
    /// <summary>
    /// Represents the SQL LEFT keyword.
    /// </summary>
    public const string LEFT = "LEFT";
    /// <summary>
    /// Represents the SQL RIGHT keyword.
    /// </summary>
    public const string RIGHT = "RIGHT";
    /// <summary>
    /// Represents the SQL FULL keyword.
    /// </summary>
    public const string FULL = "FULL";
    /// <summary>
    /// Represents the SQL CROSS keyword.
    /// </summary>
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